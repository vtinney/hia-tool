"""Built-in HIA data resolver.

Given a pollutant/country/year/analysisLevel, assembles per-polygon
arrays of concentration, population, and incidence aligned to the
requested reporting polygon. Handles finest-of-each-input logic:
broadcast coarser inputs, aggregate finer inputs.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np


@dataclass
class Provenance:
    """Records the native grain and source of each resolved input."""
    concentration: dict[str, Any]
    population: dict[str, Any]
    incidence: dict[str, Any]


@dataclass
class ResolvedInputs:
    """Per-polygon arrays + metadata returned by the resolver."""
    zone_ids: list[str]
    zone_names: list[str | None]
    parent_ids: list[str | None]
    geometries: list[dict]
    c_baseline: np.ndarray
    c_control: np.ndarray
    population: np.ndarray
    provenance: Provenance
    warnings: list[str] = field(default_factory=list)


import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely import wkt
from shapely.geometry import mapping
from shapely.ops import unary_union


def _data_root() -> Path:
    return Path(os.getenv("DATA_ROOT", "./data/processed"))


def _acs_path(country: str, year: int) -> Path:
    path = _data_root() / "demographics" / country / f"{year}.parquet"
    if not path.exists():
        raise FileNotFoundError(str(path))
    return path


def _gdf_from_acs(country: str, year: int) -> gpd.GeoDataFrame:
    """Load the ACS tract parquet and convert WKT geometry to shapely."""
    df = pd.read_parquet(_acs_path(country, year))
    df["geometry"] = df["geometry"].apply(
        lambda g: wkt.loads(g) if isinstance(g, str) else g
    )
    return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")


def load_reporting_polygons(
    country: str,
    year: int,
    analysis_level: str,
    state_filter: str | None = None,
    county_filter: str | None = None,
) -> dict[str, Any]:
    """Return reporting polygons at the requested grain.

    For ``analysis_level`` values:
        - ``tract``:   ACS tracts directly (requires ``state_filter``)
        - ``county``:  tracts dissolved by ``state_fips + county_fips``
        - ``state``:   tracts dissolved by ``state_fips``
        - ``country``: single polygon (union of all tracts)

    Returns a dict with ``zone_ids``, ``zone_names``, ``parent_ids``,
    ``geometries``, ``population``, and ``state_ids`` (for later C-broadcast joins).
    """
    if analysis_level not in {"tract", "county", "state", "country"}:
        raise ValueError(f"Unknown analysis_level: {analysis_level}")

    gdf = _gdf_from_acs(country, year)
    if state_filter is not None:
        gdf = gdf[gdf["state_fips"] == state_filter]
    if county_filter is not None:
        if state_filter is None:
            raise ValueError("county_filter requires state_filter")
        gdf = gdf[gdf["county_fips"] == county_filter]

    if len(gdf) == 0:
        raise ValueError(
            f"No tracts matched filters state={state_filter} county={county_filter}"
        )

    if analysis_level == "tract":
        return {
            "zone_ids": gdf["geoid"].astype(str).tolist(),
            "zone_names": gdf["geoid"].astype(str).tolist(),
            "parent_ids": gdf["county_fips"].astype(str).tolist(),
            "state_ids": gdf["state_fips"].astype(str).tolist(),
            "geometries": [mapping(g) for g in gdf["geometry"]],
            "population": gdf["total_pop"].to_numpy(dtype=float),
        }

    if analysis_level == "county":
        # Dissolve tracts up to state_fips + county_fips
        gdf = gdf.copy()
        gdf["county_geoid"] = gdf["state_fips"] + gdf["county_fips"]
        dissolved = gdf.dissolve(
            by="county_geoid",
            aggfunc={"total_pop": "sum", "state_fips": "first", "county_fips": "first"},
        ).reset_index()
        return {
            "zone_ids": dissolved["county_geoid"].astype(str).tolist(),
            "zone_names": dissolved["county_geoid"].astype(str).tolist(),
            "parent_ids": dissolved["state_fips"].astype(str).tolist(),
            "state_ids": dissolved["state_fips"].astype(str).tolist(),
            "geometries": [mapping(g) for g in dissolved["geometry"]],
            "population": dissolved["total_pop"].to_numpy(dtype=float),
        }

    if analysis_level == "state":
        dissolved = gdf.dissolve(
            by="state_fips",
            aggfunc={"total_pop": "sum"},
        ).reset_index()
        return {
            "zone_ids": dissolved["state_fips"].astype(str).tolist(),
            "zone_names": dissolved["state_fips"].astype(str).tolist(),
            "parent_ids": [country] * len(dissolved),
            "state_ids": dissolved["state_fips"].astype(str).tolist(),
            "geometries": [mapping(g) for g in dissolved["geometry"]],
            "population": dissolved["total_pop"].to_numpy(dtype=float),
        }

    # analysis_level == "country"
    country_geom = unary_union(gdf["geometry"].to_list())
    return {
        "zone_ids": [country],
        "zone_names": [country.upper()],
        "parent_ids": [None],
        "state_ids": [None],
        "geometries": [mapping(country_geom)],
        "population": np.array([gdf["total_pop"].sum()], dtype=float),
    }


def _epa_aqs_state_path(pollutant: str, year: int) -> Path:
    return (
        _data_root() / "epa_aqs" / pollutant / "ne_states" / f"{year}.parquet"
    )


def _epa_aqs_country_path(pollutant: str, year: int) -> Path:
    return (
        _data_root() / "epa_aqs" / pollutant / "ne_countries" / f"{year}.parquet"
    )


def _who_aap_path(year: int) -> Path:
    return _data_root() / "who_aap" / "ne_countries" / f"{year}.parquet"


def _concentration_column(df: pd.DataFrame, pollutant: str) -> str:
    """Find the concentration column for a pollutant."""
    candidates = [f"mean_{pollutant}", "mean", "concentration", "value"]
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"No concentration column found in {list(df.columns)}")


def resolve_concentration(
    pollutant: str,
    country: str,
    year: int,
    analysis_level: str,
    polygons: dict[str, Any],
) -> tuple[np.ndarray, dict[str, Any], list[str]]:
    """Return per-polygon concentration values, provenance, and warnings.

    Resolution order:
    1. EPA AQS state-level parquet (US only): direct use if analysis_level == 'state',
       broadcast to tracts/counties via state_ids otherwise.
    2. EPA AQS country-level parquet (US only): broadcast scalar to all polygons.
    3. WHO AAP country-level parquet: broadcast scalar to all polygons.

    Raises FileNotFoundError if no source matches.
    """
    n_zones = len(polygons["zone_ids"])
    warnings: list[str] = []

    # EPA AQS state-level (US)
    state_path = _epa_aqs_state_path(pollutant, year)
    if country == "us" and state_path.exists():
        df = pd.read_parquet(state_path)
        df = df[df["admin_id"].str.startswith("US-", na=False)]
        df["state_fips"] = df["admin_id"].str.replace("US-", "", regex=False)
        col = _concentration_column(df, pollutant)
        lookup = dict(zip(df["state_fips"], df[col]))

        if analysis_level == "state":
            c = np.array(
                [lookup.get(sid, np.nan) for sid in polygons["zone_ids"]],
                dtype=float,
            )
            prov = {"grain": "state", "source": "epa_aqs"}
            return c, prov, warnings

        # Broadcast state value to each tract/county via state_ids
        state_ids = polygons.get("state_ids", [None] * n_zones)
        c = np.array(
            [lookup.get(sid, np.nan) for sid in state_ids], dtype=float,
        )
        prov = {
            "grain": "state", "source": "epa_aqs", "broadcast_to": analysis_level,
        }
        warnings.append(
            f"Concentration (state) broadcast to {analysis_level} reporting unit — "
            f"per-{analysis_level} C is uniform within a state"
        )
        return c, prov, warnings

    # EPA AQS country-level (US)
    country_path = _epa_aqs_country_path(pollutant, year)
    if country == "us" and country_path.exists():
        df = pd.read_parquet(country_path)
        df = df[df["admin_id"] == "USA"]
        if len(df) == 0:
            raise FileNotFoundError(f"No US row in {country_path}")
        col = _concentration_column(df, pollutant)
        scalar = float(df[col].iloc[0])
        c = np.full(n_zones, scalar, dtype=float)
        prov = {
            "grain": "country", "source": "epa_aqs", "broadcast_to": analysis_level,
        }
        warnings.append(
            f"Concentration (country) broadcast to {analysis_level} — "
            f"all polygons share the same C"
        )
        return c, prov, warnings

    # WHO AAP country-level (PM2.5 only)
    who_path = _who_aap_path(year)
    if pollutant == "pm25" and who_path.exists():
        df = pd.read_parquet(who_path)
        iso3_by_slug = {"us": "USA", "mexico": "MEX", "mex": "MEX"}
        iso3 = iso3_by_slug.get(country, country.upper() if len(country) == 3 else None)
        if iso3 is None:
            raise FileNotFoundError(f"No ISO3 mapping for country={country}")
        df = df[df["admin_id"] == iso3]
        if len(df) == 0:
            raise FileNotFoundError(f"No {iso3} row in {who_path}")
        col = _concentration_column(df, pollutant)
        scalar = float(df[col].iloc[0])
        c = np.full(n_zones, scalar, dtype=float)
        prov = {
            "grain": "country", "source": "who_aap", "broadcast_to": analysis_level,
        }
        warnings.append(
            f"Concentration (WHO AAP country-level) broadcast to {analysis_level}"
        )
        return c, prov, warnings

    raise FileNotFoundError(
        f"No concentration data for {pollutant}/{country}/{year} at any grain"
    )
