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
