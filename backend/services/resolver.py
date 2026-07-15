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


def _who_aap_adm2_path(year: int) -> Path:
    return _data_root() / "who_aap" / "gadm_adm2" / f"{year}.parquet"


def _gadm_adm2_gpkg_path() -> Path:
    return _data_root() / "boundaries" / "gadm_adm2.gpkg"


# ISO3 mapping for slugs accepted in API requests. Extended whenever a new
# country is added to the built-in path; matches the mapping baked into
# resolve_concentration's WHO AAP fallback.
_ISO3_BY_SLUG: dict[str, str] = {
    "us": "USA",
    "usa": "USA",
    "mexico": "MEX",
    "mex": "MEX",
}


def _normalize_iso3(country: str) -> str | None:
    """Return ISO3 for a country slug, name shortcut, or pre-normalized ISO3."""
    if not country:
        return None
    key = country.lower()
    if key in _ISO3_BY_SLUG:
        return _ISO3_BY_SLUG[key]
    if len(country) == 3 and country.isalpha():
        return country.upper()
    return None


def _concentration_column(df: pd.DataFrame, pollutant: str) -> str:
    """Find the concentration column for a pollutant."""
    candidates = [f"mean_{pollutant}", "mean", "concentration", "value"]
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"No concentration column found in {list(df.columns)}")


# USPS state abbreviation → 2-digit FIPS. EPA AQS state files key states by
# abbreviation (admin_id "US-CA"), but tract/county/state reporting units key
# by FIPS, so the state→reporting-unit broadcast must translate. Values already
# in FIPS form (e.g. a "US-06" test fixture) pass through unchanged.
_US_ABBR_TO_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "DC": "11", "FL": "12", "GA": "13", "HI": "15",
    "ID": "16", "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21",
    "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46",
    "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56", "PR": "72",
}


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
    if country == "us" and state_path.exists() and analysis_level != "custom":
        df = pd.read_parquet(state_path)
        df = df[df["admin_id"].str.startswith("US-", na=False)]
        tokens = df["admin_id"].str.replace("US-", "", regex=False)
        # admin_id is "US-<abbr>" in production data, "US-<fips>" in fixtures.
        df["state_fips"] = tokens.map(lambda t: _US_ABBR_TO_FIPS.get(t, t))
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


def resolve_control(
    c_base: np.ndarray,
    control_mode: str,
    control_value: float | None = None,
    rollback_percent: float | None = None,
) -> np.ndarray:
    """Compute per-polygon control concentration given the baseline array.

    Modes:
    - ``scalar`` / ``benchmark``: broadcast ``control_value`` to every polygon.
    - ``rollback``: multiply baseline by (1 − rollback_percent/100).
    - ``builtin``: not yet implemented; falls back to baseline (no change scenario).
    """
    if control_mode in ("scalar", "benchmark"):
        if control_value is None:
            raise ValueError(f"control_mode={control_mode} requires control_value")
        return np.full_like(c_base, control_value, dtype=float)

    if control_mode == "rollback":
        if rollback_percent is None:
            raise ValueError("control_mode=rollback requires rollback_percent")
        return c_base * (1.0 - rollback_percent / 100.0)

    if control_mode == "builtin":
        # Future: fetch alternate-year/alternate-scenario from parquet.
        # For v1: no-change fallback.
        return c_base.copy()

    raise ValueError(f"Unknown control_mode: {control_mode}")


class YearGapTooLarge(Exception):
    """Raised when requested year is > 2 years from the nearest ACS file."""


MAX_ACS_YEAR_GAP = 2


def _available_acs_years(country: str) -> list[int]:
    """Return sorted years for which we have a demographics/{country}/{y}.parquet."""
    dirpath = _data_root() / "demographics" / country
    if not dirpath.exists():
        return []
    return sorted(
        int(p.stem) for p in dirpath.iterdir()
        if p.suffix == ".parquet" and p.stem.isdigit()
    )


def _resolve_acs_year(country: str, requested_year: int) -> tuple[int, int]:
    """Find the closest available ACS year. Returns (year, |gap|).

    Raises ``YearGapTooLarge`` when no year within MAX_ACS_YEAR_GAP exists.
    """
    years = _available_acs_years(country)
    if not years:
        raise FileNotFoundError(f"No ACS demographics for country={country}")
    nearest = min(years, key=lambda y: abs(y - requested_year))
    gap = abs(nearest - requested_year)
    if gap > MAX_ACS_YEAR_GAP:
        raise YearGapTooLarge(
            f"Nearest ACS year {nearest} is {gap} years from requested {requested_year}"
        )
    return nearest, gap


def prepare_builtin_inputs(
    pollutant: str,
    country: str,
    year: int,
    analysis_level: str,
    control_mode: str,
    state_filter: str | None = None,
    county_filter: str | None = None,
    control_value: float | None = None,
    rollback_percent: float | None = None,
) -> ResolvedInputs:
    # Resolve ACS year first — may differ from requested concentration year
    acs_year, year_gap = _resolve_acs_year(country, year)

    polygons = load_reporting_polygons(
        country=country, year=acs_year, analysis_level=analysis_level,
        state_filter=state_filter, county_filter=county_filter,
    )

    c_base, c_prov, c_warnings = resolve_concentration(
        pollutant=pollutant, country=country, year=year,
        analysis_level=analysis_level, polygons=polygons,
    )

    c_ctrl = resolve_control(
        c_base=c_base, control_mode=control_mode,
        control_value=control_value, rollback_percent=rollback_percent,
    )

    pop_prov = {"grain": analysis_level, "source": "acs", "year": acs_year}
    inc_prov = {"grain": "crf_default", "source": "crf_library"}

    warnings = list(c_warnings)
    if year_gap > 0:
        warnings.append(
            f"Population year {acs_year} used for concentration year {year} "
            f"(gap of {year_gap} year{'s' if year_gap != 1 else ''})"
        )

    return ResolvedInputs(
        zone_ids=polygons["zone_ids"],
        zone_names=polygons["zone_names"],
        parent_ids=polygons["parent_ids"],
        geometries=polygons["geometries"],
        c_baseline=c_base,
        c_control=c_ctrl,
        population=polygons["population"],
        provenance=Provenance(
            concentration=c_prov,
            population=pop_prov,
            incidence=inc_prov,
        ),
        warnings=warnings,
    )


from backend.services.geo_processor import (
    read_boundaries, _detect_id_column, _detect_name_column,
)


def prepare_custom_boundary_inputs(
    pollutant: str,
    country: str,
    year: int,
    boundary_path: str,
    control_mode: str,
    control_value: float | None = None,
    rollback_percent: float | None = None,
) -> ResolvedInputs:
    """Resolver for user-uploaded boundary + built-in C/pop.

    Today, population for non-US custom boundaries falls back to
    WHO AAP country-level scalar broadcast. US custom boundaries use
    the same fallback until we add zonal-stats of ACS tracts — the
    follow-up for that is noted in the spec.
    """
    gdf = read_boundaries(boundary_path)
    n_zones = len(gdf)
    id_col = _detect_id_column(gdf)
    name_col = _detect_name_column(gdf)

    zone_ids = (
        gdf[id_col].astype(str).tolist()
        if id_col != "index" else [str(i) for i in range(n_zones)]
    )
    zone_names = (
        gdf[name_col].astype(str).tolist() if name_col else [None] * n_zones
    )
    geometries = [mapping(g) if g else None for g in gdf.geometry]

    # Build a synthetic polygons dict so resolve_concentration works
    # unchanged. state_ids is unknown for custom boundaries.
    polygons = {
        "zone_ids": zone_ids,
        "zone_names": zone_names,
        "parent_ids": [None] * n_zones,
        "state_ids": [None] * n_zones,
        "geometries": geometries,
        "population": np.zeros(n_zones, dtype=float),
    }

    # resolve_concentration only broadcasts country-level scalars for
    # custom boundaries (state-level broadcast needs state_ids).
    # We drop back to country-level resolution by clearing state_ids.
    c_base, c_prov, c_warnings = resolve_concentration(
        pollutant=pollutant, country=country, year=year,
        analysis_level="custom", polygons=polygons,
    )
    c_ctrl = resolve_control(
        c_base=c_base, control_mode=control_mode,
        control_value=control_value, rollback_percent=rollback_percent,
    )

    # Population fallback for custom boundaries: country-level scalar,
    # split evenly across polygons. Flagged explicitly.
    pop_total_path = _data_root() / "population" / country / f"{year}.parquet"
    pop_per_zone = np.zeros(n_zones, dtype=float)
    pop_prov = {"grain": "country_scalar", "source": "fallback_even_split"}
    warnings = list(c_warnings)
    warnings.append(
        "Population fallback: country-level total split evenly across "
        "custom polygons. Upload a population raster for per-polygon accuracy."
    )

    if pop_total_path.exists():
        df = pd.read_parquet(pop_total_path)
        total = float(df["total"].sum()) if "total" in df.columns else 0.0
        pop_per_zone = np.full(n_zones, total / max(n_zones, 1))

    return ResolvedInputs(
        zone_ids=zone_ids,
        zone_names=zone_names,
        parent_ids=[None] * n_zones,
        geometries=geometries,
        c_baseline=c_base,
        c_control=c_ctrl,
        population=pop_per_zone,
        provenance=Provenance(
            concentration=c_prov,
            population=pop_prov,
            incidence={"grain": "crf_default", "source": "crf_library"},
        ),
        warnings=warnings,
    )


# ─────────────────────────────────────────────────────────────────────────
#  Admin-2 (GADM) global path
# ─────────────────────────────────────────────────────────────────────────
#
#  Used when analysisLevel == "adm2" or country == "global". Both PM2.5
#  (population-weighted) and population (with age bins) come from the same
#  per-year parquet at data/processed/who_aap/gadm_adm2/{year}.parquet,
#  produced by the GEE export script. Boundary geometry comes from the
#  one-time slim GeoPackage at data/processed/boundaries/gadm_adm2.gpkg.

# Module-level cache for the boundary GeoPackage. Loaded once per process,
# filtered per call. ~46k rows × a few columns is small enough that holding
# the full GeoDataFrame in memory is fine.
_gadm_adm2_gdf: gpd.GeoDataFrame | None = None


def _load_gadm_adm2_boundaries() -> gpd.GeoDataFrame:
    global _gadm_adm2_gdf
    if _gadm_adm2_gdf is None:
        path = _gadm_adm2_gpkg_path()
        if not path.exists():
            raise FileNotFoundError(
                f"GADM admin-2 boundary file missing: {path}. "
                "Build it with scripts/gadm_adm2_to_geopackage.py."
            )
        _gadm_adm2_gdf = gpd.read_file(path)
    return _gadm_adm2_gdf


def prepare_global_adm2_inputs(
    pollutant: str,
    country: str,
    year: int,
    control_mode: str,
    control_value: float | None = None,
    rollback_percent: float | None = None,
) -> ResolvedInputs:
    """Resolver for the GADM admin-2 path: per-zone PM2.5 + population.

    ``country`` may be ``"global"`` (no filter, all ~46k zones) or a country
    slug / ISO3. The same parquet provides both concentration
    (``pm25_popweighted``) and population (``pop_total`` + age bins), so this
    function does not consult the EPA AQS or WHO AAP country-scalar fallbacks.

    Currently only PM2.5 is supported on this path — the GEE export only
    carries PM2.5. NO2 / Ozone callers should not route here.
    """
    if pollutant != "pm25":
        raise FileNotFoundError(
            f"Admin-2 path only carries PM2.5 today (requested {pollutant})"
        )

    stats_path = _who_aap_adm2_path(year)
    if not stats_path.exists():
        raise FileNotFoundError(
            f"GADM admin-2 stats parquet missing for {year}: {stats_path}. "
            "Run scripts/gee_export_pm25.py --boundary gadm_adm2 then "
            "scripts/pm25_csv_to_parquet.py."
        )

    boundaries = _load_gadm_adm2_boundaries()
    df = pd.read_parquet(stats_path)

    # Country filtering. "global" = no filter; otherwise resolve to ISO3 and
    # filter both layers by country_iso3.
    if country and country.lower() != "global":
        iso3 = _normalize_iso3(country)
        if iso3 is None:
            raise FileNotFoundError(
                f"Cannot resolve country='{country}' to ISO3 for admin-2 path"
            )
        boundaries = boundaries[boundaries["country_iso3"] == iso3]
        df = df[df["country_iso3"] == iso3] if "country_iso3" in df.columns else df[
            df["feature_id"].str.startswith(iso3 + ".", na=False)
        ]

    if len(boundaries) == 0:
        raise FileNotFoundError(
            f"No GADM admin-2 boundaries matched country='{country}'"
        )
    if len(df) == 0:
        raise FileNotFoundError(
            f"No GADM admin-2 stats matched country='{country}' in {stats_path}"
        )

    # Inner join on feature_id. Sort to keep zone order stable.
    merged = boundaries.merge(df, on="feature_id", how="inner",
                              suffixes=("", "_stats"))
    merged = merged.sort_values("feature_id").reset_index(drop=True)

    if len(merged) == 0:
        raise FileNotFoundError(
            f"No GADM admin-2 features matched between gpkg and parquet "
            f"for country='{country}', year={year}"
        )

    n_zones = len(merged)
    n_drop_b = len(boundaries) - n_zones
    n_drop_s = len(df) - n_zones
    warnings: list[str] = []
    if n_drop_b or n_drop_s:
        warnings.append(
            f"GADM admin-2 join dropped {n_drop_b} unmatched boundaries and "
            f"{n_drop_s} unmatched stats rows (likely simplification or "
            f"version mismatch between gpkg and stats parquet)."
        )

    c_baseline = merged["pm25_popweighted"].astype(float).to_numpy()
    population = merged["pop_total"].astype(float).to_numpy()

    c_control = resolve_control(
        c_base=c_baseline,
        control_mode=control_mode,
        control_value=control_value,
        rollback_percent=rollback_percent,
    )

    # Use NAME_2 ("name" in the gpkg) for human-readable zone names; merge
    # may have suffixed it with _stats if both sides carry it.
    name_col = "name" if "name" in merged.columns else "name_stats"

    return ResolvedInputs(
        zone_ids=merged["feature_id"].astype(str).tolist(),
        zone_names=merged[name_col].astype(str).tolist(),
        parent_ids=merged["country_iso3"].astype(str).tolist(),
        geometries=[mapping(g) if g is not None else None
                    for g in merged["geometry"]],
        c_baseline=c_baseline,
        c_control=c_control,
        population=population,
        provenance=Provenance(
            concentration={
                "grain": "adm2",
                "source": "acag_via_gee",
                "year": year,
            },
            population={
                "grain": "adm2",
                "source": "worldpop_via_gee",
                "year": int(merged["pop_source_year"].iloc[0])
                        if "pop_source_year" in merged.columns else year,
            },
            incidence={"grain": "crf_default", "source": "crf_library"},
        ),
        warnings=warnings,
    )


# ─────────────────────────────────────────────────────────────────────────
#  Urban-centre (GHS-UCDB R2024A) path
# ─────────────────────────────────────────────────────────────────────────
#
#  Used when analysisLevel == "urban". PM2.5 (population-weighted) and
#  population come from data/processed/ghs_smod_gee/ghs_smod/{year}.parquet
#  (GEE export over the GHS-UCDB R2024A urban centres). Boundary geometry +
#  country_iso3 come from data/processed/boundaries/ghs_ucdb_r2024a.gpkg
#  (built by scripts/ghs_ucdb_to_boundaries.py from the same UCDB source as
#  the GEE asset, so feature_id matches 1:1). The stats parquet has NO
#  country column — country scoping happens through the gpkg.

def _ghs_smod_stats_path(year: int) -> Path:
    return _data_root() / "ghs_smod_gee" / "ghs_smod" / f"{year}.parquet"


def _ghs_ucdb_gpkg_path() -> Path:
    return _data_root() / "boundaries" / "ghs_ucdb_r2024a.gpkg"


_ghs_ucdb_gdf: gpd.GeoDataFrame | None = None


def _load_ghs_ucdb_boundaries() -> gpd.GeoDataFrame:
    global _ghs_ucdb_gdf
    if _ghs_ucdb_gdf is None:
        path = _ghs_ucdb_gpkg_path()
        if not path.exists():
            raise FileNotFoundError(
                f"Urban-centre boundary file missing: {path}. "
                "Build it with scripts/ghs_ucdb_to_boundaries.py."
            )
        _ghs_ucdb_gdf = gpd.read_file(path)
    return _ghs_ucdb_gdf


def prepare_urban_centre_inputs(
    pollutant: str,
    country: str,
    year: int,
    control_mode: str,
    city_ids: list[str] | None = None,
    control_value: float | None = None,
    rollback_percent: float | None = None,
) -> ResolvedInputs:
    """Resolver for the urban-centre path: per-city PM2.5 + population.

    ``country`` must resolve to an ISO3 (no ``"global"`` — an all-world urban
    run is not a supported study area). ``city_ids`` optionally restricts to
    specific centres; None/empty means every centre in the country.
    """
    if pollutant != "pm25":
        raise FileNotFoundError(
            f"Urban-centre path only carries PM2.5 today (requested {pollutant})"
        )
    if not country or country.lower() == "global":
        raise FileNotFoundError(
            "Urban-centre path requires a specific country (got "
            f"'{country}')"
        )
    iso3 = _normalize_iso3(country)
    if iso3 is None:
        raise FileNotFoundError(
            f"Cannot resolve country='{country}' to ISO3 for urban-centre path"
        )

    stats_path = _ghs_smod_stats_path(year)
    if not stats_path.exists():
        raise FileNotFoundError(
            f"Urban-centre stats parquet missing for {year}: {stats_path}. "
            "Run scripts/gee_export_pm25.py --boundary ghs_smod then "
            "scripts/pm25_csv_to_parquet.py --boundary ghs_smod --split-by-year."
        )

    boundaries = _load_ghs_ucdb_boundaries()
    boundaries = boundaries[boundaries["country_iso3"] == iso3]
    if len(boundaries) == 0:
        raise FileNotFoundError(
            f"No urban centres matched country='{country}'"
        )

    if city_ids:
        wanted = {str(c) for c in city_ids}
        boundaries = boundaries[boundaries["feature_id"].isin(wanted)]
        missing = wanted - set(boundaries["feature_id"])
        if missing:
            raise FileNotFoundError(
                f"Unknown urban-centre ids for {iso3}: {sorted(missing)}"
            )

    df = pd.read_parquet(stats_path)
    df["feature_id"] = df["feature_id"].astype(str)

    merged = boundaries.merge(df, on="feature_id", how="inner",
                              suffixes=("", "_stats"))
    merged = merged.sort_values("pop_total", ascending=False).reset_index(drop=True)
    if len(merged) == 0:
        raise FileNotFoundError(
            f"No urban-centre stats matched country='{country}' "
            f"for year={year}"
        )

    warnings: list[str] = []
    n_drop = len(boundaries) - len(merged)
    if n_drop:
        warnings.append(
            f"Urban-centre join dropped {n_drop} boundaries with no stats "
            f"row for {year} (GEE export gap)."
        )

    c_baseline = merged["pm25_popweighted"].astype(float).to_numpy()
    population = merged["pop_total"].astype(float).to_numpy()
    c_control = resolve_control(
        c_base=c_baseline, control_mode=control_mode,
        control_value=control_value, rollback_percent=rollback_percent,
    )

    name_col = "name" if "name" in merged.columns else "name_stats"
    return ResolvedInputs(
        zone_ids=merged["feature_id"].astype(str).tolist(),
        zone_names=merged[name_col].astype(str).tolist(),
        parent_ids=merged["country_iso3"].astype(str).tolist(),
        geometries=[mapping(g) if g is not None else None
                    for g in merged["geometry"]],
        c_baseline=c_baseline,
        c_control=c_control,
        population=population,
        provenance=Provenance(
            concentration={"grain": "urban_centre", "source": "acag_via_gee",
                           "year": year},
            population={"grain": "urban_centre", "source": "worldpop_via_gee",
                        "year": int(merged["pop_source_year"].iloc[0])
                                if "pop_source_year" in merged.columns else year},
            incidence={"grain": "crf_default", "source": "crf_library"},
        ),
        warnings=warnings,
    )
