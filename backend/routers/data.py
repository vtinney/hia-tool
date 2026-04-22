"""Data endpoints — serve pre-processed concentration, population, and
incidence datasets as GeoJSON / JSON from local Parquet/CSV files.

Routes
------
GET /api/data/concentration/{pollutant}/{country}/{year}
GET /api/data/population/{country}/{year}
GET /api/data/incidence/{country}/{cause}/{year}
GET /api/data/datasets
"""

from __future__ import annotations

import csv
import json
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from shapely import wkt

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/data", tags=["data"])

STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "local")
DATA_ROOT = Path(os.getenv("DATA_ROOT", "./data/processed"))

# Non-pollutant top-level directories under DATA_ROOT — excluded from the
# concentration dataset scan so they don't masquerade as pollutants.
_NON_POLLUTANT_DIRS = {
    "population", "incidence", "demographics", "epa_aqs", "who_aap",
    "pollution", "boundaries", "crf",
}

# Accept ISO3 (upper/lower), common slugs, and canonical directory names.
# The canonical value is the on-disk directory name under DATA_ROOT.
_COUNTRY_ALIASES = {
    "us": "us", "usa": "us", "united-states": "us",
    "mx": "mexico", "mex": "mexico", "mexico": "mexico",
}

# Map canonical country slug → the GBD `location_name` string used in
# `incidence/gbd_rates.parquet`. Extend as more countries get wired up.
# The `_gbd_location_names()` helper augments this at runtime by reading
# distinct ISO3 / location_name pairs from the parquet itself.
_GBD_LOCATION_NAMES = {
    "us": "United States of America",
    "mexico": "Mexico",
}

# Map canonical country slug → the ISO-3 alpha code used in WHO AAP's
# ``admin_id`` column. Keep this small and explicit rather than a full
# pycountry dependency — ``_canonical_country`` normalizes input first.
_ISO3_BY_SLUG = {
    "us": "USA", "usa": "USA",
    "mexico": "MEX", "mex": "MEX",
}


def _canonical_country(raw: str) -> str:
    """Normalize a country identifier to its on-disk directory slug.

    Accepts ISO3 alpha-3 (``USA``/``usa``), common names, and the slug
    itself. Unknown inputs are returned lowercased as a best-effort
    fallback so new countries still resolve if their dir exists.
    """
    return _COUNTRY_ALIASES.get(raw.lower(), raw.lower())


@lru_cache(maxsize=1)
def _gbd_location_names() -> dict[str, str]:
    """Return a slug → GBD ``location_name`` map, merging the hard-coded
    aliases with every distinct name in ``gbd_rates.parquet``.

    The parquet's ISO3 columns are mostly null, so we key on a slug
    derived from the location_name (lowercased, spaces → dashes) as
    well as on the curated aliases in ``_GBD_LOCATION_NAMES``.
    """
    out = dict(_GBD_LOCATION_NAMES)
    path = DATA_ROOT / "incidence" / "gbd_rates.parquet"
    if not path.exists():
        return out
    try:
        df = _read_parquet(str(path.resolve()))
        for name in df["location_name"].dropna().unique():
            slug = str(name).lower().replace(" ", "-")
            out.setdefault(slug, name)
    except Exception:
        logger.warning("Failed to enumerate GBD location names", exc_info=True)
    return out


# ────────────────────────────────────────────────────────────────────
#  Local filesystem reader
# ────────────────────────────────────────────────────────────────────


def _resolve_path(*segments: str) -> Path:
    """Join path segments under DATA_ROOT and verify existence."""
    path = DATA_ROOT.joinpath(*segments)
    if not path.exists():
        raise FileNotFoundError(str(path))
    return path


@lru_cache(maxsize=64)
def _read_parquet(path_str: str) -> pd.DataFrame:
    """Read and cache a Parquet file. Keyed by string path for hashability."""
    logger.debug("Cache miss — reading Parquet: %s", path_str)
    return pd.read_parquet(path_str, engine="pyarrow")


@lru_cache(maxsize=64)
def _read_csv(path_str: str) -> pd.DataFrame:
    """Read and cache a CSV file."""
    logger.debug("Cache miss — reading CSV: %s", path_str)
    return pd.read_csv(path_str)


def _find_file(directory: Path, stem: str, extensions: tuple[str, ...] = (".parquet", ".csv")) -> Path:
    """Find the first file matching *stem* with any of *extensions*."""
    for ext in extensions:
        candidate = directory / f"{stem}{ext}"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"No file matching '{stem}' with extensions {extensions} in {directory}"
    )


def _read_table(path: Path) -> pd.DataFrame:
    """Read a Parquet or CSV file, using the LRU cache."""
    key = str(path.resolve())
    if path.suffix == ".parquet":
        return _read_parquet(key)
    return _read_csv(key)


# ────────────────────────────────────────────────────────────────────
#  Helpers
# ────────────────────────────────────────────────────────────────────


def _df_to_geojson(df: pd.DataFrame, geometry_col: str = "geometry") -> dict:
    """Convert a DataFrame with a WKT geometry column to a GeoJSON dict."""
    features = []
    for _, row in df.iterrows():
        props = {
            k: _sanitize(v)
            for k, v in row.items()
            if k != geometry_col
        }
        geom_val = row.get(geometry_col)
        geom = (
            wkt.loads(geom_val).__geo_interface__
            if isinstance(geom_val, str)
            else None
        )
        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": geom,
        })
    return {"type": "FeatureCollection", "features": features}


def _sanitize(v: Any) -> Any:
    """Make a value JSON-safe (handle NaN, numpy types)."""
    if isinstance(v, float) and (v != v):  # NaN check without numpy import
        return None
    # numpy scalar → python native
    if hasattr(v, "item"):
        return v.item()
    return v


# ────────────────────────────────────────────────────────────────────
#  1. Concentration
# ────────────────────────────────────────────────────────────────────


@router.get("/concentration/{pollutant}/{country}/{year}")
async def get_concentration(
    pollutant: str,
    country: str,
    year: int,
    aggregation: str = Query(
        "state",
        description="For EPA AQS fallback: 'state' (ne_states) or "
                    "'country' (ne_countries). Ignored when a direct "
                    "{pollutant}/{country}/{year} file exists.",
        pattern="^(state|country)$",
    ),
):
    """Return GeoJSON with admin polygons and concentration values.

    Primary source: ``data/processed/{pollutant}/{country}/{year}.parquet``.

    Fallback for the US: ``data/processed/epa_aqs/{pollutant}/
    {ne_states|ne_countries}/{year}.parquet`` — state- or country-level
    means aggregated from EPA AQS monitor observations.
    """
    slug = _canonical_country(country)

    # 1. Primary path — direct pollutant/country/year file.
    try:
        directory = _resolve_path(pollutant, slug)
        file_path = _find_file(directory, str(year))
        return _df_to_geojson(_read_table(file_path))
    except FileNotFoundError:
        pass

    # 2. EPA AQS fallback — only covers the US dataset on disk today.
    if slug == "us":
        agg_dir = "ne_states" if aggregation == "state" else "ne_countries"
        try:
            directory = _resolve_path("epa_aqs", pollutant, agg_dir)
            file_path = _find_file(directory, str(year))
        except FileNotFoundError:
            file_path = None
        if file_path is not None:
            df = _read_table(file_path)
            # EPA AQS ne_countries contains USA and PRI; ne_states contains
            # all US states. Filter to rows whose admin_id identifies the US.
            if agg_dir == "ne_countries":
                df = df[df["admin_id"] == "USA"]
            else:
                df = df[df["admin_id"].str.startswith("US-", na=False)]
            if len(df) > 0:
                return _df_to_geojson(df)

    # 3. WHO AAP fallback — global PM2.5 coverage, country-level aggregate.
    # Only PM2.5 is available in who_aap/ today, so pollutant must match.
    who_agg = "ne_states" if (slug == "us" and aggregation == "state") else "ne_countries"
    try:
        who_dir = _resolve_path("who_aap", who_agg)
        who_file = _find_file(who_dir, str(year))
    except FileNotFoundError:
        who_file = None
    if who_file is not None and pollutant == "pm25":
        df = _read_table(who_file)
        # WHO AAP's admin_id is ISO3 alpha-3 for countries and `US-XX`
        # for US states. Prefer the raw ISO3 input when it's 3-letter
        # (e.g. ``MEX``, ``IND``); otherwise fall back to the curated map.
        if who_agg == "ne_states" and slug == "us":
            df = df[df["admin_id"].str.startswith("US-", na=False)]
        else:
            iso3 = country.upper() if len(country) == 3 else _ISO3_BY_SLUG.get(slug)
            if iso3 is None:
                df = df.iloc[0:0]  # no way to identify — refuse rather than return all
            else:
                df = df[df["admin_id"] == iso3]
        if len(df) > 0:
            return _df_to_geojson(df)

    raise HTTPException(
        status_code=404,
        detail=f"No {pollutant} data for {country}/{year}",
    )


# ────────────────────────────────────────────────────────────────────
#  2. Population
# ────────────────────────────────────────────────────────────────────


@router.get("/population/{country}/{year}")
async def get_population(country: str, year: int):
    """Return JSON with population by admin unit and age group.

    Primary source: ``data/processed/population/{country}/{year}.parquet``
    with columns ``admin_id``, ``admin_name``, ``total``, and optional
    ``age_*`` columns.

    Fallback: ``data/processed/demographics/{country}/{year}.parquet``
    (tract-level ACS), using ``total_pop`` as the total. No age
    breakdown is available from the ACS fallback.
    """
    slug = _canonical_country(country)

    # 1. Primary path — dedicated population file.
    try:
        directory = _resolve_path("population", slug)
        file_path = _find_file(directory, str(year))
        df = _read_table(file_path)
        age_cols = [c for c in df.columns if c.startswith("age_")]
        records = []
        for _, row in df.iterrows():
            entry: dict[str, Any] = {
                "admin_id": _sanitize(row.get("admin_id")),
                "admin_name": _sanitize(row.get("admin_name")),
                "total": _sanitize(row.get("total", row.get("population"))),
            }
            if age_cols:
                entry["age_groups"] = {
                    col: _sanitize(row[col]) for col in age_cols
                }
            records.append(entry)
        return {"country": slug, "year": year, "units": records}
    except FileNotFoundError:
        pass

    # 2. Demographics fallback — derive total from ACS tract-level.
    try:
        directory = _resolve_path("demographics", slug)
        file_path = _find_file(directory, str(year))
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"No population data for {country}/{year}",
        )

    df = _read_table(file_path)
    if "total_pop" not in df.columns:
        raise HTTPException(
            status_code=404,
            detail=f"No population data for {country}/{year}",
        )

    records = [
        {
            "admin_id": _sanitize(row.get("geoid")),
            "admin_name": _sanitize(row.get("geoid")),
            "total": _sanitize(row["total_pop"]),
        }
        for _, row in df.iterrows()
    ]
    return {
        "country": slug,
        "year": year,
        "units": records,
        "source": "acs_demographics_derived",
    }


# ────────────────────────────────────────────────────────────────────
#  3. Incidence
# ────────────────────────────────────────────────────────────────────


@router.get("/incidence/{country}/{cause}/{year}")
async def get_incidence(
    country: str,
    cause: str,
    year: int,
    measure: str | None = Query(
        None,
        description="Filter GBD fallback rows by measure slug "
                    "('deaths', 'incidence', 'prevalence', ...). Default "
                    "is 'deaths' for mortality causes when multiple are "
                    "present; ignored for the per-country file path.",
    ),
    sex: str = Query(
        "both",
        description="Sex slug ('both', 'male', 'female'). GBD fallback only.",
    ),
):
    """Return JSON with baseline incidence rates by admin unit.

    Primary source: ``data/processed/incidence/{country}/{cause}/
    {year}.parquet`` — tract/state-level rates with ``admin_id``,
    ``admin_name``, ``incidence_rate``.

    Fallback: ``data/processed/incidence/gbd_rates.parquet`` — a global
    long-format table with one row per (cause, location, year,
    age_group, measure, sex). Filtered to the requested country's
    location_name, cause, and year. Returns one entry per age_group
    with ``incidence_rate`` plus ``rate_lower`` / ``rate_upper`` bounds.
    """
    slug = _canonical_country(country)

    # 1. Primary path — country/cause/year parquet.
    try:
        directory = _resolve_path("incidence", slug, cause)
        file_path = _find_file(directory, str(year))
        df = _read_table(file_path)
        records = [
            {
                "admin_id": _sanitize(row.get("admin_id")),
                "admin_name": _sanitize(row.get("admin_name")),
                "incidence_rate": _sanitize(
                    row.get("incidence_rate", row.get("rate"))
                ),
                "age_group": _sanitize(row.get("age_group")),
                "cause": cause,
            }
            for _, row in df.iterrows()
        ]
        return {
            "country": slug, "cause": cause, "year": year,
            "units": records,
        }
    except FileNotFoundError:
        pass

    # 2. Global GBD rates fallback.
    gbd_path = DATA_ROOT / "incidence" / "gbd_rates.parquet"
    location_name = _gbd_location_names().get(slug)
    if not gbd_path.exists() or location_name is None:
        raise HTTPException(
            status_code=404,
            detail=f"No incidence data for {country}/{cause}/{year}",
        )

    df = _read_parquet(str(gbd_path.resolve()))
    subset = df[
        (df["cause"] == cause)
        & (df["location_name"] == location_name)
        & (df["year"] == year)
    ]
    if "sex" in subset.columns:
        subset = subset[subset["sex"] == sex]

    # Apply measure filter. When unset, prefer 'deaths' for mortality
    # causes when both 'deaths' and 'incidence' rows exist — matches the
    # old behavior of the per-country files which only carried mortality.
    if "measure" in subset.columns and len(subset) > 0:
        if measure is not None:
            subset = subset[subset["measure"] == measure]
        elif "deaths" in set(subset["measure"].unique()):
            subset = subset[subset["measure"] == "deaths"]

    if len(subset) == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No incidence data for {country}/{cause}/{year}",
        )

    def _entry(row: Any) -> dict[str, Any]:
        e: dict[str, Any] = {
            "admin_id": slug,
            "admin_name": location_name,
            "incidence_rate": _sanitize(row["rate"]),
            "age_group": _sanitize(row.get("age_group")),
            "cause": cause,
        }
        if "rate_lower" in row:
            e["rate_lower"] = _sanitize(row.get("rate_lower"))
            e["rate_upper"] = _sanitize(row.get("rate_upper"))
        if "measure" in row:
            e["measure"] = _sanitize(row.get("measure"))
        if "sex" in row:
            e["sex"] = _sanitize(row.get("sex"))
        return e

    records = [_entry(row) for _, row in subset.iterrows()]
    return {
        "country": slug, "cause": cause, "year": year,
        "units": records, "source": "gbd_rates",
    }


# ────────────────────────────────────────────────────────────────────
#  4. Demographics (ACS)
# ────────────────────────────────────────────────────────────────────


@router.get("/demographics/{country}/{year}")
async def get_demographics(
    country: str,
    year: int,
    state: str | None = Query(
        None,
        description="2-digit state FIPS filter (e.g. '06' for California). "
                    "Required when the full nationwide dataset would be too "
                    "large to return in one response.",
        min_length=2,
        max_length=2,
    ),
    county: str | None = Query(
        None,
        description="3-digit county FIPS filter (e.g. '037' for Los Angeles). "
                    "Must be combined with `state`.",
        min_length=3,
        max_length=3,
    ),
    simplify: float = Query(
        0.0001,
        description="Douglas-Peucker tolerance in degrees for geometry "
                    "simplification. Default 0.0001° (~11 m) is visually "
                    "lossless but drops ~60-80%% of TIGER vertices. Pass 0 "
                    "to disable simplification and return full precision.",
        ge=0.0,
        le=0.1,
    ),
):
    """Return GeoJSON with ACS 5-year demographics by census tract.

    Reads from ``data/processed/demographics/{country}/{year}.parquet``
    (output of ``backend/etl/process_acs.py``).

    The nationwide file is ~85k tracts / ~170 MB as raw GeoJSON, which
    is too large to drop into a browser. Callers should filter by
    ``state`` (and optionally ``county``) for any interactive use.
    """
    if county is not None and state is None:
        raise HTTPException(
            status_code=400,
            detail="`county` filter requires `state` to also be set.",
        )

    slug = _canonical_country(country)
    try:
        directory = _resolve_path("demographics", slug)
        file_path = _find_file(directory, str(year))
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"No demographics data for {country}/{year}",
        )

    df = _read_table(file_path)

    if state is not None:
        df = df[df["state_fips"] == state]
    if county is not None:
        df = df[df["county_fips"] == county]

    if len(df) == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No demographics rows match filter "
                   f"(state={state}, county={county})",
        )

    return _df_to_geojson_simplified(df, simplify_tolerance=simplify)


def _df_to_geojson_simplified(
    df: pd.DataFrame,
    geometry_col: str = "geometry",
    simplify_tolerance: float = 0.0,
) -> dict:
    """Like ``_df_to_geojson`` but simplifies each geometry on the fly.

    A tolerance of 0 means "no simplification" (behavior matches
    ``_df_to_geojson`` exactly). Non-zero tolerance applies
    Douglas-Peucker with ``preserve_topology=True`` so neighboring
    tracts don't develop slivers at shared borders.
    """
    features = []
    for _, row in df.iterrows():
        props = {
            k: _sanitize(v)
            for k, v in row.items()
            if k != geometry_col
        }
        geom_val = row.get(geometry_col)
        if isinstance(geom_val, str):
            shape = wkt.loads(geom_val)
            if simplify_tolerance > 0:
                shape = shape.simplify(simplify_tolerance, preserve_topology=True)
            geom = shape.__geo_interface__
        else:
            geom = None
        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": geom,
        })
    return {"type": "FeatureCollection", "features": features}


# ────────────────────────────────────────────────────────────────────
#  5. Dataset listing
# ────────────────────────────────────────────────────────────────────


def _scan_datasets() -> list[dict[str, Any]]:
    """Walk DATA_ROOT to discover available datasets and their metadata.

    Returns a list of dataset descriptors with pollutant, source, years,
    and countries. Intentionally NOT cached so newly-built parquet files
    (e.g. from running an ETL script against a live backend) are picked
    up on the next request without a server restart.
    """
    datasets: list[dict[str, Any]] = []

    if not DATA_ROOT.exists():
        return datasets

    # Concentration datasets: {pollutant}/{country}/{year}.parquet
    pollutant_names = {"pm25": "PM2.5", "ozone": "Ozone", "no2": "NO2"}
    for pollutant_dir in sorted(DATA_ROOT.iterdir()):
        if not pollutant_dir.is_dir():
            continue
        key = pollutant_dir.name
        if key in _NON_POLLUTANT_DIRS:
            continue

        for country_dir in sorted(pollutant_dir.iterdir()):
            if not country_dir.is_dir():
                continue
            years = sorted(
                int(f.stem) for f in country_dir.iterdir()
                if f.suffix in (".parquet", ".csv") and f.stem.isdigit()
            )
            if years:
                datasets.append({
                    "type": "concentration",
                    "pollutant": key,
                    "pollutant_label": pollutant_names.get(key, key),
                    "country": country_dir.name,
                    "countries_covered": [country_dir.name],
                    "years": years,
                    "source": f"Processed {pollutant_names.get(key, key)} raster",
                })

    # EPA AQS concentration datasets: epa_aqs/{pollutant}/{agg}/{year}.parquet
    aqs_dir = DATA_ROOT / "epa_aqs"
    if aqs_dir.exists():
        for pollutant_dir in sorted(aqs_dir.iterdir()):
            if not pollutant_dir.is_dir():
                continue
            pkey = pollutant_dir.name
            state_sub = pollutant_dir / "ne_states"
            if not state_sub.exists():
                continue
            year_files = [
                f for f in state_sub.iterdir()
                if f.suffix in (".parquet", ".csv") and f.stem.isdigit()
            ]
            years = sorted(int(f.stem) for f in year_files)
            if years:
                covered: set[str] = set()
                for f in year_files:
                    try:
                        df = _read_table(f)
                        if "admin_id" in df.columns:
                            covered.update(
                                str(x) for x in df["admin_id"].dropna().unique()
                            )
                    except Exception:
                        logger.warning("Failed to read %s for coverage", f, exc_info=True)
                datasets.append({
                    "id": f"epa_aqs_{pkey}",
                    "type": "concentration",
                    "pollutant": pkey,
                    "pollutant_label": pollutant_names.get(pkey, pkey),
                    "country": "us",
                    "countries_covered": sorted(covered),
                    "years": years,
                    "aggregation": "state",
                    "source": "EPA AQS — state-level monitor means",
                    "label": (
                        f"EPA AQS — {pollutant_names.get(pkey, pkey)} "
                        "(US state-level)"
                    ),
                })

    # WHO AAP — global PM2.5 concentration, country-aggregated. The same
    # parquets also contain state-level rows (US states) so we surface
    # both a country entry (global) and a US-state entry.
    who_countries = DATA_ROOT / "who_aap" / "ne_countries"
    if who_countries.exists():
        year_files = [
            f for f in who_countries.iterdir()
            if f.suffix in (".parquet", ".csv") and f.stem.isdigit()
        ]
        years = sorted(int(f.stem) for f in year_files)
        if years:
            covered: set[str] = set()
            for f in year_files:
                try:
                    df = _read_table(f)
                    if "admin_id" in df.columns:
                        covered.update(
                            str(x) for x in df["admin_id"].dropna().unique()
                        )
                except Exception:
                    logger.warning("Failed to read %s for coverage", f, exc_info=True)
            datasets.append({
                "id": "who_aap_pm25_global",
                "type": "concentration",
                "pollutant": "pm25",
                "pollutant_label": "PM2.5",
                "country": "global",
                "countries_covered": sorted(covered),
                "years": years,
                "aggregation": "country",
                "source": "WHO Ambient Air Pollution Database",
                "label": "WHO AAP — PM2.5 (global, country-level)",
            })

    # Population datasets
    pop_dir = DATA_ROOT / "population"
    if pop_dir.exists():
        for country_dir in sorted(pop_dir.iterdir()):
            if not country_dir.is_dir():
                continue
            years = sorted(
                int(f.stem) for f in country_dir.iterdir()
                if f.suffix in (".parquet", ".csv") and f.stem.isdigit()
            )
            if years:
                datasets.append({
                    "type": "population",
                    "country": country_dir.name,
                    "years": years,
                    "source": "Processed population data",
                })

    # Incidence datasets
    inc_dir = DATA_ROOT / "incidence"
    if inc_dir.exists():
        for country_dir in sorted(inc_dir.iterdir()):
            if not country_dir.is_dir():
                continue
            for cause_dir in sorted(country_dir.iterdir()):
                if not cause_dir.is_dir():
                    continue
                years = sorted(
                    int(f.stem) for f in cause_dir.iterdir()
                    if f.suffix in (".parquet", ".csv") and f.stem.isdigit()
                )
                if years:
                    datasets.append({
                        "type": "incidence",
                        "country": country_dir.name,
                        "cause": cause_dir.name,
                        "years": years,
                        "source": "Processed incidence data",
                    })

    # Demographics datasets: demographics/{country}/{year}.parquet
    demo_dir = DATA_ROOT / "demographics"
    if demo_dir.exists():
        for country_dir in sorted(demo_dir.iterdir()):
            if not country_dir.is_dir():
                continue
            years = sorted(
                int(f.stem) for f in country_dir.iterdir()
                if f.suffix in (".parquet", ".csv") and f.stem.isdigit()
            )
            if years:
                datasets.append({
                    "type": "demographics",
                    "country": country_dir.name,
                    "years": years,
                    "source": "ACS 5-year estimates (B03002, B19013, C17002)",
                })
                # Surface the same file as a population source, derived
                # from ``total_pop``. This is what the population endpoint
                # falls back to when no dedicated population file exists.
                if not (DATA_ROOT / "population" / country_dir.name).exists():
                    datasets.append({
                        "id": f"acs_population_{country_dir.name}",
                        "type": "population",
                        "country": country_dir.name,
                        "years": years,
                        "source": "ACS 5-year total population (tract-level)",
                        "label": (
                            "US Census ACS 5-Year — Total Population "
                            "(tract-level)"
                        ),
                    })

    # GBD global incidence rates — one entry per (country, cause) in the
    # long table, surfaced only when the per-country directory layout is
    # missing that cause. Keeps the primary path authoritative.
    gbd_path = DATA_ROOT / "incidence" / "gbd_rates.parquet"
    if gbd_path.exists():
        try:
            gbd = _read_parquet(str(gbd_path.resolve()))
        except Exception:
            gbd = None
        if gbd is not None and len(gbd) > 0:
            for slug, location_name in _gbd_location_names().items():
                per_country = gbd[gbd["location_name"] == location_name]
                if len(per_country) == 0:
                    continue
                for cause in sorted(per_country["cause"].dropna().unique()):
                    existing_dir = (
                        DATA_ROOT / "incidence" / slug / cause
                    )
                    if existing_dir.exists():
                        continue
                    years = sorted(
                        int(y) for y in
                        per_country[per_country["cause"] == cause]["year"]
                        .dropna().unique()
                    )
                    if not years:
                        continue
                    datasets.append({
                        "id": f"gbd_{slug}_{cause}",
                        "type": "incidence",
                        "country": slug,
                        "cause": cause,
                        "years": years,
                        "source": "GBD 2023 baseline incidence rates",
                        "label": (
                            f"GBD 2023 — {cause.replace('_', ' ').title()} "
                            f"({location_name})"
                        ),
                    })

    return datasets


@router.get("/datasets")
async def list_datasets(
    pollutant: str | None = Query(None, description="Filter by pollutant"),
    country: str | None = Query(None, description="Filter by country"),
    type: str | None = Query(None, description="Filter by type: concentration, population, incidence"),
):
    """List available built-in datasets with metadata."""
    datasets = _scan_datasets()

    if type:
        datasets = [d for d in datasets if d.get("type") == type]
    if pollutant:
        datasets = [d for d in datasets if d.get("pollutant") == pollutant]
    if country:
        slug = _canonical_country(country)
        datasets = [d for d in datasets if d.get("country") == slug]

    return {"datasets": datasets}
