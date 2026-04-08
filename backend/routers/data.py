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
async def get_concentration(pollutant: str, country: str, year: int):
    """Return GeoJSON with admin polygons and concentration values.

    Reads from ``data/processed/{pollutant}/{country}/{year}.parquet``
    (output of the ETL pipeline).
    """
    try:
        directory = _resolve_path(pollutant, country)
        file_path = _find_file(directory, str(year))
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"No {pollutant} data for {country}/{year}",
        )

    df = _read_table(file_path)
    return _df_to_geojson(df)


# ────────────────────────────────────────────────────────────────────
#  2. Population
# ────────────────────────────────────────────────────────────────────


@router.get("/population/{country}/{year}")
async def get_population(country: str, year: int):
    """Return JSON with population by admin unit and age group.

    Reads from ``data/processed/population/{country}/{year}.parquet``.
    Expected columns: ``admin_id``, ``admin_name``, ``total``, and
    optional age-group columns (e.g. ``age_0_4``, ``age_5_14``, ...).
    """
    try:
        directory = _resolve_path("population", country)
        file_path = _find_file(directory, str(year))
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"No population data for {country}/{year}",
        )

    df = _read_table(file_path)

    # Identify age-group columns (anything starting with "age_")
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

    return {"country": country, "year": year, "units": records}


# ────────────────────────────────────────────────────────────────────
#  3. Incidence
# ────────────────────────────────────────────────────────────────────


@router.get("/incidence/{country}/{cause}/{year}")
async def get_incidence(country: str, cause: str, year: int):
    """Return JSON with baseline incidence rates by admin unit.

    Reads from ``data/processed/incidence/{country}/{cause}/{year}.*``.
    Expected columns: ``admin_id``, ``admin_name``, ``incidence_rate``.
    """
    try:
        directory = _resolve_path("incidence", country, cause)
        file_path = _find_file(directory, str(year))
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"No incidence data for {country}/{cause}/{year}",
        )

    df = _read_table(file_path)

    records = []
    for _, row in df.iterrows():
        records.append({
            "admin_id": _sanitize(row.get("admin_id")),
            "admin_name": _sanitize(row.get("admin_name")),
            "incidence_rate": _sanitize(row.get("incidence_rate", row.get("rate"))),
            "cause": cause,
        })

    return {"country": country, "cause": cause, "year": year, "units": records}


# ────────────────────────────────────────────────────────────────────
#  4. Demographics (ACS)
# ────────────────────────────────────────────────────────────────────


@router.get("/demographics/{country}/{year}")
async def get_demographics(country: str, year: int):
    """Return GeoJSON with ACS 5-year demographics by census tract.

    Reads from ``data/processed/demographics/{country}/{year}.parquet``
    (output of ``backend/etl/process_acs.py``).
    """
    try:
        directory = _resolve_path("demographics", country)
        file_path = _find_file(directory, str(year))
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"No demographics data for {country}/{year}",
        )

    df = _read_table(file_path)
    return _df_to_geojson(df)


# ────────────────────────────────────────────────────────────────────
#  5. Dataset listing
# ────────────────────────────────────────────────────────────────────


@lru_cache(maxsize=1)
def _scan_datasets() -> list[dict[str, Any]]:
    """Walk DATA_ROOT to discover available datasets and their metadata.

    Returns a list of dataset descriptors with pollutant, source, years,
    and countries.
    """
    datasets: list[dict[str, Any]] = []

    if not DATA_ROOT.exists():
        return datasets

    # Concentration datasets: {pollutant}/{country}/{year}.parquet
    pollutant_names = {"pm25": "PM2.5", "ozone": "Ozone", "no2": "NO2", "so2": "SO2"}
    for pollutant_dir in sorted(DATA_ROOT.iterdir()):
        if not pollutant_dir.is_dir():
            continue
        key = pollutant_dir.name
        if key in ("population", "incidence", "demographics"):
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
                    "years": years,
                    "source": f"Processed {pollutant_names.get(key, key)} raster",
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
        datasets = [d for d in datasets if d.get("country") == country]

    return {"datasets": datasets}
