#!/usr/bin/env python3
"""ETL: Process WorldPop age-group population rasters into Parquet.

Takes WorldPop total-population-per-age-group GeoTIFFs (the 2024+
release that provides combined male+female totals) and an admin
boundary file, computes zonal sums per polygon, and writes a single
Parquet file with one row per admin unit and columns for each 5-year
age bin.

WorldPop file naming convention (R2025A total population)
----------------------------------------------------------
    global_t_{age}_{year}_{ISO2}_{resolution}_{release}_{variant}_{version}.tif

Examples:

    global_t_0_2015_CN_1km_R2025A_UA_v1.tif    → age 0 (under 1)
    global_t_1_2015_CN_1km_R2025A_UA_v1.tif    → age 1–4
    global_t_5_2015_CN_1km_R2025A_UA_v1.tif    → age 5–9
    global_t_10_2015_CN_1km_R2025A_UA_v1.tif   → age 10–14
    ...
    global_t_80_2015_CN_1km_R2025A_UA_v1.tif   → age 80–84
    global_t_85_2015_CN_1km_R2025A_UA_v1.tif   → age 85–89
    global_t_90_2015_CN_1km_R2025A_UA_v1.tif   → age 90+

Fields:
    - ``global``  — spatial scope
    - ``t``       — total (combined male + female)
    - ``{age}``   — age-group start (0, 1, 5, 10, ..., 80, 85, 90)
    - ``{year}``  — 2015–2030
    - ``{ISO2}``  — 2-letter country code (CN, MX, BR, US, ...)
    - ``1km``     — resolution
    - ``R2025A``  — release identifier
    - ``UA``      — variant (UA = unconstrained adjusted, etc.)
    - ``v1``      — version

Usage
-----
    # Single country / year
    python process_worldpop.py \
        --input-dir data/raw/worldpop/CN \
        --boundaries data/processed/boundaries/china_admin1.geojson \
        --country CN \
        --output data/processed/population/china/2020.parquet

    # Batch: all years for a country
    python process_worldpop.py \
        --input-dir data/raw/worldpop/CN \
        --boundaries data/processed/boundaries/china_admin1.geojson \
        --country CN \
        --output-dir data/processed/population/china \
        --batch

Output schema
-------------
    admin_id, admin_name, total, age_0_4, age_5_9, age_10_14, ...,
    age_80_plus, pixel_count, geometry (WKT)
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterstats import zonal_stats
from shapely import wkt

logger = logging.getLogger("process_worldpop")

# ────────────────────────────────────────────────────────────────────
#  Age-bin definitions
# ────────────────────────────────────────────────────────────────────

# WorldPop age-start codes that map to each 5-year HIA bin.
# Keys: output column name → Values: list of WorldPop age-start codes.
# Codes 0 (<1 yr) and 1 (1–4 yrs) combine into the 0–4 bin.
# Codes 80, 85, 90 (and any higher) combine into 80+.
AGE_BINS: dict[str, list[int]] = {
    "age_0_4":     [0, 1],
    "age_5_9":     [5],
    "age_10_14":   [10],
    "age_15_19":   [15],
    "age_20_24":   [20],
    "age_25_29":   [25],
    "age_30_34":   [30],
    "age_35_39":   [35],
    "age_40_44":   [40],
    "age_45_49":   [45],
    "age_50_54":   [50],
    "age_55_59":   [55],
    "age_60_64":   [60],
    "age_65_69":   [65],
    "age_70_74":   [70],
    "age_75_79":   [75],
    "age_80_plus": [80, 85, 90],
}

# All known age codes — used to assign any unexpected codes ≥80 to age_80_plus
_KNOWN_CODES: set[int] = {code for codes in AGE_BINS.values() for code in codes}


# ────────────────────────────────────────────────────────────────────
#  1. Discover WorldPop rasters
# ────────────────────────────────────────────────────────────────────


def _build_pattern(country: str | None = None) -> re.Pattern:
    """Build the regex for WorldPop R2025A filenames.

    Pattern: ``global_t_{age}_{year}_{ISO2}_{res}_{release}_{variant}_{ver}.tif``

    If *country* is given (2-letter ISO), the pattern is anchored to that
    country code; otherwise it matches any 2-letter code.
    """
    iso = country.upper() if country else r"[A-Z]{2}"
    return re.compile(
        rf"^global_t_(\d+)_(\d{{4}})_{iso}_[^_]+_[^_]+_[^_]+_[^_]+\.tif$",
        re.IGNORECASE,
    )


def discover_rasters(
    input_dir: Path,
    country: str | None = None,
    year: int | None = None,
) -> dict[int, Path]:
    """Find WorldPop total-population GeoTIFFs and map age codes to paths.

    Looks for files matching the R2025A naming convention:
    ``global_t_{age}_{year}_{ISO2}_1km_R2025A_UA_v1.tif``

    Parameters
    ----------
    input_dir : Path
        Directory containing WorldPop GeoTIFFs (flat or year-subdirs).
    country : str or None
        2-letter ISO country code to filter on (e.g. "CN", "MX").
    year : int or None
        If provided, only match files for this year.

    Returns
    -------
    dict[int, Path]
        Mapping of age-start code → GeoTIFF path.

    Raises
    ------
    FileNotFoundError
        If no matching rasters are found.
    """
    pattern = _build_pattern(country)

    rasters: dict[int, Path] = {}
    search_dirs = [input_dir]

    # Also look in year subdirectories
    if year and (input_dir / str(year)).is_dir():
        search_dirs.insert(0, input_dir / str(year))

    for search_dir in search_dirs:
        for tif in sorted(search_dir.glob("*.tif")):
            m = pattern.match(tif.name)
            if not m:
                continue
            age_code = int(m.group(1))
            file_year = int(m.group(2))
            if year and file_year != year:
                continue
            rasters[age_code] = tif

    if not rasters:
        raise FileNotFoundError(
            f"No WorldPop total-population rasters found in {input_dir}"
            + (f" for country {country}" if country else "")
            + (f", year {year}" if year else "")
        )

    logger.info(
        "Found %d age-group rasters%s in %s",
        len(rasters),
        f" for {year}" if year else "",
        input_dir,
    )
    return rasters


def discover_years(input_dir: Path, country: str | None = None) -> list[int]:
    """Detect available years from filenames in input_dir (and subdirs).

    Returns
    -------
    list[int]
        Sorted list of unique years found.
    """
    pattern = _build_pattern(country)
    years: set[int] = set()
    for tif in input_dir.rglob("*.tif"):
        m = pattern.match(tif.name)
        if m:
            years.add(int(m.group(2)))
    return sorted(years)


# ────────────────────────────────────────────────────────────────────
#  2. Open boundaries (reuses pattern from process_pm25.py)
# ────────────────────────────────────────────────────────────────────


def open_boundaries(path: Path) -> gpd.GeoDataFrame:
    """Read a boundary file and reproject to EPSG:4326 if needed."""
    if not path.exists():
        raise FileNotFoundError(f"Boundary file not found: {path}")

    read_path = f"zip://{path}" if path.suffix == ".zip" else str(path)
    gdf = gpd.read_file(read_path)

    if gdf.crs and not gdf.crs.equals("EPSG:4326"):
        logger.info("Reprojecting boundaries from %s to EPSG:4326", gdf.crs)
        gdf = gdf.to_crs("EPSG:4326")

    logger.info("Loaded %d admin polygons from %s", len(gdf), path.name)
    return gdf


def detect_columns(gdf: gpd.GeoDataFrame) -> tuple[str, str | None]:
    """Detect the admin ID and name columns heuristically."""
    id_candidates = [
        "GEOID", "geoid", "GEO_ID", "FIPS", "fips",
        "ADM1_CODE", "ADM2_CODE", "ADM1_PCODE", "ADM2_PCODE",
        "ISO_A3", "iso", "id", "ID", "FID", "fid",
    ]
    name_candidates = [
        "NAME", "name", "Name", "ADM1_NAME", "ADM2_NAME",
        "ADM1_EN", "ADM2_EN", "NAMELSAD", "STATE_NAME", "COUNTY",
    ]

    id_col = next((c for c in id_candidates if c in gdf.columns), None)
    if id_col is None:
        non_geom = [c for c in gdf.columns if c != "geometry"]
        id_col = non_geom[0] if non_geom else "index"

    name_col = next((c for c in name_candidates if c in gdf.columns), None)
    logger.info("ID column: %s | Name column: %s", id_col, name_col)
    return id_col, name_col


# ────────────────────────────────────────────────────────────────────
#  3. Compute zonal population sums per age group
# ────────────────────────────────────────────────────────────────────


def compute_population_by_age(
    rasters: dict[int, Path],
    boundaries: gpd.GeoDataFrame,
    all_touched: bool = False,
) -> gpd.GeoDataFrame:
    """Run zonal sum for each age-group raster and aggregate into bins.

    Parameters
    ----------
    rasters : dict[int, Path]
        Age-code → GeoTIFF path (output of ``discover_rasters``).
    boundaries : gpd.GeoDataFrame
        Admin boundary polygons.
    all_touched : bool
        Include all pixels touched by a polygon.

    Returns
    -------
    gpd.GeoDataFrame
        Input GeoDataFrame with added columns: ``total``, one per
        age bin (``age_0_4``, ..., ``age_80_plus``), and ``pixel_count``.
    """
    n_zones = len(boundaries)
    out = boundaries.copy()

    # Accumulate zonal sums keyed by WorldPop age code
    age_code_sums: dict[int, np.ndarray] = {}

    for age_code, tif_path in sorted(rasters.items()):
        logger.info("  Zonal sum for age code %d → %s", age_code, tif_path.name)

        results = zonal_stats(
            boundaries.geometry,
            str(tif_path),
            stats=["sum", "count"],
            all_touched=all_touched,
            geojson_out=False,
        )

        sums = np.array(
            [r.get("sum", 0) or 0 for r in results], dtype=np.float64
        )
        age_code_sums[age_code] = sums

        # Track pixel count from the first raster (they share the same grid)
        if "pixel_count" not in out.columns:
            out["pixel_count"] = [r.get("count", 0) for r in results]

    # Catch any age codes ≥80 not explicitly listed (e.g. 95, 100)
    # and fold them into age_80_plus
    extra_80_plus = [
        code for code in age_code_sums
        if code >= 80 and code not in _KNOWN_CODES
    ]
    if extra_80_plus:
        logger.info(
            "Folding extra age codes %s into age_80_plus", extra_80_plus,
        )

    # Map individual age-code sums into 5-year bins
    total = np.zeros(n_zones, dtype=np.float64)

    for bin_name, age_codes in AGE_BINS.items():
        bin_sum = np.zeros(n_zones, dtype=np.float64)
        codes_to_sum = (
            age_codes + extra_80_plus if bin_name == "age_80_plus"
            else age_codes
        )
        for code in codes_to_sum:
            if code in age_code_sums:
                bin_sum += age_code_sums[code]
            else:
                logger.warning(
                    "Age code %d not found in rasters — %s will be incomplete",
                    code, bin_name,
                )
        out[bin_name] = bin_sum
        total += bin_sum

    out["total"] = total

    # Report coverage
    n_empty = (total == 0).sum()
    if n_empty > 0:
        logger.warning(
            "%d of %d polygons had zero total population", n_empty, n_zones
        )

    return out


# ────────────────────────────────────────────────────────────────────
#  4. Save as Parquet
# ────────────────────────────────────────────────────────────────────


def save_parquet(
    gdf: gpd.GeoDataFrame,
    id_col: str,
    name_col: str | None,
    output_path: Path,
) -> None:
    """Write population results as a Parquet file.

    Columns: admin_id, admin_name, total, age_0_4, ..., age_80_plus,
    pixel_count, geometry (WKT).
    """
    out = pd.DataFrame()

    if id_col == "index":
        out["admin_id"] = [str(i) for i in range(len(gdf))]
    else:
        out["admin_id"] = gdf[id_col].astype(str).values

    out["admin_name"] = (
        gdf[name_col].astype(str).values if name_col else None
    )

    out["total"] = gdf["total"].values

    # Age-bin columns in order
    for bin_name in AGE_BINS:
        if bin_name in gdf.columns:
            out[bin_name] = gdf[bin_name].values

    out["pixel_count"] = gdf["pixel_count"].values

    out["geometry"] = gdf.geometry.apply(
        lambda g: g.wkt if g is not None else None
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_path, index=False, engine="pyarrow")

    logger.info(
        "Saved %d rows to %s (%.1f KB)",
        len(out), output_path, output_path.stat().st_size / 1024,
    )


# ────────────────────────────────────────────────────────────────────
#  CLI
# ────────────────────────────────────────────────────────────────────


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process WorldPop age-group GeoTIFFs into population Parquet.",
    )
    parser.add_argument(
        "--input-dir", required=True, type=Path,
        help=(
            "Directory containing WorldPop GeoTIFFs. "
            "For single-year mode, point to the folder with the TIFs. "
            "For --batch mode, point to the country folder containing "
            "year subdirectories or mixed-year TIFs."
        ),
    )
    parser.add_argument(
        "--boundaries", required=True, type=Path,
        help="Path to admin boundary file (GeoJSON, GeoPackage, shapefile ZIP).",
    )
    parser.add_argument(
        "--country", default=None,
        help=(
            "2-letter ISO country code (e.g. CN, MX, BR). "
            "Filters filenames to this country. Required when the "
            "input directory contains files for multiple countries."
        ),
    )
    parser.add_argument(
        "--output", type=Path, default=None,
        help="Output Parquet file path (single-year mode).",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=None,
        help="Output directory for batch mode (creates {year}.parquet per year).",
    )
    parser.add_argument(
        "--year", type=int, default=None,
        help="Year to process (auto-detected from filenames if omitted).",
    )
    parser.add_argument(
        "--batch", action="store_true",
        help="Process all available years and write one Parquet per year.",
    )
    parser.add_argument(
        "--all-touched", action="store_true",
        help="Include all pixels touched by a polygon (default: center-only).",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args(argv)


def process_single_year(
    input_dir: Path,
    boundaries: gpd.GeoDataFrame,
    id_col: str,
    name_col: str | None,
    year: int,
    output_path: Path,
    country: str | None = None,
    all_touched: bool = False,
) -> None:
    """Process one country-year and write Parquet."""
    logger.info("Processing year %d", year)
    t0 = time.perf_counter()

    rasters = discover_rasters(input_dir, country=country, year=year)
    result_gdf = compute_population_by_age(
        rasters, boundaries, all_touched=all_touched,
    )
    save_parquet(result_gdf, id_col, name_col, output_path)

    elapsed = time.perf_counter() - t0
    logger.info("Year %d complete in %.1f s", year, elapsed)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    t_start = time.perf_counter()
    logger.info("Starting WorldPop ETL pipeline")

    # Validate args
    if not args.batch and args.output is None:
        logger.error("--output is required in single-year mode")
        sys.exit(1)
    if args.batch and args.output_dir is None:
        logger.error("--output-dir is required in --batch mode")
        sys.exit(1)

    # Load boundaries once (shared across all years)
    boundaries = open_boundaries(args.boundaries)
    id_col, name_col = detect_columns(boundaries)

    country = args.country

    if args.batch:
        years = discover_years(args.input_dir, country=country)
        if not years:
            logger.error("No years found in %s", args.input_dir)
            sys.exit(1)
        logger.info("Batch mode: found years %s", years)

        for year in years:
            output_path = args.output_dir / f"{year}.parquet"
            process_single_year(
                args.input_dir, boundaries, id_col, name_col,
                year, output_path, country=country,
                all_touched=args.all_touched,
            )
    else:
        # Single-year mode — detect year from files if not specified
        if args.year:
            year = args.year
        else:
            years = discover_years(args.input_dir, country=country)
            if len(years) != 1:
                logger.error(
                    "Found %d years (%s) — specify --year or use --batch",
                    len(years), years,
                )
                sys.exit(1)
            year = years[0]

        process_single_year(
            args.input_dir, boundaries, id_col, name_col,
            year, args.output, country=country,
            all_touched=args.all_touched,
        )

    elapsed = time.perf_counter() - t_start
    logger.info("Pipeline complete in %.1f s", elapsed)


if __name__ == "__main__":
    main()
