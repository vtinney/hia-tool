#!/usr/bin/env python3
"""ETL: Process EPA AQS annual monitor data into boundary-level Parquet.

Reads yearly EPA AQS annual_conc_by_monitor ZIP files, filters to target
pollutants, spatial-joins monitor points to boundary sets (NE countries,
NE states, GHS SMOD urban centres), aggregates mean concentration per
polygon per year, and writes Parquet output.

Usage
-----
    python process_epa_aqs.py \
        --input-dir data/raw/epa_aqs \
        --boundaries-dir data/raw/boundaries \
        --output-dir data/processed/epa_aqs \
        --years 2015-2024

Output structure
----------------
    {output-dir}/
        pm25/
            ne_countries/{year}.parquet
            ne_states/{year}.parquet
            ghs_smod/{year}.parquet
        ozone/
            ...
        no2/
            ...
        so2/
            ...
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

logger = logging.getLogger("process_epa_aqs")

# ---------------------------------------------------------------------------
#  Constants
# ---------------------------------------------------------------------------

POLLUTANT_CONFIGS = {
    "pm25": {
        "parameter_code": 88101,
        "sample_duration": "24 HOUR",
        "parameter_label": "PM2.5 - Local Conditions",
        "units": "µg/m³",
        "mean_col": "mean_pm25",
    },
    "ozone": {
        "parameter_code": 44201,
        "sample_duration": "8-HR RUN AVG BEGIN HOUR",
        "parameter_label": "Ozone",
        "units": "ppm",
        "mean_col": "mean_ozone",
    },
    "no2": {
        "parameter_code": 42602,
        "sample_duration": "1 HOUR",
        "parameter_label": "Nitrogen dioxide (NO2)",
        "units": "ppb",
        "mean_col": "mean_no2",
    },
    "so2": {
        "parameter_code": 42401,
        "sample_duration": "1 HOUR",
        "parameter_label": "Sulfur dioxide",
        "units": "ppb",
        "mean_col": "mean_so2",
    },
}

BOUNDARY_CONFIGS = {
    "ne_countries": {
        "path": "natural_earth_gee/ne_countries/ne_countries.shp",
        "id_col": "ISO_A3",
        "name_col": "NAME",
        "buffer_km": 0,
    },
    "ne_states": {
        "path": "natural_earth_gee/ne_states/ne_states.shp",
        "id_col": "iso_3166_2",
        "name_col": "name",
        "buffer_km": 0,
    },
    "ghs_smod": {
        "path": "GHS_SMOD/GHS_SMOD_E2020_GLOBE_R2023A_54009_1000_UC_V2_0.shp",
        "id_col": "ID_UC_G0",
        "name_col": None,
        "buffer_km": 5,
    },
}


# ---------------------------------------------------------------------------
#  1. Load EPA AQS data
# ---------------------------------------------------------------------------


def load_aqs_year(
    input_dir: Path,
    year: int,
    pollutant: str,
) -> gpd.GeoDataFrame | None:
    """Load one year of EPA AQS data, filtered to a single pollutant.

    Reads the ZIP, filters by parameter code and sample duration,
    deduplicates to one row per monitor, and returns a GeoDataFrame
    with point geometries.
    """
    zip_path = input_dir / f"annual_conc_by_monitor_{year}.zip"
    if not zip_path.exists():
        logger.warning("File not found, skipping: %s", zip_path)
        return None

    pcfg = POLLUTANT_CONFIGS[pollutant]

    with zipfile.ZipFile(zip_path) as z:
        csv_name = next(n for n in z.namelist() if n.endswith(".csv"))
        df = pd.read_csv(z.open(csv_name), low_memory=False)

    logger.debug("  %d: raw rows = %d", year, len(df))

    # Filter to target pollutant and sample duration
    mask = (
        (df["Parameter Code"] == pcfg["parameter_code"])
        & (df["Sample Duration"] == pcfg["sample_duration"])
    )
    df = df[mask].copy()
    logger.debug("  %d: after pollutant filter = %d", year, len(df))

    if df.empty:
        return None

    # Deduplicate: one row per monitor (State + County + Site + POC).
    # Multiple rows exist per monitor due to different Pollutant Standards
    # (e.g. PM25 24-hour 1997, 2006, 2012). The Arithmetic Mean is identical
    # across standards, so we keep the first.
    df = df.drop_duplicates(
        subset=["State Code", "County Code", "Site Num", "POC"],
        keep="first",
    )
    logger.debug("  %d: after dedup = %d monitors", year, len(df))

    # Drop rows with missing coordinates or mean
    df = df.dropna(subset=["Latitude", "Longitude", "Arithmetic Mean"])

    # Build point geometries
    geometry = [
        Point(lon, lat)
        for lon, lat in zip(df["Longitude"], df["Latitude"])
    ]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    gdf["year"] = year

    return gdf


# ---------------------------------------------------------------------------
#  2. Load boundaries (reused from WHO AAP pattern)
# ---------------------------------------------------------------------------


def load_boundaries(boundaries_dir: Path, config: dict) -> gpd.GeoDataFrame:
    """Load a boundary shapefile and reproject to EPSG:4326 if needed."""
    path = boundaries_dir / config["path"]
    if not path.exists():
        raise FileNotFoundError(f"Boundary file not found: {path}")

    gdf = gpd.read_file(str(path))
    logger.info(
        "Loaded %d polygons from %s (CRS: %s)",
        len(gdf), path.name, gdf.crs,
    )

    if gdf.crs and not gdf.crs.equals("EPSG:4326"):
        logger.info("Reprojecting from %s to EPSG:4326", gdf.crs)
        gdf = gdf.to_crs("EPSG:4326")

    return gdf


# ---------------------------------------------------------------------------
#  3. Spatial join + aggregation
# ---------------------------------------------------------------------------


def spatial_join_and_aggregate(
    points: gpd.GeoDataFrame,
    boundaries: gpd.GeoDataFrame,
    boundary_config: dict,
    pollutant: str,
) -> pd.DataFrame:
    """Join monitor points to boundary polygons and aggregate by year.

    Returns DataFrame with: admin_id, admin_name, year, mean_{pollutant},
    station_count, geometry (WKT).
    """
    id_col = boundary_config["id_col"]
    name_col = boundary_config["name_col"]
    buffer_km = boundary_config["buffer_km"]
    mean_col = POLLUTANT_CONFIGS[pollutant]["mean_col"]

    polys = boundaries.copy()

    if buffer_km > 0:
        logger.info("Applying %d km buffer to polygons", buffer_km)
        polys_metric = polys.to_crs("EPSG:3857")
        polys_metric["geometry"] = polys_metric.geometry.buffer(buffer_km * 1000)
        polys = polys_metric.to_crs("EPSG:4326")

    joined = gpd.sjoin(points, polys, how="inner", predicate="within")
    logger.info("Points matched: %d / %d", len(joined), len(points))

    if len(joined) == 0:
        return pd.DataFrame(
            columns=["admin_id", "admin_name", "year", mean_col,
                      "station_count", "geometry"]
        )

    group_cols = [id_col, "year"]
    agg = (
        joined
        .groupby(group_cols, as_index=False)
        .agg(
            **{mean_col: ("Arithmetic Mean", "mean")},
            station_count=("Arithmetic Mean", "count"),
        )
    )

    # Attach name and geometry from original (unbuffered) boundaries
    poly_dedup = boundaries.drop_duplicates(subset=[id_col]).set_index(id_col)

    if name_col and name_col in poly_dedup.columns:
        agg["admin_name"] = agg[id_col].map(poly_dedup[name_col])
    else:
        agg["admin_name"] = agg[id_col].astype(str)

    agg["geometry"] = agg[id_col].map(
        poly_dedup.geometry.apply(lambda g: g.wkt if g is not None else None)
    )

    agg = agg.rename(columns={id_col: "admin_id"})
    agg["admin_id"] = agg["admin_id"].astype(str)

    return agg[["admin_id", "admin_name", "year", mean_col,
                "station_count", "geometry"]]


# ---------------------------------------------------------------------------
#  4. Write Parquet files
# ---------------------------------------------------------------------------


def write_parquet_by_year(
    df: pd.DataFrame,
    output_dir: Path,
    pollutant: str,
    boundary_name: str,
) -> None:
    """Write one Parquet per year under {output_dir}/{pollutant}/{boundary_name}/."""
    if df.empty:
        logger.warning("No data for %s/%s — skipping", pollutant, boundary_name)
        return

    out_dir = output_dir / pollutant / boundary_name
    out_dir.mkdir(parents=True, exist_ok=True)

    for year, group in df.groupby("year"):
        out_path = out_dir / f"{year}.parquet"
        group.drop(columns=["year"]).to_parquet(
            out_path, engine="pyarrow", index=False,
        )
        logger.info(
            "    %s: %d rows → %s (%.1f KB)",
            year, len(group), out_path, out_path.stat().st_size / 1024,
        )


# ---------------------------------------------------------------------------
#  CLI
# ---------------------------------------------------------------------------


def parse_year_range(s: str) -> tuple[int, int]:
    parts = s.split("-")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(f"Expected YYYY-YYYY, got '{s}'")
    return int(parts[0]), int(parts[1])


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process EPA AQS monitor data into boundary-level Parquet.",
    )
    parser.add_argument(
        "--input-dir", required=True, type=Path,
        help="Directory containing annual_conc_by_monitor_YYYY.zip files.",
    )
    parser.add_argument(
        "--boundaries-dir", required=True, type=Path,
        help="Root directory containing boundary subdirectories.",
    )
    parser.add_argument(
        "--output-dir", required=True, type=Path,
        help="Output root. Files at {output-dir}/{pollutant}/{boundary}/{year}.parquet.",
    )
    parser.add_argument(
        "--years", default="2015-2024", type=parse_year_range,
        help="Year range to process (default: 2015-2024).",
    )
    parser.add_argument(
        "--pollutants", nargs="*", default=list(POLLUTANT_CONFIGS.keys()),
        choices=list(POLLUTANT_CONFIGS.keys()),
        help="Which pollutants to process (default: all).",
    )
    parser.add_argument(
        "--boundaries", nargs="*", default=list(BOUNDARY_CONFIGS.keys()),
        choices=list(BOUNDARY_CONFIGS.keys()),
        help="Which boundary sets to process (default: all).",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    t_start = time.perf_counter()
    year_min, year_max = args.years
    years = list(range(year_min, year_max + 1))

    logger.info(
        "EPA AQS ETL: years %d–%d, pollutants: %s, boundaries: %s",
        year_min, year_max, args.pollutants, args.boundaries,
    )

    # Pre-load boundaries (shared across pollutants and years)
    boundary_gdfs = {}
    for bname in args.boundaries:
        try:
            boundary_gdfs[bname] = load_boundaries(
                args.boundaries_dir, BOUNDARY_CONFIGS[bname],
            )
        except FileNotFoundError as e:
            logger.error("Skipping boundary %s: %s", bname, e)

    # Process each pollutant
    for pollutant in args.pollutants:
        logger.info("═══ Pollutant: %s ═══", pollutant)

        # Load all years for this pollutant
        frames = []
        for year in years:
            gdf = load_aqs_year(args.input_dir, year, pollutant)
            if gdf is not None and len(gdf) > 0:
                frames.append(gdf)

        if not frames:
            logger.warning("No data found for %s — skipping", pollutant)
            continue

        all_points = pd.concat(frames, ignore_index=True)
        all_points = gpd.GeoDataFrame(all_points, geometry="geometry", crs="EPSG:4326")
        logger.info(
            "  Total monitors across all years: %d (%d unique years)",
            len(all_points), all_points["year"].nunique(),
        )

        # Spatial join to each boundary set
        for bname, polys in boundary_gdfs.items():
            logger.info("  ── Boundary: %s ──", bname)
            agg = spatial_join_and_aggregate(
                all_points, polys, BOUNDARY_CONFIGS[bname], pollutant,
            )
            write_parquet_by_year(agg, args.output_dir, pollutant, bname)

            n_years = agg["year"].nunique() if not agg.empty else 0
            n_polys = agg["admin_id"].nunique() if not agg.empty else 0
            logger.info(
                "    %s/%s: %d rows, %d polygons, %d years",
                pollutant, bname, len(agg), n_polys, n_years,
            )

    elapsed = time.perf_counter() - t_start
    logger.info("EPA AQS ETL finished in %.1f s", elapsed)


if __name__ == "__main__":
    main()
