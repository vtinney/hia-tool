#!/usr/bin/env python3
"""ETL: Process WHO Ambient Air Pollution Database into boundary-level Parquet.

Reads the WHO AAP city-level monitoring Excel, creates point geometries from
lat/lon, spatial-joins them to three boundary sets (NE countries, NE states,
GHS SMOD urban centres), and aggregates mean PM2.5 per polygon per year.

Usage
-----
    python process_who_aap.py \
        --input data/raw/who_aap/who_aap_v6.1.xlsx \
        --boundaries-dir data/raw/boundaries \
        --output-dir data/processed/who_aap \
        --years 2015-2021

Output structure
----------------
    {output-dir}/
        ne_countries/{year}.parquet
        ne_states/{year}.parquet
        ghs_smod/{year}.parquet
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

logger = logging.getLogger("process_who_aap")

# ---------------------------------------------------------------------------
#  Constants
# ---------------------------------------------------------------------------

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
        "name_col": None,  # no name column; we'll use ID
        "buffer_km": 5,
    },
}

WHO_SHEET = "Update 2024 (V6.1)"


# ---------------------------------------------------------------------------
#  1. Load WHO data
# ---------------------------------------------------------------------------


def load_who_data(
    path: Path,
    year_min: int,
    year_max: int,
) -> gpd.GeoDataFrame:
    """Read WHO AAP Excel, filter to PM2.5 rows in the year range.

    Returns a GeoDataFrame with point geometries (EPSG:4326).
    """
    logger.info("Reading WHO AAP from %s", path)
    df = pd.read_excel(path, sheet_name=WHO_SHEET)
    logger.info("Raw rows: %d", len(df))

    # Drop rows without PM2.5
    df = df.dropna(subset=["pm25_concentration"])
    logger.info("Rows with PM2.5: %d", len(df))

    # Drop rows without valid year or coordinates
    df = df.dropna(subset=["year", "latitude", "longitude"])
    df["year"] = df["year"].astype(int)

    # Filter year range
    df = df[(df["year"] >= year_min) & (df["year"] <= year_max)]
    logger.info("Rows in %d–%d: %d", year_min, year_max, len(df))

    # Build point geometries
    geometry = [Point(lon, lat) for lon, lat in zip(df["longitude"], df["latitude"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    return gdf


# ---------------------------------------------------------------------------
#  2. Load boundaries
# ---------------------------------------------------------------------------


def load_boundaries(
    boundaries_dir: Path,
    config: dict,
) -> gpd.GeoDataFrame:
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
    config: dict,
) -> pd.DataFrame:
    """Join city points into boundary polygons and aggregate PM2.5 by year.

    For GHS SMOD, applies a buffer (in km) around each urban centre polygon
    to catch city points that fall just outside the boundary.

    Returns a DataFrame with columns:
        admin_id, admin_name, year, mean_pm25, station_count, geometry (WKT)
    """
    id_col = config["id_col"]
    name_col = config["name_col"]
    buffer_km = config["buffer_km"]

    polys = boundaries.copy()

    # Apply buffer if needed (project to metric CRS, buffer, back to 4326)
    if buffer_km > 0:
        logger.info("Applying %d km buffer to polygons", buffer_km)
        polys_metric = polys.to_crs("EPSG:3857")
        polys_metric["geometry"] = polys_metric.geometry.buffer(buffer_km * 1000)
        polys = polys_metric.to_crs("EPSG:4326")

    # Spatial join: assign each city point to a polygon
    joined = gpd.sjoin(points, polys, how="inner", predicate="within")
    logger.info("Points matched: %d / %d", len(joined), len(points))

    if len(joined) == 0:
        logger.warning("No points matched any polygon — returning empty DataFrame")
        return pd.DataFrame(
            columns=["admin_id", "admin_name", "year", "mean_pm25",
                      "station_count", "geometry"]
        )

    # Aggregate: mean PM2.5 and count per polygon per year
    group_cols = [id_col, "year"]
    agg = (
        joined
        .groupby(group_cols, as_index=False)
        .agg(
            mean_pm25=("pm25_concentration", "mean"),
            station_count=("pm25_concentration", "count"),
        )
    )

    # Attach polygon name and geometry (WKT)
    # Deduplicate: keep first occurrence per ID (handles multi-polygon countries)
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

    return agg[["admin_id", "admin_name", "year", "mean_pm25",
                "station_count", "geometry"]]


# ---------------------------------------------------------------------------
#  4. Write Parquet files (one per year)
# ---------------------------------------------------------------------------


def write_parquet_by_year(
    df: pd.DataFrame,
    output_dir: Path,
    boundary_name: str,
) -> None:
    """Write one Parquet file per year under {output_dir}/{boundary_name}/."""
    if df.empty:
        logger.warning("No data for %s — skipping", boundary_name)
        return

    out_dir = output_dir / boundary_name
    out_dir.mkdir(parents=True, exist_ok=True)

    for year, group in df.groupby("year"):
        out_path = out_dir / f"{year}.parquet"
        group.drop(columns=["year"]).to_parquet(
            out_path, engine="pyarrow", index=False,
        )
        logger.info(
            "  %s: %d rows → %s (%.1f KB)",
            year, len(group), out_path, out_path.stat().st_size / 1024,
        )


# ---------------------------------------------------------------------------
#  CLI
# ---------------------------------------------------------------------------


def parse_year_range(s: str) -> tuple[int, int]:
    """Parse '2015-2021' into (2015, 2021)."""
    parts = s.split("-")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(f"Expected YYYY-YYYY, got '{s}'")
    return int(parts[0]), int(parts[1])


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process WHO AAP city monitoring data into boundary-level Parquet.",
    )
    parser.add_argument(
        "--input", required=True, type=Path,
        help="Path to WHO AAP Excel file (e.g. who_aap_v6.1.xlsx).",
    )
    parser.add_argument(
        "--boundaries-dir", required=True, type=Path,
        help="Root directory containing boundary subdirectories "
             "(natural_earth_gee/, GHS_SMOD/).",
    )
    parser.add_argument(
        "--output-dir", required=True, type=Path,
        help="Output root. Files written to {output-dir}/{boundary_type}/{year}.parquet.",
    )
    parser.add_argument(
        "--years", default="2015-2021", type=parse_year_range,
        help="Year range to process, e.g. '2015-2021' (default: 2015-2021).",
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

    logger.info("WHO AAP ETL: years %d–%d, boundaries: %s",
                year_min, year_max, args.boundaries)

    # Step 1: Load WHO point data
    points = load_who_data(args.input, year_min, year_max)

    # Step 2: Process each boundary set
    for bname in args.boundaries:
        logger.info("── Processing boundary set: %s ──", bname)
        config = BOUNDARY_CONFIGS[bname]

        try:
            polys = load_boundaries(args.boundaries_dir, config)
        except FileNotFoundError as e:
            logger.error("Skipping %s: %s", bname, e)
            continue

        agg = spatial_join_and_aggregate(points, polys, config)
        write_parquet_by_year(agg, args.output_dir, bname)

        n_years = agg["year"].nunique() if not agg.empty else 0
        n_polys = agg["admin_id"].nunique() if not agg.empty else 0
        logger.info(
            "  %s complete: %d rows, %d polygons, %d years",
            bname, len(agg), n_polys, n_years,
        )

    elapsed = time.perf_counter() - t_start
    logger.info("WHO AAP ETL finished in %.1f s", elapsed)


if __name__ == "__main__":
    main()
