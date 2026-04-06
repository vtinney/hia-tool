#!/usr/bin/env python3
"""ETL: Extract population-weighted mean PM2.5 by admin unit from a NetCDF.

Takes a van Donkelaar (or similar) PM2.5 NetCDF raster and a country
boundary GeoJSON, computes zonal statistics per admin polygon, and
writes a Parquet file with one row per admin unit.

Usage
-----
    python process_pm25.py \
        --input data/raw/pm25_2020.nc \
        --boundaries data/boundaries/mexico_admin1.geojson \
        --output data/processed/pm25/mexico/2020.parquet

The same pattern can be reused for other pollutants by changing the
variable name (``--variable``) and the input file.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterstats import zonal_stats
from shapely import wkt

logger = logging.getLogger("process_pm25")


# ────────────────────────────────────────────────────────────────────
#  1. Open NetCDF raster
# ────────────────────────────────────────────────────────────────────


def open_raster(path: Path, variable: str | None = None) -> str:
    """Open a NetCDF (or GeoTIFF) and return a rasterio-readable path.

    For NetCDF files with multiple variables, ``variable`` selects the
    subdataset.  If *None*, the first subdataset is used.  GeoTIFF files
    are returned as-is.

    Parameters
    ----------
    path : Path
        Input raster file.
    variable : str or None
        NetCDF variable name (e.g. ``"GWRPM25"``).

    Returns
    -------
    str
        A path string that ``rasterio.open`` can read (may be a
        ``NETCDF:`` subdataset URI).

    Raises
    ------
    FileNotFoundError
        If *path* does not exist.
    ValueError
        If the file has no readable subdatasets and is not a valid raster.
    """
    if not path.exists():
        raise FileNotFoundError(f"Raster file not found: {path}")

    suffix = path.suffix.lower()

    # GeoTIFF or single-band raster — return directly
    if suffix in (".tif", ".tiff", ".geotiff"):
        logger.info("Detected GeoTIFF: %s", path)
        return str(path)

    # NetCDF — resolve subdataset
    with rasterio.open(str(path)) as src:
        subdatasets = src.subdatasets
        if not subdatasets:
            # Single-variable NetCDF that rasterio can read directly
            logger.info("Single-variable NetCDF: %s", path)
            return str(path)

    logger.info("NetCDF has %d subdatasets", len(subdatasets))

    if variable:
        matches = [s for s in subdatasets if variable in s]
        if not matches:
            raise ValueError(
                f"Variable '{variable}' not found in subdatasets: "
                f"{[s.split(':')[-1] for s in subdatasets]}"
            )
        selected = matches[0]
    else:
        selected = subdatasets[0]
        inferred_var = selected.split(":")[-1]
        logger.info("No --variable specified; using first subdataset: %s", inferred_var)

    logger.info("Using subdataset: %s", selected)
    return selected


def log_raster_info(raster_path: str) -> None:
    """Log basic metadata about the raster."""
    with rasterio.open(raster_path) as src:
        logger.info(
            "Raster: %dx%d, CRS=%s, bounds=%s, nodata=%s",
            src.width, src.height, src.crs, src.bounds, src.nodata,
        )


# ────────────────────────────────────────────────────────────────────
#  2. Open boundaries
# ────────────────────────────────────────────────────────────────────


def open_boundaries(path: Path) -> gpd.GeoDataFrame:
    """Read a boundary file and reproject to EPSG:4326 if needed.

    Supports GeoJSON, GeoPackage, shapefile, and zipped shapefiles.

    Parameters
    ----------
    path : Path
        Path to the vector file.

    Returns
    -------
    gpd.GeoDataFrame

    Raises
    ------
    FileNotFoundError
        If *path* does not exist.
    """
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
    """Detect the admin ID and name columns heuristically.

    Returns
    -------
    (id_col, name_col)
        *name_col* may be None if no name column is found.
    """
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
#  3. Compute zonal statistics
# ────────────────────────────────────────────────────────────────────


def compute_pm25_zonal_stats(
    raster_path: str,
    boundaries: gpd.GeoDataFrame,
    all_touched: bool = False,
) -> gpd.GeoDataFrame:
    """Compute mean PM2.5 per admin polygon via zonal statistics.

    Parameters
    ----------
    raster_path : str
        Rasterio-readable path to the PM2.5 raster.
    boundaries : gpd.GeoDataFrame
        Admin boundary polygons.
    all_touched : bool
        Include all pixels touched by a polygon (default: center-only).

    Returns
    -------
    gpd.GeoDataFrame
        Input GeoDataFrame with an added ``mean_pm25`` column.
    """
    n = len(boundaries)
    logger.info("Computing zonal stats for %d polygons...", n)
    t0 = time.perf_counter()

    results = zonal_stats(
        boundaries.geometry,
        raster_path,
        stats=["mean", "count"],
        all_touched=all_touched,
        geojson_out=False,
    )

    elapsed = time.perf_counter() - t0
    logger.info("Zonal stats completed in %.1f s", elapsed)

    out = boundaries.copy()
    out["mean_pm25"] = [r.get("mean") for r in results]
    out["pixel_count"] = [r.get("count", 0) for r in results]

    # Report coverage
    n_valid = out["mean_pm25"].notna().sum()
    n_empty = n - n_valid
    if n_empty > 0:
        logger.warning(
            "%d of %d polygons had no raster coverage (mean_pm25 = NaN)",
            n_empty, n,
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
    """Write the results as a Parquet file.

    Columns: ``admin_id``, ``admin_name``, ``mean_pm25``,
    ``pixel_count``, ``geometry`` (as WKT).

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Result of zonal statistics.
    id_col : str
        Column to use as ``admin_id``.
    name_col : str or None
        Column to use as ``admin_name``.
    output_path : Path
        Destination Parquet file.
    """
    out = gpd.GeoDataFrame()

    if id_col == "index":
        out["admin_id"] = [str(i) for i in range(len(gdf))]
    else:
        out["admin_id"] = gdf[id_col].astype(str).values

    out["admin_name"] = (
        gdf[name_col].astype(str).values if name_col else None
    )

    out["mean_pm25"] = gdf["mean_pm25"].values
    out["pixel_count"] = gdf["pixel_count"].values
    out["geometry"] = gdf.geometry.apply(
        lambda g: g.wkt if g is not None else None
    )

    # Ensure output directory exists
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
        description="Extract population-weighted mean PM2.5 by admin unit.",
    )
    parser.add_argument(
        "--input", required=True, type=Path,
        help="Path to PM2.5 NetCDF (or GeoTIFF) raster file.",
    )
    parser.add_argument(
        "--boundaries", required=True, type=Path,
        help="Path to admin boundary file (GeoJSON, GeoPackage, shapefile ZIP).",
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Output Parquet file path.",
    )
    parser.add_argument(
        "--variable", default=None,
        help="NetCDF variable name (e.g. 'GWRPM25'). Auto-detected if omitted.",
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


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    t_start = time.perf_counter()
    logger.info("Starting PM2.5 ETL pipeline")

    # Step 1: Open raster
    raster_path = open_raster(args.input, variable=args.variable)
    log_raster_info(raster_path)

    # Step 2: Open boundaries
    boundaries = open_boundaries(args.boundaries)

    # Step 3: Zonal statistics
    result_gdf = compute_pm25_zonal_stats(
        raster_path, boundaries, all_touched=args.all_touched,
    )

    # Step 4: Save Parquet
    id_col, name_col = detect_columns(result_gdf)
    save_parquet(result_gdf, id_col, name_col, args.output)

    elapsed = time.perf_counter() - t_start
    logger.info("Pipeline complete in %.1f s", elapsed)


if __name__ == "__main__":
    main()
