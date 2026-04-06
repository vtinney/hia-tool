"""Geospatial processing service for the HIA engine.

Provides raster/vector I/O, zonal statistics, and the spatial input
preparation pipeline that feeds NumPy arrays into ``compute_hia``.

Dependencies: rasterio, geopandas, rasterstats, shapely, pyproj.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import rasterio
from rasterstats import zonal_stats

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────
#  Raster I/O
# ────────────────────────────────────────────────────────────────────


def read_raster(path: str | Path) -> dict[str, Any]:
    """Read a GeoTIFF raster and return its data + metadata.

    Parameters
    ----------
    path : str or Path
        Path to the raster file.

    Returns
    -------
    dict
        Keys: ``data`` (np.ndarray, band 1), ``transform``,
        ``crs``, ``nodata``, ``bounds``.
    """
    with rasterio.open(path) as src:
        data = src.read(1, masked=True)  # band 1, masked nodata
        return {
            "data": data,
            "transform": src.transform,
            "crs": src.crs,
            "nodata": src.nodata,
            "bounds": {
                "west": src.bounds.left,
                "south": src.bounds.bottom,
                "east": src.bounds.right,
                "north": src.bounds.top,
            },
        }


def validate_raster(path: str | Path) -> dict[str, Any]:
    """Extract metadata from a raster without reading the full array.

    Parameters
    ----------
    path : str or Path
        Path to the raster file.

    Returns
    -------
    dict
        Keys: ``crs``, ``bounds``, ``resolution``, ``shape``,
        ``band_count``, ``dtype``, ``nodata``.
    """
    with rasterio.open(path) as src:
        return {
            "crs": str(src.crs) if src.crs else None,
            "bounds": {
                "west": src.bounds.left,
                "south": src.bounds.bottom,
                "east": src.bounds.right,
                "north": src.bounds.top,
            },
            "resolution": list(src.res),
            "shape": [src.height, src.width],
            "band_count": src.count,
            "dtype": str(src.dtypes[0]),
            "nodata": src.nodata,
        }


# ────────────────────────────────────────────────────────────────────
#  Vector I/O
# ────────────────────────────────────────────────────────────────────


def read_boundaries(path: str | Path) -> gpd.GeoDataFrame:
    """Read a vector boundary file (shapefile ZIP, GeoPackage, or GeoJSON).

    Automatically handles zipped shapefiles via the ``zip://`` prefix.
    Reprojects to EPSG:4326 if the source CRS differs.

    Parameters
    ----------
    path : str or Path
        Path to the vector file.

    Returns
    -------
    gpd.GeoDataFrame
    """
    path = Path(path)
    read_path = f"zip://{path}" if path.suffix == ".zip" else str(path)
    gdf = gpd.read_file(read_path)

    if gdf.crs and not gdf.crs.equals("EPSG:4326"):
        logger.info("Reprojecting boundaries from %s to EPSG:4326", gdf.crs)
        gdf = gdf.to_crs("EPSG:4326")

    return gdf


def validate_boundaries(path: str | Path) -> dict[str, Any]:
    """Extract metadata from a vector file without loading all geometries.

    Parameters
    ----------
    path : str or Path
        Path to the vector file.

    Returns
    -------
    dict
        Keys: ``crs``, ``bounds``, ``feature_count``,
        ``geometry_types``, ``columns``.
    """
    gdf = read_boundaries(path)
    bounds = gdf.total_bounds
    return {
        "crs": str(gdf.crs) if gdf.crs else None,
        "bounds": {
            "west": float(bounds[0]),
            "south": float(bounds[1]),
            "east": float(bounds[2]),
            "north": float(bounds[3]),
        },
        "feature_count": len(gdf),
        "geometry_types": gdf.geometry.geom_type.unique().tolist(),
        "columns": [c for c in gdf.columns if c != "geometry"],
    }


# ────────────────────────────────────────────────────────────────────
#  Zonal Statistics
# ────────────────────────────────────────────────────────────────────


def compute_zonal_stats(
    raster_path: str | Path,
    boundaries: gpd.GeoDataFrame,
    stats: list[str] | None = None,
    all_touched: bool = False,
) -> gpd.GeoDataFrame:
    """Compute zonal statistics of a raster within each polygon.

    Wraps ``rasterstats.zonal_stats`` and merges results back into
    the GeoDataFrame as new columns.

    Parameters
    ----------
    raster_path : str or Path
        Path to the raster file.
    boundaries : gpd.GeoDataFrame
        Boundary polygons.
    stats : list of str, optional
        Statistics to compute (e.g. ``["mean"]``, ``["sum"]``).
        Defaults to ``["mean"]``.
    all_touched : bool
        If True, all pixels touched by a polygon are included.
        Default False (only pixels whose center is within the polygon).

    Returns
    -------
    gpd.GeoDataFrame
        Input GeoDataFrame with additional columns for each stat.
    """
    if stats is None:
        stats = ["mean"]

    results = zonal_stats(
        boundaries.geometry,
        str(raster_path),
        stats=stats,
        all_touched=all_touched,
        geojson_out=False,
    )

    for stat in stats:
        boundaries = boundaries.copy()
        boundaries[stat] = [r.get(stat) for r in results]

    return boundaries


# ────────────────────────────────────────────────────────────────────
#  Zone ID detection
# ────────────────────────────────────────────────────────────────────


def _detect_id_column(gdf: gpd.GeoDataFrame) -> str:
    """Heuristically detect the zone ID column in a GeoDataFrame.

    Looks for common ID column names. Falls back to the DataFrame index.
    """
    candidates = [
        "GEOID", "geoid", "GEO_ID", "FIPS", "fips",
        "ADM1_CODE", "ADM2_CODE", "ISO_A3", "iso",
        "id", "ID", "FID", "fid", "NAME", "name",
    ]
    for col in candidates:
        if col in gdf.columns:
            return col
    # Fall back to first non-geometry column
    non_geom = [c for c in gdf.columns if c != "geometry"]
    return non_geom[0] if non_geom else "index"


def _detect_name_column(gdf: gpd.GeoDataFrame) -> str | None:
    """Heuristically detect the zone name column."""
    candidates = [
        "NAME", "name", "Name", "ADM1_NAME", "ADM2_NAME",
        "NAMELSAD", "STATE_NAME", "COUNTY",
    ]
    for col in candidates:
        if col in gdf.columns:
            return col
    return None


# ────────────────────────────────────────────────────────────────────
#  Spatial Input Orchestrator
# ────────────────────────────────────────────────────────────────────


def prepare_spatial_inputs(
    concentration_raster_path: str | Path,
    population_raster_path: str | Path,
    boundary_path: str | Path,
    control_raster_path: str | Path | None = None,
    control_value: float | None = None,
) -> dict[str, Any]:
    """Full spatial pipeline: read inputs, compute zonal stats, return arrays.

    This is the main entry point called by the spatial compute endpoint.

    Parameters
    ----------
    concentration_raster_path : path
        Baseline concentration raster (GeoTIFF).
    population_raster_path : path
        Population raster (GeoTIFF).
    boundary_path : path
        Study area boundary file (shapefile ZIP, GeoPackage, or GeoJSON).
    control_raster_path : path, optional
        Control/counterfactual concentration raster.
    control_value : float, optional
        Scalar control concentration (used if no control raster provided).

    Returns
    -------
    dict
        Keys:

        - ``zone_ids``: list[str] — ID per spatial unit
        - ``zone_names``: list[str | None] — name per unit (if available)
        - ``c_baseline``: np.ndarray shape (n_zones,)
        - ``c_control``: np.ndarray shape (n_zones,)
        - ``population``: np.ndarray shape (n_zones,)
        - ``geometries``: list[dict] — GeoJSON geometry per zone

    Raises
    ------
    ValueError
        If neither control raster nor control value is provided.
    """
    logger.info("Reading boundaries from %s", boundary_path)
    boundaries = read_boundaries(boundary_path)
    n_zones = len(boundaries)
    logger.info("Found %d zones in boundary file", n_zones)

    # Detect ID and name columns
    id_col = _detect_id_column(boundaries)
    name_col = _detect_name_column(boundaries)

    zone_ids = (
        boundaries[id_col].astype(str).tolist()
        if id_col != "index"
        else [str(i) for i in range(n_zones)]
    )
    zone_names = (
        boundaries[name_col].astype(str).tolist()
        if name_col
        else [None] * n_zones
    )

    # Zonal stats: mean baseline concentration per zone
    logger.info("Computing zonal stats for baseline concentration")
    conc_gdf = compute_zonal_stats(
        concentration_raster_path, boundaries, stats=["mean"]
    )
    c_baseline = np.array(conc_gdf["mean"].fillna(0).values, dtype=np.float64)

    # Zonal stats: control concentration
    if control_raster_path:
        logger.info("Computing zonal stats for control concentration")
        ctrl_gdf = compute_zonal_stats(
            control_raster_path, boundaries, stats=["mean"]
        )
        c_control = np.array(ctrl_gdf["mean"].fillna(0).values, dtype=np.float64)
    elif control_value is not None:
        c_control = np.full(n_zones, control_value, dtype=np.float64)
    else:
        # Default: no change scenario (control = baseline)
        c_control = c_baseline.copy()

    # Zonal stats: total population per zone
    logger.info("Computing zonal stats for population")
    pop_gdf = compute_zonal_stats(
        population_raster_path, boundaries, stats=["sum"]
    )
    population = np.array(pop_gdf["sum"].fillna(0).values, dtype=np.float64)

    # Extract GeoJSON geometries for frontend rendering
    geometries = []
    for _, row in boundaries.iterrows():
        geom = row.geometry
        if geom is not None:
            geometries.append(geom.__geo_interface__)
        else:
            geometries.append(None)

    return {
        "zone_ids": zone_ids,
        "zone_names": zone_names,
        "c_baseline": c_baseline,
        "c_control": c_control,
        "population": population,
        "geometries": geometries,
    }
