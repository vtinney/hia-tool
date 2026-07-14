"""Build a slim GADM v4.1 admin-2 GeoPackage for the HIA resolver.

Reads the worldwide GADM v4.1 admin-2 layer, keeps only the fields the
resolver needs (feature_id, name, country_iso3), simplifies geometry, and
writes a single GeoPackage at data/processed/boundaries/gadm_adm2.gpkg.

The output is what backend/services/resolver.py loads when an analysis runs
at admin-2 grain — the resolver joins it to data/processed/who_aap/
gadm_adm2/{year}.parquet on feature_id (= GID_2).

Source
------
GADM v4.1 worldwide, from gadm.org:
    https://geodata.ucdavis.edu/gadm/gadm4.1/gadm_410-gpkg.zip
Unzip into data/raw/boundaries/ so you have data/raw/boundaries/gadm_410.gpkg.

IMPORTANT: gadm_410.gpkg is a SINGLE FLAT LAYER holding the deepest available
admin level per country (admin-2 in the US, admin-4 in Indonesia, etc.), NOT a
multi-layer file with a named ADM_2 layer. So admin-2 is extracted the same way
scripts/build_gadm_adm2_for_gee.py built the GEE asset: keep rows where GID_2 is
populated, then dissolve by GID_2 to merge admin-3/4/5 leaves into one polygon
per admin-2. Using the identical extraction guarantees the gpkg's GID_2 set
matches the asset's, so the resolver's inner join on feature_id is clean.

The version of the source MUST match the GEE asset used to produce the per-year
stats parquets (both are gadm_410 here), otherwise GID_2 strings won't join.

Run
---
    python scripts/gadm_adm2_to_geopackage.py \
        --input data/raw/boundaries/gadm_410.gpkg \
        --simplify-tolerance 0.005

Output
------
    data/processed/boundaries/gadm_adm2.gpkg
        feature_id    str    e.g. 'USA.5.13_1'  (= GID_2)
        name          str    e.g. 'Alameda'      (= NAME_2)
        country_iso3  str    e.g. 'USA'          (= GID_0)
        geometry      Polygon / MultiPolygon, WGS84

Expected size: 30-60 MB at tolerance 0.005°. Tolerance 0.01° drops to ~15-30 MB
at the cost of jagged edges visible at country zoom levels.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import geopandas as gpd
import pyogrio

logger = logging.getLogger("gadm_adm2_to_geopackage")

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = REPO_ROOT / "data" / "processed" / "boundaries" / "gadm_adm2.gpkg"

# GADM v4.1 admin-2 columns read from the flat source. Keys are source columns;
# values are what the resolver expects. Edit if a source uses a truncated schema.
COLUMN_MAP = {
    "GID_2":  "feature_id",
    "NAME_2": "name",
    "GID_0":  "country_iso3",
}


def open_source(path: Path, layer: str | None) -> gpd.GeoDataFrame:
    """Read admin-2 rows from the flat gadm_410 source and dissolve by GID_2.

    Mirrors scripts/build_gadm_adm2_for_gee.py exactly (same rows, same
    dissolve) so the resulting GID_2 set matches the GEE asset the stats
    parquets were computed against. ``layer`` is accepted for API compatibility
    but ignored — gadm_410.gpkg is single-layer.
    """
    if not path.exists():
        raise FileNotFoundError(f"GADM source not found: {path}")
    read_path = f"zip://{path}" if path.suffix == ".zip" else str(path)
    logger.info("reading admin-2 rows (GID_2 populated) from %s ...", path.name)
    gdf = pyogrio.read_dataframe(
        read_path,
        columns=[*COLUMN_MAP.keys()],
        where="GID_2 IS NOT NULL AND GID_2 != ''",
    )
    logger.info("  loaded %d admin-2+ rows; dissolving by GID_2 ...", len(gdf))
    # Merge admin-3/4/5 leaves into one polygon per admin-2. GADM children tile
    # their parent exactly, so the dissolve is geometrically clean.
    adm2 = gdf.dissolve(by="GID_2", aggfunc="first", as_index=False)
    logger.info("  produced %d admin-2 polygons", len(adm2))
    return adm2


def slim_and_simplify(
    gdf: gpd.GeoDataFrame,
    tolerance: float,
) -> gpd.GeoDataFrame:
    """Rename to resolver columns, simplify geometry, ensure WGS84."""
    missing = [c for c in COLUMN_MAP if c not in gdf.columns]
    if missing:
        raise SystemExit(
            f"Source is missing expected columns {missing}. "
            f"Available: {sorted(c for c in gdf.columns if c != 'geometry')}\n"
            f"If your source uses a non-standard schema, edit COLUMN_MAP in this script."
        )

    slim = gdf[[*COLUMN_MAP.keys(), "geometry"]].rename(columns=COLUMN_MAP)

    if slim.crs and not slim.crs.equals("EPSG:4326"):
        logger.info("reprojecting %s -> EPSG:4326", slim.crs)
        slim = slim.to_crs(epsg=4326)

    if tolerance > 0:
        logger.info("simplifying geometry at tolerance %.4f° ...", tolerance)
        t0 = time.perf_counter()
        # preserve_topology=True keeps polygon ring closure and avoids
        # producing crossed/self-intersecting rings on tight features.
        slim["geometry"] = slim.geometry.simplify(tolerance, preserve_topology=True)
        logger.info("  done in %.1fs", time.perf_counter() - t0)

    # Drop any rows where simplification collapsed geometry to None/empty —
    # extremely small features near the simplification tolerance can vanish.
    before = len(slim)
    slim = slim[slim.geometry.notna() & ~slim.geometry.is_empty].reset_index(drop=True)
    if len(slim) < before:
        logger.warning(
            "dropped %d features whose geometry collapsed under simplification",
            before - len(slim),
        )

    return slim


def write_gpkg(slim: gpd.GeoDataFrame, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        output.unlink()  # GeoPackage append vs overwrite is fiddly; just rewrite
    slim.to_file(output, driver="GPKG", layer="gadm_adm2")
    size_mb = output.stat().st_size / (1024 * 1024)
    logger.info("wrote %s (%.1f MB, %d features)", output, size_mb, len(slim))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    p.add_argument(
        "--input", type=Path, required=True,
        help="GADM v4.1 source (gpkg, .gpkg.zip, shp, or shp.zip)",
    )
    p.add_argument(
        "--layer", default="ADM_2",
        help="Layer name inside multi-layer GPKGs (default: ADM_2)",
    )
    p.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT,
        help=f"Output GeoPackage path (default: {DEFAULT_OUTPUT})",
    )
    p.add_argument(
        "--simplify-tolerance", type=float, default=0.005,
        help=(
            "Douglas-Peucker tolerance in degrees. Lower = more faithful but "
            "larger file. 0.005 ≈ 500m, 0.01 ≈ 1km. Pass 0 to skip simplification."
        ),
    )
    p.add_argument("--verbose", "-v", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    layer = args.layer if args.layer.lower() != "none" else None
    src = open_source(args.input, layer)
    slim = slim_and_simplify(src, args.simplify_tolerance)
    write_gpkg(slim, args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
