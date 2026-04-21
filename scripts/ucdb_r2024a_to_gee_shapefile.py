"""Build a slim GHS-UCDB R2024A shapefile ready for upload as a GEE asset.

Reads the GENERAL_CHARACTERISTICS layer of the R2024A GeoPackage (11,422 urban
centre polygons in Mollweide), keeps only the fields we need for the PM2.5
pipeline (ID, main name, country code), reprojects to WGS84, and writes a
zipped shapefile under data/processed/ucdb/ ready to drag-and-drop into the
GEE asset upload UI.

Notes:
- R2024A column names ship with a leading UTF-8 BOM in the GPKG we downloaded
  from JRC (e.g. '\\ufeffID_UC_G0'). We strip BOMs on load.
- Shapefile DBF field names are capped at 10 chars; we use short names so
  nothing gets silently truncated.
- Mollweide (ESRI:54009) -> WGS84 (EPSG:4326) reprojection is lossless for
  feature identity; GEE expects WGS84 for FeatureCollection assets.

Run:
    python scripts/ucdb_r2024a_to_gee_shapefile.py
"""
from __future__ import annotations

import shutil
import zipfile
from pathlib import Path

import geopandas as gpd

REPO_ROOT = Path(__file__).resolve().parent.parent
GPKG = REPO_ROOT / "data" / "raw" / "ucdb" / "GHS_UCDB_GLOBE_R2024A.gpkg"
OUT_DIR = REPO_ROOT / "data" / "processed" / "ucdb"
SHP_STEM = "GHS_UCDB_R2024A_slim"
LAYER = "GHS_UCDB_THEME_GENERAL_CHARACTERISTICS_GLOBE_R2024A"

# Map raw column name (post-BOM-strip) -> short output name (<=10 chars for DBF).
COLUMN_MAP = {
    "ID_UC_G0":        "ID_UC_G0",   # unique urban centre ID
    "GC_UCN_MAI_2025": "UC_NM_MN",   # main city name (matches classic UCDB convention)
    "GC_CNT_GAD_2025": "CTR_GADM",   # GADM country name
    "GC_CNT_UNN_2025": "CTR_UN",     # UN country code
}


def main() -> None:
    if not GPKG.exists():
        raise SystemExit(f"missing input GPKG: {GPKG}")

    print(f"reading {LAYER} from {GPKG.name} ...")
    gdf = gpd.read_file(GPKG, layer=LAYER)
    print(f"  loaded {len(gdf):,} features in CRS {gdf.crs}")

    # JRC's GPKG export tags every string field — both column names AND cell
    # values — with a leading UTF-8 BOM (\ufeff). Strip both so downstream
    # tools can write the data to any encoding.
    gdf.columns = [c.lstrip("\ufeff") for c in gdf.columns]
    for col in gdf.select_dtypes(include="object").columns:
        if col == "geometry":
            continue
        gdf[col] = gdf[col].astype("string").str.lstrip("\ufeff")

    missing = [c for c in COLUMN_MAP if c not in gdf.columns]
    if missing:
        raise SystemExit(
            f"expected columns not present after BOM strip: {missing}\n"
            f"  available: {sorted(gdf.columns)}"
        )

    # Keep only the columns we need plus geometry, rename to short DBF-safe forms.
    slim = gdf[[*COLUMN_MAP.keys(), "geometry"]].rename(columns=COLUMN_MAP)

    # Reproject from Mollweide to WGS84 for GEE ingestion.
    print("  reprojecting ESRI:54009 -> EPSG:4326 ...")
    slim = slim.to_crs(epsg=4326)

    # Write shapefile (DBF + SHP + SHX + PRJ + CPG) into a scratch folder, then zip.
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    scratch = OUT_DIR / SHP_STEM
    if scratch.exists():
        shutil.rmtree(scratch)
    scratch.mkdir()
    shp_path = scratch / f"{SHP_STEM}.shp"
    print(f"  writing shapefile {shp_path} ...")
    slim.to_file(shp_path, driver="ESRI Shapefile")

    zip_path = OUT_DIR / f"{SHP_STEM}.zip"
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for sibling in scratch.iterdir():
            zf.write(sibling, arcname=sibling.name)

    total_bytes = zip_path.stat().st_size
    print(f"done: {zip_path}  ({total_bytes / 1e6:.1f} MB)")
    print()
    print("upload to GEE at:")
    print("  https://code.earthengine.google.com/tasks  ->  NEW -> Shape files")
    print("  Source files: pick the 5 files under")
    print(f"    {scratch}")
    print("  or upload the .zip directly.")
    print("  Suggested Asset ID: projects/hia-tool/assets/GHS_SMOD_R2024A")


if __name__ == "__main__":
    main()
