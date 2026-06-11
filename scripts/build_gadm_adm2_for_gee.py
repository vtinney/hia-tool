"""Build a slim admin-2-only shapefile zip for upload to GEE as gadm_adm2.

The local gadm_410.gpkg from gadm.org is a single flat layer that holds only
the deepest available admin level per country (admin-2 in the US, admin-4 in
Indonesia, etc.). The PM2.5 export script (scripts/gee_export_pm25.py) needs
a single FeatureCollection of pure admin-2 polygons keyed on GID_2.

This script builds that asset by:
  1. Reading rows where GID_2 is populated (drops admin-0/1-only microstates).
  2. Dissolving by GID_2 to merge admin-3/4/5 leaves into one polygon per
     admin-2. GADM children tile their parent exactly by construction, so
     dissolution gives geometrically clean admin-2 boundaries.
  3. Slimming columns to GID_0, GID_2, NAME_2 (the only fields the export
     script reads).
  4. Writing a UTF-8 zipped shapefile, ready for the EE Code Editor's asset
     uploader.

Run from repo root using the venv that has pyogrio + geopandas:

    venv\Scripts\python scripts\build_gadm_adm2_for_gee.py   (Windows cmd)
    venv/Scripts/python scripts/build_gadm_adm2_for_gee.py   (PowerShell / bash)

Output: data/raw/boundaries/GADM/gadm_adm2_for_gee.zip

Next step (manual): in the EE Code Editor, Assets tab -> New -> Shape files,
upload the zip as projects/hia-tool/assets/gadm_adm2, wait for ingest, then
rerun python scripts/gee_export_pm25.py --boundary gadm_adm2.
"""
from __future__ import annotations

import shutil
import time
import zipfile
from pathlib import Path

import geopandas as gpd
import pyogrio
import shapely

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "data" / "raw" / "boundaries" / "GADM" / "gadm_410.gpkg"
OUT_DIR = REPO_ROOT / "data" / "raw" / "boundaries" / "GADM" / "gadm_adm2_for_gee"
OUT_SHP = OUT_DIR / "gadm_adm2.shp"
OUT_ZIP = OUT_DIR.parent / "gadm_adm2_for_gee.zip"

# GEE caps each feature's primary geometry at 1,000,000 vertices.
GEE_VERTEX_LIMIT = 1_000_000
# ~55m at the equator. Safely below PM2.5 (~1km) and population (~100m–1km)
# raster resolution, so zonal stats are unaffected.
SIMPLIFY_TOLERANCE_DEG = 0.0005


def _report_vertices(adm2: gpd.GeoDataFrame, label: str) -> None:
    counts = shapely.get_num_coordinates(adm2.geometry.values)
    over = int((counts > GEE_VERTEX_LIMIT).sum())
    idx_max = int(counts.argmax())
    print(
        f"  vertices [{label}]: max={int(counts.max()):,} "
        f"(GID_2={adm2.iloc[idx_max]['GID_2']}, "
        f"NAME_2={adm2.iloc[idx_max]['NAME_2']}), "
        f"total={int(counts.sum()):,}, over_limit={over}",
        flush=True,
    )


def main() -> int:
    if not SRC.exists():
        raise SystemExit(f"GADM source not found: {SRC}")

    t0 = time.time()
    elapsed = lambda: f"[{time.time() - t0:7.1f}s]"

    print(f"{elapsed()} reading admin-2+ rows from {SRC.name} ...", flush=True)
    gdf = pyogrio.read_dataframe(
        SRC,
        columns=["GID_0", "GID_2", "NAME_2"],
        where="GID_2 IS NOT NULL AND GID_2 != ''",
    )
    print(f"{elapsed()}   loaded {len(gdf):,} rows", flush=True)

    print(f"{elapsed()} dissolving by GID_2 ...", flush=True)
    adm2 = gdf.dissolve(by="GID_2", aggfunc="first", as_index=False)
    # Reorder columns so the shapefile schema is deterministic
    adm2 = adm2[["GID_2", "GID_0", "NAME_2", "geometry"]]
    print(f"{elapsed()}   produced {len(adm2):,} admin-2 polygons", flush=True)
    _report_vertices(adm2, "pre-simplify")

    print(
        f"{elapsed()} simplifying geometries "
        f"(tol={SIMPLIFY_TOLERANCE_DEG}°) ...",
        flush=True,
    )
    adm2["geometry"] = adm2.geometry.simplify(
        tolerance=SIMPLIFY_TOLERANCE_DEG, preserve_topology=True
    )
    _report_vertices(adm2, "post-simplify")

    counts_after = shapely.get_num_coordinates(adm2.geometry.values)
    if (counts_after > GEE_VERTEX_LIMIT).any():
        offenders = adm2.loc[counts_after > GEE_VERTEX_LIMIT, ["GID_2", "NAME_2"]]
        raise SystemExit(
            f"{elapsed()} ERROR: {len(offenders)} feature(s) still exceed "
            f"GEE's {GEE_VERTEX_LIMIT:,}-vertex limit after simplify. "
            f"Increase SIMPLIFY_TOLERANCE_DEG. Offenders:\n{offenders.to_string(index=False)}"
        )

    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(parents=True)

    print(f"{elapsed()} writing shapefile to {OUT_SHP.name} ...", flush=True)
    pyogrio.write_dataframe(
        adm2,
        OUT_SHP,
        driver="ESRI Shapefile",
        encoding="UTF-8",
    )

    if OUT_ZIP.exists():
        OUT_ZIP.unlink()

    print(f"{elapsed()} zipping to {OUT_ZIP.name} ...", flush=True)
    with zipfile.ZipFile(OUT_ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(OUT_DIR.iterdir()):
            zf.write(f, arcname=f.name)

    size_mb = OUT_ZIP.stat().st_size / 1e6
    print(
        f"{elapsed()} done. {OUT_ZIP.name} = {size_mb:.1f} MB, "
        f"{len(adm2):,} features",
        flush=True,
    )
    print()
    print("Next step: upload via EE Code Editor")
    print(f"  Assets tab -> New -> Shape files -> {OUT_ZIP}")
    print("  Asset ID:  projects/hia-tool/assets/gadm_adm2")
    print("Then: python scripts/gee_export_pm25.py --boundary gadm_adm2")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
