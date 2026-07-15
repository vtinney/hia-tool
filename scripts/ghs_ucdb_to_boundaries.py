#!/usr/bin/env python3
"""Build the urban-centre boundary GeoPackage for the HIA resolver.

Reads the same GHS-UCDB R2024A source the GEE asset was built from
(scripts/ucdb_r2024a_to_gee_shapefile.py), so feature_id matches the
pm25_ghs_smod_* export 1:1. Attaches country_iso3 by joining the UCDB's
GADM country name (GC_CNT_GAD_2025) to GADM v4.1's COUNTRY -> GID_0
(verified 2026-07-15: 0 unmatched, 0 null, 0 multi-country cells).

Output: data/processed/boundaries/ghs_ucdb_r2024a.gpkg with
    feature_id (str), name, country_iso3, geometry (EPSG:4326)

Run:
    python scripts/ghs_ucdb_to_boundaries.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import pyogrio

REPO_ROOT = Path(__file__).resolve().parent.parent
UCDB_GPKG = REPO_ROOT / "data" / "raw" / "ucdb" / "GHS_UCDB_GLOBE_R2024A.gpkg"
UCDB_LAYER = "GHS_UCDB_THEME_GENERAL_CHARACTERISTICS_GLOBE_R2024A"
GADM_GPKG = REPO_ROOT / "data" / "raw" / "boundaries" / "gadm_410.gpkg"
OUT = REPO_ROOT / "data" / "processed" / "boundaries" / "ghs_ucdb_r2024a.gpkg"

# ~100 m in degrees. Urban centres are 1-100 km across; this trims payload
# without visibly distorting city outlines (GADM admin-2 uses 0.005, but
# those polygons are 100x larger).
SIMPLIFY_TOLERANCE_DEG = 0.001


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    if not UCDB_GPKG.exists():
        raise SystemExit(f"missing input: {UCDB_GPKG}")
    if not GADM_GPKG.exists():
        raise SystemExit(f"missing input: {GADM_GPKG}")

    print(f"reading {UCDB_LAYER} ...")
    gdf = gpd.read_file(UCDB_GPKG, layer=UCDB_LAYER)
    # R2024A columns ship with a leading UTF-8 BOM.
    gdf.columns = [c.lstrip("﻿") for c in gdf.columns]
    print(f"  {len(gdf):,} features in {gdf.crs}")

    print("reading GADM country name -> ISO3 ...")
    gadm = pyogrio.read_dataframe(
        GADM_GPKG, sql="SELECT DISTINCT GID_0, COUNTRY FROM gadm_410",
        read_geometry=False,
    )
    name2iso = dict(zip(gadm["COUNTRY"], gadm["GID_0"]))

    iso3 = gdf["GC_CNT_GAD_2025"].str.lstrip("﻿").map(name2iso)
    unmatched = gdf.loc[iso3.isna(), "GC_CNT_GAD_2025"].str.lstrip("﻿").unique()
    if len(unmatched):
        raise SystemExit(f"unmapped UCDB country names: {sorted(unmatched)}")

    out = gpd.GeoDataFrame(
        {
            "feature_id": gdf["ID_UC_G0"].astype(int).astype(str),
            "name": gdf["GC_UCN_MAI_2025"].str.lstrip("﻿"),
            "country_iso3": iso3,
        },
        geometry=gdf.geometry,
        crs=gdf.crs,
    ).to_crs("EPSG:4326")
    out["geometry"] = out.geometry.simplify(
        SIMPLIFY_TOLERANCE_DEG, preserve_topology=True
    )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_file(OUT, driver="GPKG")
    print(f"wrote {OUT} ({len(out):,} features, "
          f"{out['country_iso3'].nunique()} countries)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
