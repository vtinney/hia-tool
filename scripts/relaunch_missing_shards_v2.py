#!/usr/bin/env python3
"""Relaunch the GADM admin-2 export shards STILL missing after the two prior
re-export passes (gee_reexport_missing.py, then relaunch_failed_shards.py).

Current-state audit (2026-07-13, rclone listing of Drive folder hia_tool_pm25/,
1,097 CSVs): 747 / 760 (year, batch) pairs complete, 13 still incomplete —
14 individual 100-feature shards never landed. Audit method is reproducible:

    rclone lsf "gdrive:" --include "pm25_gadm_adm2_*.csv" -R --files-only
    python scripts/audit... (a (year,batch) is complete if the whole-batch CSV
    OR all 5 _s0.._s4 shards exist, where a shard _sK counts as present if the
    plain _sK file exists OR both re-tiled halves _sKa and _sKb exist).

The residual failures are dominated by ONE pathological batch:

    batch 040  shard s3  -> fails in every year; even the 50-feature _s3b half
                           times out (a single Arctic/large admin-2 polygon in
                           features 20350..20399 with a huge vertex count).
    batch 040  shard s2  -> 2022 only.
    batch 061  shard s2  -> 2018, 2021.
    batch 067  shard s0  -> 2016, 2019, 2020.

Why this pass is different from the last one
--------------------------------------------
1. **Geometry simplification.** The root cause of the chronic batch-040 timeout
   is vertex count, not feature count. This pass simplifies each polygon to a
   1 km tolerance before reduceRegions (via gee_export_pm25's new
   ``simplify_error`` arg). 1 km is below the 1113 m PM2.5 grid cell, so the
   population-weighted zonal sums are unchanged for these large, sparsely
   populated units — but the vertex count of the offending polygon drops from
   millions to thousands, which is what actually beats the per-operation
   compute ceiling.
2. **Smaller tiles.** Each missing 100-feature shard is re-tiled into
   25-feature tasks (4x headroom vs the 100 that timed out).
3. **Only the truly-missing feature ranges.** Where the ``_sKa`` half already
   landed (batch 040 s3 in 2015/2016/2017), only the missing ``b`` half is
   relaunched — no duplicate features, no double counting on concat.

Collision safety
----------------
New task/file names use the suffix ``_s{k}_x{tile}`` (e.g.
``pm25_gadm_adm2_2018_040_s3_x0``). No existing Drive file uses ``_x``, so
there is zero name collision and the extra suffix is harmless to the
``pm25_gadm_adm2_*.csv`` glob in pm25_csv_to_parquet.py. Every feature is
exported exactly once across all landed files, so a plain concat is correct.

SAFE BY DEFAULT: prints the plan and launches NOTHING unless you pass --launch.

Usage
-----
::

    # Dry run (default) — show the tasks that would launch
    python scripts/relaunch_missing_shards_v2.py

    # Actually start them
    python scripts/relaunch_missing_shards_v2.py --launch

    # Watch progress (any shell, any time)
    python scripts/gee_export_pm25.py --boundary gadm_adm2 --status

    # Re-audit Drive afterwards to confirm 760/760
    rclone lsf "gdrive:" --include "pm25_gadm_adm2_*.csv" -R --files-only

NOTE: if the hia-tool EE project is in restricted (noncommercial) compute
quota, tasks may queue but not run. Check the EE Tasks panel / --status.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    import ee
except ImportError:
    ee = None

sys.path.insert(0, str(Path(__file__).resolve().parent))
from gee_export_pm25 import BOUNDARIES, _launch_one  # noqa: E402

BOUNDARY = "gadm_adm2"
BATCH_SIZE = 500          # features per original batch
SHARD_SIZE = 100          # features per _sK shard (batch_size / 5-way split)
TILE_SIZE = 25            # features per re-tiled task this pass
TILE_SCALE = 16           # EE hard max for reduceRegions
SIMPLIFY_ERROR = 1000.0   # metres — sub-grid; kills pathological vertex counts

# (year, batch, shard_k, present_halves) — from the 2026-07-13 Drive audit.
# present_halves lists the 50-feature halves that ALREADY landed for this
# (year, shard) as _sK a/b files; those feature ranges are skipped.
#   "" -> whole 100-feature shard missing
#   "a" -> first 50 (features offset..offset+49) already present, relaunch b
MISSING = [
    # batch 040 s3 — a-half already landed in 2015-2017, only b-half missing
    (2015, 40, 3, "a"),
    (2016, 40, 3, "a"),
    (2017, 40, 3, "a"),
    # batch 040 s3 — whole shard missing 2018-2022
    (2018, 40, 3, ""),
    (2019, 40, 3, ""),
    (2020, 40, 3, ""),
    (2021, 40, 3, ""),
    (2022, 40, 3, ""),
    # batch 040 s2 — 2022 only
    (2022, 40, 2, ""),
    # batch 061 s2 — whole shard missing
    (2018, 61, 2, ""),
    (2021, 61, 2, ""),
    # batch 067 s0 — whole shard missing
    (2016, 67, 0, ""),
    (2019, 67, 0, ""),
    (2020, 67, 0, ""),
]


def missing_ranges(k_offset: int, present: str) -> list[tuple[int, int]]:
    """Feature (offset, count) sub-ranges of a 100-feat shard still to export."""
    a_start, b_start = k_offset, k_offset + SHARD_SIZE // 2
    ranges = []
    if "a" not in present:
        ranges.append((a_start, SHARD_SIZE // 2))
    if "b" not in present:
        ranges.append((b_start, SHARD_SIZE // 2))
    return ranges


def plan() -> list[dict]:
    """Expand each missing shard into 25-feature tasks over its absent ranges."""
    tasks = []
    for year, batch, k, present in MISSING:
        k_offset = batch * BATCH_SIZE + k * SHARD_SIZE
        tile = 0
        for start, count in missing_ranges(k_offset, present):
            n = 0
            while n < count:
                off = start + n
                take = min(TILE_SIZE, count - n)
                tasks.append({
                    "year": year,
                    "batch": batch,
                    "offset": off,
                    "count": take,
                    "suffix": f"_{batch:03d}_s{k}_x{tile}",
                    "name": f"pm25_{BOUNDARY}_{year}_{batch:03d}_s{k}_x{tile}",
                })
                tile += 1
                n += take
    return tasks


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__.split("\n\n", 1)[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--launch", action="store_true",
                   help="Actually start the tasks (default is a dry run)")
    p.add_argument("--ee-project", default="hia-tool",
                   help="Earth Engine cloud project (default: hia-tool)")
    args = p.parse_args(argv)

    tasks = plan()
    print(f"{len(MISSING)} missing shards -> {len(tasks)} x {TILE_SIZE}-feature "
          f"tasks  [tileScale={TILE_SCALE}, simplify={SIMPLIFY_ERROR:.0f} m]\n")

    if not args.launch:
        for t in tasks:
            print(f"  would launch {t['name']:<38} "
                  f"(features {t['offset']}..{t['offset'] + t['count'] - 1})")
        print(f"\nDRY RUN — nothing launched. Re-run with --launch to start "
              f"{len(tasks)} tasks.")
        return 0

    if ee is None:
        sys.stderr.write("earthengine-api not installed.\n")
        return 2
    ee.Initialize(project=args.ee_project)

    cfg = BOUNDARIES[BOUNDARY]
    boundaries = ee.FeatureCollection(cfg["asset_id"])
    launched = 0
    for t in tasks:
        sub_fc = ee.FeatureCollection(boundaries.toList(t["count"], t["offset"]))
        task = _launch_one(
            sub_fc, BOUNDARY, t["year"], t["suffix"],
            cfg["id_field"], cfg["name_field"], cfg.get("country_field"),
            tile_scale=TILE_SCALE, simplify_error=SIMPLIFY_ERROR,
        )
        launched += 1
        print(f"  queued {task.config['description']:<38} id={task.id}")
    print(f"\nlaunched {launched} tasks. watch with: "
          "python scripts/gee_export_pm25.py --boundary gadm_adm2 --status")
    return 0


if __name__ == "__main__":
    sys.exit(main())
