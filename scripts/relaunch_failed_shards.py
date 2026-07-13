#!/usr/bin/env python3
"""Relaunch the GEE export shards that are STILL missing after gee_reexport_missing.py.

A Drive audit (2026-06-26) of pm25_gadm_adm2_*.csv found 747/760 (year, batch)
pairs complete and 13 still incomplete — all because individual re-export
sub-shards (the _s0.._s4 files from the 5-way split) never landed:

    batch 040  s2/s3  in all 8 years (chronic: 100 features/task still times out)
    batch 061  s2     in 2018, 2021
    batch 067  s0     in 2016, 2019, 2020

That's 18 missing 100-feature shards. This script re-tiles EACH missing shard
into two 50-feature sub-shards named ``_<batch>_s<k>a`` / ``_<batch>_s<k>b``.

Why new names instead of relaunching the whole batch:
  * The good shards (e.g. 040 s0/s1/s4) already exist in Drive. Relaunching the
    whole batch would create duplicate copies of them (GEE never overwrites — it
    appends a new file), and concatenation would then double-count those
    features. Targeting only the missing shard avoids that entirely.
  * ``_s<k>a/_b`` names don't exist yet, so there is no collision and no overlap.
  * 50 features/task is half the 100 that just failed — more headroom against the
    admin-2 geometry timeouts that caused these failures.

Downstream ingestion globs pm25_gadm_adm2_*.csv, so the extra suffix is
harmless; s<k>a + s<k>b together reproduce the missing 100-feature shard exactly.

SAFE BY DEFAULT: prints the plan and launches NOTHING unless you pass --launch.

Usage
-----
::

    # Dry run (default) — show the 36 tasks that would launch
    python scripts/relaunch_failed_shards.py

    # Actually start them
    python scripts/relaunch_failed_shards.py --launch

    # Watch progress
    python scripts/gee_export_pm25.py --boundary gadm_adm2 --status

NOTE: the hia-tool project's EE compute quota was in restricted (noncommercial)
mode as of 2026-06-26; if tasks queue but never run, that quota is why.
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
SHARD_SIZE = 100          # features per _s<k> shard (batch_size / split-of-5)
SUBSUB_SIZE = 50          # features per re-tiled sub-shard
TILE_SCALE = 16           # EE's hard max for reduceRegions

# (year, batch, shard_index) tuples that are still missing — from the
# 2026-06-26 Drive audit. shard_index k means original file _s<k> never landed.
MISSING_SHARDS: list[tuple[int, int, int]] = [
    # batch 040 — chronic, fails across all years
    (2015, 40, 2), (2015, 40, 3),
    (2016, 40, 3),
    (2017, 40, 3),
    (2018, 40, 3),
    (2019, 40, 2), (2019, 40, 3),
    (2020, 40, 2), (2020, 40, 3),
    (2021, 40, 2), (2021, 40, 3),
    (2022, 40, 2), (2022, 40, 3),
    # batch 061 — single shard, two years
    (2018, 61, 2), (2021, 61, 2),
    # batch 067 — single shard, three years
    (2016, 67, 0), (2019, 67, 0), (2020, 67, 0),
]


def plan() -> list[dict]:
    """Expand each missing 100-feature shard into two 50-feature sub-shards."""
    tasks = []
    for year, batch, k in MISSING_SHARDS:
        shard_offset = batch * BATCH_SIZE + k * SHARD_SIZE
        for part, letter in enumerate("ab"):
            off = shard_offset + part * SUBSUB_SIZE
            tasks.append({
                "year": year,
                "batch": batch,
                "offset": off,
                "count": SUBSUB_SIZE,
                "suffix": f"_{batch:03d}_s{k}{letter}",
                "name": f"pm25_{BOUNDARY}_{year}_{batch:03d}_s{k}{letter}",
            })
    return tasks


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__.split("\n\n", 1)[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--launch", action="store_true",
                   help="Actually start the tasks (default is a dry run)")
    p.add_argument("--ee-project", default="hia-tool",
                   help="Earth Engine cloud project for ee.Initialize() (default: hia-tool)")
    args = p.parse_args(argv)

    tasks = plan()
    print(f"{len(MISSING_SHARDS)} missing 100-feature shards "
          f"-> {len(tasks)} x 50-feature tasks [tileScale={TILE_SCALE}]\n")

    if not args.launch:
        for t in tasks:
            print(f"  would launch {t['name']:<40} "
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
            tile_scale=TILE_SCALE,
        )
        launched += 1
        print(f"  queued {task.config['description']:<40} id={task.id}")
    print(f"\nlaunched {launched} tasks. watch with: "
          "python scripts/gee_export_pm25.py --boundary gadm_adm2 --status")
    return 0


if __name__ == "__main__":
    sys.exit(main())
