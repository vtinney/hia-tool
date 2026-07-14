#!/usr/bin/env python3
"""Reprocess the batch-040 s3 feature range that runs GEE out of memory.

The 2026-07-13 v2 relaunch (relaunch_missing_shards_v2.py) beat the *timeout*
on batch 040 with a 1 km simplify + 25-feature tiles, but the tile covering
GADM features 20350..20374 then failed with **out of memory** in every year.

Root cause (probed on the asset): that range holds three enormous Canadian
Arctic (Nunavut) admin-2 units —

    CAN.8.1_1  Baffin     808,252 vertices
    CAN.8.3_1  Kitikmeot  246,862 vertices
    CAN.8.2_1  Keewatin   107,098 vertices

Even simplified to 1 km, Baffin's coastline keeps enough vertices that
reduceRegions over its multi-million-km2 extent exhausts the per-operation
memory ceiling (tileScale is already at EE's max of 16).

Fix: reprocess ONLY features 20350..20374, one tiny tile at a time, with an
aggressive 15 km simplify. These are near-empty Arctic districts (Baffin's
population ~13k, almost all in Iqaluit); a 15 km boundary tolerance is far
below any effect on their population-weighted PM2.5, but it collapses Baffin
from ~800k vertices to a few thousand, which fits in memory.

Collision safety
----------------
1. First cancels the still-active OOM tiles that cover 20350..20374
   (``_040_s3_x2`` for 2018-2022; the 2015-2017 ``_040_s3_x0`` tiles already
   FAILED, so they produce no output). This guarantees the range is not also
   written under an ``_x`` name -> no double counting.
2. Writes new ``_040_s3_z<tile>`` names (no existing file uses ``_z``).

SAFE BY DEFAULT: prints the plan and cancels/launches NOTHING unless --launch.

Usage
-----
::

    python scripts/relaunch_arctic_oom.py            # dry run
    python scripts/relaunch_arctic_oom.py --launch   # cancel OOM tiles + launch
    python scripts/gee_export_pm25.py --boundary gadm_adm2 --status   # watch
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
OOM_START = 20350        # first feature of the out-of-memory range
OOM_COUNT = 25           # 20350..20374
TILE_SIZE = 5            # features per task (Baffin ends up near-alone)
SIMPLIFY_ERROR = 15000.0  # metres — aggressive; Arctic districts, ~zero pop
TILE_SCALE = 16
YEARS = list(range(2015, 2023))

# The active OOM tiles to cancel first, keyed by the x-tile that covers
# 20350..20374 in each year (b-half-only years used x0, full years used x2).
OOM_XTILE_SUFFIX = {
    2015: "_040_s3_x0", 2016: "_040_s3_x0", 2017: "_040_s3_x0",
    2018: "_040_s3_x2", 2019: "_040_s3_x2", 2020: "_040_s3_x2",
    2021: "_040_s3_x2", 2022: "_040_s3_x2",
}


def plan() -> list[dict]:
    tasks = []
    for year in YEARS:
        tile = 0
        n = 0
        while n < OOM_COUNT:
            off = OOM_START + n
            take = min(TILE_SIZE, OOM_COUNT - n)
            tasks.append({
                "year": year, "offset": off, "count": take,
                "suffix": f"_040_s3_z{tile}",
                "name": f"pm25_{BOUNDARY}_{year}_040_s3_z{tile}",
            })
            tile += 1
            n += take
    return tasks


# Per-feature fallback for the one 5-feature z-tile (20355..20359) that still
# OOMs, because it mixes three enormous Nunavut units with two small populated
# ones. Each feature runs alone, with a simplify tolerance matched to its size:
# the small districts keep 1 km (heavy simplify would distort them), the Arctic
# giants get 40 km (immaterial to their ~zero-pop PM2.5, cuts the vertex load).
PER_FEATURE = [
    # (offset, name,             simplify_m)
    (20355, "Hants",            1000.0),
    (20356, "Baffin",           40000.0),
    (20357, "Keewatin",         40000.0),
    (20358, "Kitikmeot",        40000.0),
    (20359, "Greater Sudbury",  1000.0),
]


def plan_per_feature() -> list[dict]:
    tasks = []
    for year in YEARS:
        for i, (off, label, simp) in enumerate(PER_FEATURE):
            tasks.append({
                "year": year, "offset": off, "count": 1, "simplify": simp,
                "label": label,
                "suffix": f"_040_s3_w{i}",
                "name": f"pm25_{BOUNDARY}_{year}_040_s3_w{i}",
            })
    return tasks


def cancel_oom_tiles() -> int:
    """Cancel any active _x tile that covers 20350..20374 to avoid double count."""
    targets = {f"pm25_{BOUNDARY}_{y}{sfx}" for y, sfx in OOM_XTILE_SUFFIX.items()}
    cancelled = 0
    for t in ee.batch.Task.list():
        desc = t.config.get("description", "")
        state = t.status().get("state", "")
        if desc in targets and state in {"READY", "RUNNING"}:
            t.cancel()
            cancelled += 1
            print(f"  cancelled {desc} ({state})")
    if not cancelled:
        print("  no active OOM _x tiles to cancel (already failed/terminal)")
    return cancelled


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__.split("\n\n", 1)[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--launch", action="store_true",
                   help="Cancel OOM tiles and start the reprocess (default: dry run)")
    p.add_argument("--per-feature", action="store_true",
                   help="Fallback: reprocess the 20355..20359 tile one feature at "
                        "a time with per-feature simplify (populated districts 1 km, "
                        "Arctic giants 40 km). Use after the 5-feature z-tile OOMs.")
    p.add_argument("--ee-project", default="hia-tool")
    args = p.parse_args(argv)

    per_feature = args.per_feature
    tasks = plan_per_feature() if per_feature else plan()
    if per_feature:
        print(f"Arctic OOM per-feature reprocess: features "
              f"{PER_FEATURE[0][0]}..{PER_FEATURE[-1][0]} x {len(YEARS)} years "
              f"-> {len(tasks)} x 1-feature tasks [tileScale={TILE_SCALE}]\n")
    else:
        print(f"Arctic OOM reprocess: features {OOM_START}..{OOM_START + OOM_COUNT - 1} "
              f"x {len(YEARS)} years -> {len(tasks)} x {TILE_SIZE}-feature tasks "
              f"[tileScale={TILE_SCALE}, simplify={SIMPLIFY_ERROR:.0f} m]\n")

    if not args.launch:
        if not per_feature:
            print("Would cancel active OOM _x tiles:")
            for y, sfx in OOM_XTILE_SUFFIX.items():
                print(f"  pm25_{BOUNDARY}_{y}{sfx}")
            print()
        for t in tasks:
            extra = (f"  {t['label']} simplify={t['simplify']:.0f}m"
                     if per_feature else "")
            print(f"  would launch {t['name']:<36} "
                  f"(feature {t['offset']}){extra}")
        print(f"\nDRY RUN — nothing cancelled or launched. Re-run with --launch.")
        return 0

    if ee is None:
        sys.stderr.write("earthengine-api not installed.\n")
        return 2
    ee.Initialize(project=args.ee_project)

    cfg = BOUNDARIES[BOUNDARY]
    boundaries = ee.FeatureCollection(cfg["asset_id"])

    if not per_feature:
        print("Cancelling active OOM _x tiles ...")
        cancel_oom_tiles()
        print()

    launched = 0
    for t in tasks:
        sub_fc = ee.FeatureCollection(boundaries.toList(t["count"], t["offset"]))
        task = _launch_one(
            sub_fc, BOUNDARY, t["year"], t["suffix"],
            cfg["id_field"], cfg["name_field"], cfg.get("country_field"),
            tile_scale=TILE_SCALE,
            simplify_error=t.get("simplify", SIMPLIFY_ERROR),
        )
        launched += 1
        print(f"  queued {task.config['description']:<36} id={task.id}")
    print(f"\nlaunched {launched} tasks. watch with: "
          "python scripts/gee_export_pm25.py --boundary gadm_adm2 --status")
    return 0


if __name__ == "__main__":
    sys.exit(main())
