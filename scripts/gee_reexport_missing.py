#!/usr/bin/env python3
"""Re-launch GEE export tasks for exactly the (year, batch) pairs that are missing.

Companion to gee_export_pm25.py. That script's --batch flag launches the full
years x batches cross-product, which would duplicate shards that already
exported fine (e.g. batch 025 is only missing for 2015 — relaunching it for
all 8 years would create 7 same-named duplicate CSVs in Drive). This script
reads a list of missing output filenames and launches one task per missing
file, nothing more.

The missing list comes from a Drive audit (2026-06-11): of the 760 expected
pm25_gadm_adm2_<year>_<batch>.csv files, 86 never exported. The same batch
indices failed across years (008, 040, 059, 075 in all 8 years; 067 in 7),
which is the signature of GEE compute timeouts on geometry-heavy admin-2
batches.

EE caps reduceRegions tileScale at 16 — which the original run already used
(a first retry attempt at tileScale=32 failed instantly with "Valid
tileScales are 1 to 16"). Since tileScale has no headroom, this script
instead shrinks the per-task workload: each missing batch of 500 features is
split into ``--split`` sub-batches (default 5 → 100 features/task), exported
as pm25_<boundary>_<year>_<batch>_s<k>.csv. Downstream ingestion should glob
pm25_gadm_adm2_*.csv, so the extra suffix is harmless; concatenating the
sub-shards reproduces the missing shard exactly.

Usage
-----
::

    # Launch all missing shards, split 5 ways (86 files -> 430 tasks)
    python scripts/gee_reexport_missing.py

    # Dry run: print what would launch without starting tasks
    python scripts/gee_reexport_missing.py --dry-run

    # No splitting (plain retry of the original 500-feature batches)
    python scripts/gee_reexport_missing.py --split 1

    # Watch progress (reuses the original script's poller)
    python scripts/gee_export_pm25.py --boundary gadm_adm2 --status

Note on Drive folders: GEE racing tasks are what created the ~1000 duplicate
hia_tool_pm25 folders in the first place. With 86 concurrent tasks a few new
duplicate folders may appear again — merge them into the surviving folder
when the run finishes.
"""
from __future__ import annotations

import argparse
import math
import re
import sys
from collections import defaultdict
from pathlib import Path

try:
    import ee
except ImportError:
    ee = None

# Reuse the canonical config and EE logic — this script only changes WHICH
# tasks get launched, never how the stats are computed.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from gee_export_pm25 import BOUNDARIES, _launch_one  # noqa: E402

DEFAULT_MISSING_FILE = Path(__file__).resolve().parent / "missing_gadm_adm2_exports.txt"
DEFAULT_TILE_SCALE = 16  # EE's hard maximum for reduceRegions
DEFAULT_SPLIT = 5

FILENAME_RE = re.compile(r"^pm25_(?P<boundary>[a-z0-9_]+)_(?P<year>\d{4})_(?P<batch>\d{3})\.csv$")


def parse_missing(path: Path) -> dict[str, dict[int, list[int]]]:
    """Parse missing filenames into {boundary: {batch: [years...]}}."""
    plan: dict[str, dict[int, list[int]]] = defaultdict(lambda: defaultdict(list))
    bad: list[str] = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        name = line.strip()
        if not name:
            continue
        m = FILENAME_RE.match(name)
        if not m:
            bad.append(name)
            continue
        plan[m["boundary"]][int(m["batch"])].append(int(m["year"]))
    if bad:
        sys.stderr.write(f"WARN: {len(bad)} unparseable lines skipped: {bad[:3]}...\n")
    return plan


def _sub_chunks(batch_size: int, split: int) -> list[tuple[int, int]]:
    """(offset-within-batch, count) pairs that exactly tile one batch."""
    sub = math.ceil(batch_size / split)
    return [(k * sub, min(sub, batch_size - k * sub)) for k in range(split) if k * sub < batch_size]


def queued_descriptions() -> set[str]:
    """Descriptions of EE tasks already alive — lets relaunches skip them."""
    alive = {"READY", "RUNNING", "COMPLETED"}
    out: set[str] = set()
    for t in ee.batch.Task.list():
        cfg = getattr(t, "config", None) or {}
        desc = cfg.get("description", "")
        if desc.startswith("pm25_") and t.state in alive:
            out.add(desc)
    return out


def launch_missing(
    plan: dict[str, dict[int, list[int]]],
    tile_scale: int,
    split: int,
    dry_run: bool,
    skip: set[str] | None = None,
) -> int:
    launched = 0
    for boundary_name, batches in sorted(plan.items()):
        cfg = BOUNDARIES.get(boundary_name)
        if cfg is None:
            sys.stderr.write(f"ERROR: unknown boundary '{boundary_name}' — skipping\n")
            continue
        batch_size = cfg["batch_size"]
        chunks = _sub_chunks(batch_size, split)
        n_files = sum(len(v) for v in batches.values())
        print(
            f"{boundary_name}: {len(batches)} batch(es), {n_files} missing files "
            f"x {len(chunks)} sub-batch(es) = {n_files * len(chunks)} tasks "
            f"[tileScale={tile_scale}, {batch_size // len(chunks)} features/task]"
        )

        if not dry_run:
            boundaries = ee.FeatureCollection(cfg["asset_id"])
            total = boundaries.size().getInfo()
            n_batches = math.ceil(total / batch_size)
        else:
            boundaries = None
            n_batches = None

        for b in sorted(batches):
            years = sorted(batches[b])
            if dry_run:
                print(f"  would launch batch {b:03d} x {len(chunks)} sub-batches for years {years}")
                launched += len(years) * len(chunks)
                continue
            if b >= n_batches:
                sys.stderr.write(
                    f"ERROR: batch {b} out of range (asset has {n_batches} "
                    "batches) — asset changed since the original run?\n"
                )
                continue
            for k, (off, count) in enumerate(chunks):
                sub_fc = ee.FeatureCollection(
                    boundaries.toList(count, b * batch_size + off)
                )
                suffix = f"_{b:03d}" if len(chunks) == 1 else f"_{b:03d}_s{k}"
                for y in years:
                    name = f"pm25_{boundary_name}_{y}{suffix}"
                    if skip and name in skip:
                        print(f"  skip   {name:<44} (already queued)")
                        continue
                    task = _launch_one(
                        sub_fc,
                        boundary_name,
                        y,
                        suffix,
                        cfg["id_field"],
                        cfg["name_field"],
                        cfg.get("country_field"),
                        tile_scale=tile_scale,
                    )
                    launched += 1
                    print(f"  queued {task.config['description']:<44} id={task.id}")
    return launched


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__.split("\n\n", 1)[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--missing-file", type=Path, default=DEFAULT_MISSING_FILE,
        help=f"Text file of missing CSV filenames, one per line (default: {DEFAULT_MISSING_FILE.name})",
    )
    p.add_argument(
        "--tile-scale", type=int, default=DEFAULT_TILE_SCALE, dest="tile_scale",
        help=f"reduceRegions tileScale (default {DEFAULT_TILE_SCALE} — EE's hard max)",
    )
    p.add_argument(
        "--split", type=int, default=DEFAULT_SPLIT,
        help=(
            f"Sub-divide each missing batch into this many smaller export "
            f"tasks (default {DEFAULT_SPLIT}). Output files get an _s<k> "
            "suffix. Use 1 to retry whole batches unchanged."
        ),
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Print the launch plan without starting any tasks",
    )
    p.add_argument(
        "--ee-project", default=None,
        help="Optional Earth Engine cloud project for ee.Initialize()",
    )
    args = p.parse_args(argv)

    if not args.missing_file.exists():
        sys.stderr.write(f"missing-file not found: {args.missing_file}\n")
        return 2

    plan = parse_missing(args.missing_file)
    if not plan:
        print("nothing to do — missing list is empty")
        return 0

    if not args.dry_run:
        if ee is None:
            sys.stderr.write(
                "earthengine-api not installed. Run:\n"
                "    pip install earthengine-api\n"
                "    python -c 'import ee; ee.Authenticate()'\n"
            )
            return 2
        init_kwargs = {"project": args.ee_project} if args.ee_project else {}
        ee.Initialize(**init_kwargs)

    if not (1 <= args.tile_scale <= 16):
        sys.stderr.write("tile-scale must be 1-16 (EE rejects anything higher)\n")
        return 2
    if args.split < 1:
        sys.stderr.write("split must be >= 1\n")
        return 2

    skip = queued_descriptions() if not args.dry_run else None
    if skip:
        print(f"found {len(skip)} pm25_* tasks already alive on EE — will skip those")
    launched = launch_missing(plan, args.tile_scale, args.split, args.dry_run, skip=skip)
    verb = "would launch" if args.dry_run else "launched"
    print(f"\n{verb} {launched} tasks. watch with: "
          "python scripts/gee_export_pm25.py --boundary gadm_adm2 --status")
    return 0


if __name__ == "__main__":
    sys.exit(main())
