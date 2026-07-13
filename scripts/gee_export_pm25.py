#!/usr/bin/env python3
"""Launch and monitor PM2.5 + population zonal-stats export tasks on GEE.

Python translation of the Earth Engine logic in scripts/pm25_popweighted.js,
intended for boundary sets large enough that clicking "Run" 700+ times in the
EE web Tasks panel is impractical (e.g. GADM admin-2: ~46k features × 8 years
× 92 batches ≈ 736 tasks). The web-editor JS file remains the canonical
reference and is still useful for visual verification.

What this script does
---------------------
1. Mirrors CONFIG.boundaries from pm25_popweighted.js (kept in sync manually
   for now; if drift becomes a problem we can promote it to a shared JSON).
2. Translates loadPM25 / loadWorldPop / prepAgeBands / computeStatsForYear to
   the Python ee.* surface.
3. Queues one Export.table.toDrive task per (boundary, year, batch) and calls
   .start() on each immediately. EE will run them with its own concurrency
   limit (~2-3 active per user), so wall clock for ~700 tasks is on the
   order of 1-2 days; you do NOT need to babysit.
4. Optional --status mode polls the EE task list every 60 s and prints
   per-task progress until everything reaches COMPLETED or FAILED.

One-time setup
--------------
::

    pip install earthengine-api
    python -c "import ee; ee.Authenticate()"   # opens browser, writes ~/.config/earthengine/credentials

CLI
---
::

    # Launch all years × batches for one boundary set
    python scripts/gee_export_pm25.py --boundary gadm_adm2

    # Launch a single year (useful for re-runs after a bad batch)
    python scripts/gee_export_pm25.py --boundary gadm_adm2 --years 2020

    # Watch task progress
    python scripts/gee_export_pm25.py --boundary gadm_adm2 --status

    # Retry specific failing batches. NOTE: EE caps reduceRegions tileScale
    # at 16, which is already the default here — there is no headroom to
    # "bump tileScale" for "Computation timed out" failures on
    # geometry-heavy batches (typical for GADM admin-2 — its features vary
    # far more in complexity than the GHS_SMOD urban centroids that
    # batch_size=500 was originally tuned for). For persistent timeouts,
    # use scripts/gee_reexport_missing.py, which splits each failing batch
    # into smaller per-task feature chunks instead.
    python scripts/gee_export_pm25.py --boundary gadm_adm2 \
        --years 2016 --batch 13

    # Retry several failing batches at once (comma-separated, no spaces).
    python scripts/gee_export_pm25.py --boundary gadm_adm2 --batch 13,42,87

The launch command prints a one-line summary plus task IDs; rerun with
``--status`` later from any directory to see what's still running.
"""
from __future__ import annotations

import argparse
import math
import sys
import time
from typing import Iterable

# earthengine-api is an optional dependency — import-guarded so the script
# still parses on systems where it isn't installed.
try:
    import ee
except ImportError:
    ee = None


# Mirror of CONFIG in scripts/pm25_popweighted.js. Keep in sync by hand when
# tuning either file. (Yes, it's duplication — promoting to a shared JSON
# is a follow-up if/when this drifts.)
PM25_COLLECTION = "projects/sat-io/open-datasets/GLOBAL-SATELLITE-PM25/ANNUAL"
WP_COLLECTION = "projects/sat-io/open-datasets/WORLDPOP/agesex"
DRIVE_FOLDER = "hia_tool_pm25"
TILE_SCALE = 16
AGE_BINS = [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90]

# Same VALID_BOUNDS / GRID_CRS_TRANSFORM as the JS — see those comments in
# pm25_popweighted.js for why.
VALID_BBOX = (-180, -85, 180, 85)
GRID_CRS_TRANSFORM = [0.01, 0, -180, 0, -0.01, 85]

# Boundary configs. Each entry mirrors a row of CONFIG.boundaries in the JS.
# Edit idField / nameField / countryField after the Task 0 probe in
# docs/methods/pm25_gee_runbook.md if your asset's actual field names differ.
BOUNDARIES = {
    "ghs_smod": {
        "asset_id": "projects/hia-tool/assets/GHS_SMOD",
        "id_field": "ID_UC_G0",
        "name_field": "UC_NM_MN",
        "country_field": None,
        "batch_size": 500,
    },
    "gadm_adm2": {
        "asset_id": "projects/hia-tool/assets/gadm_adm2",
        "id_field": "GID_2",
        "name_field": "NAME_2",
        "country_field": "GID_0",
        "batch_size": 500,
    },
}

DEFAULT_YEARS = list(range(2015, 2023))  # 2015..2022 inclusive — clip to PM2.5 availability


# ─────────────────────────────────────────────────────────────────────────
#  EE-side image builders (mirror the JS functions one-for-one)
# ─────────────────────────────────────────────────────────────────────────


def _valid_bounds() -> "ee.Geometry":
    return ee.Geometry.BBox(*VALID_BBOX)


def load_pm25(year: int) -> "ee.Image":
    col = ee.ImageCollection(PM25_COLLECTION).filterDate(
        f"{year}-01-01", f"{year + 1}-01-01"
    )
    img = ee.Image(col.first()).rename(["pm25"]).clip(_valid_bounds())
    return img.set("year", year)


def load_worldpop(year: int) -> "ee.Image":
    col = ee.ImageCollection(WP_COLLECTION).filterDate(
        f"{year}-01-01", f"{year + 1}-01-01"
    )
    img = (
        col.mosaic()
        .clip(_valid_bounds())
        .setDefaultProjection("EPSG:4326", None, 100)
    )
    return img.set("year", year).set("pop_source_year", year)


def prep_age_bands(wp: "ee.Image") -> "ee.Image":
    age_images = []
    for bin_ in AGE_BINS:
        pad = f"{bin_:02d}"
        m = wp.select(f"m_{pad}")
        f = wp.select(f"f_{pad}")
        age_images.append(m.add(f).rename(f"age_{bin_}"))
    age_stack = ee.Image.cat(age_images)
    pop_total = age_stack.reduce(ee.Reducer.sum()).rename("pop_total")
    return ee.Image.cat([pop_total, age_stack])


def align_pop_to_pm25_grid(pop_image: "ee.Image") -> "ee.Image":
    # Aggregate WorldPop's 100 m bands onto the PM2.5 ~1113 m grid using SUM.
    # Without this, reduceRegions at scale 1113 with a sum reducer relies on
    # the image pyramid; float-image pyramids default to mean-of-children, so
    # totals get undercounted by ~(1113/100)^2 ≈ 124x. maxPixels must exceed
    # that ratio or reduceResolution silently degrades — 256 leaves headroom.
    return (
        pop_image.reduceResolution(
            reducer=ee.Reducer.sum().unweighted(),
            bestEffort=False,
            maxPixels=256,
        )
        .reproject(crs="EPSG:4326", crsTransform=GRID_CRS_TRANSFORM)
        .clip(_valid_bounds())
    )


def compute_stats_for_year(
    boundaries: "ee.FeatureCollection",
    year: int,
    id_field: str,
    name_field: str,
    country_field: str | None,
    tile_scale: int = TILE_SCALE,
    simplify_error: float | None = None,
) -> "ee.FeatureCollection":
    pm25 = load_pm25(year)
    wp = load_worldpop(year)
    pop_source_year = ee.Image(wp).get("pop_source_year")
    pop_image = prep_age_bands(wp)
    pop_image = align_pop_to_pm25_grid(pop_image)

    pm25_x_pop = pm25.multiply(pop_image.select("pop_total")).rename("pm25_x_pop")
    pixel_count = pm25.multiply(0).add(1).rename("pixel_count")
    sum_stack = ee.Image.cat(
        [pop_image, pm25_x_pop, pm25.rename("sum_pm25"), pixel_count]
    )

    valid = _valid_bounds()

    def _slim(f: "ee.Feature") -> "ee.Feature":
        props: dict = {
            "feature_id": f.get(id_field),
            "name": f.get(name_field),
        }
        if country_field:
            props["country_iso3"] = f.get(country_field)
        geom = f.geometry().intersection(valid, 1)
        # simplify_error (metres) collapses the vertex count of pathological
        # admin-2 polygons (fjord-heavy Arctic districts with millions of
        # vertices) that blow past reduceRegions' per-operation timeout. It is
        # only used by the residual-shard relaunch path; at sub-grid tolerance
        # (<1113 m PM2.5 cell) the pop-weighted zonal sums are unchanged for
        # these large, sparsely-populated units.
        if simplify_error:
            geom = geom.simplify(simplify_error)
        return ee.Feature(geom, props)

    slim = boundaries.map(_slim)

    summed = sum_stack.reduceRegions(
        collection=slim,
        reducer=ee.Reducer.sum(),
        crs="EPSG:4326",
        crsTransform=GRID_CRS_TRANSFORM,
        tileScale=tile_scale,
    )

    def _add_mean(f: "ee.Feature") -> "ee.Feature":
        pm25_mean = ee.Number(f.get("sum_pm25")).divide(ee.Number(f.get("pixel_count")))
        return (
            f.set("pm25_mean", pm25_mean)
            .set("year", year)
            .set("pop_source_year", pop_source_year)
        )

    return summed.map(_add_mean)


# ─────────────────────────────────────────────────────────────────────────
#  Task launching
# ─────────────────────────────────────────────────────────────────────────


def _selectors(country_field: str | None) -> list[str]:
    sel = ["feature_id", "name"]
    if country_field:
        sel.append("country_iso3")
    sel += ["year", "pop_source_year", "pop_total", "pm25_x_pop", "pm25_mean"]
    sel += [f"age_{b}" for b in AGE_BINS]
    return sel


def _launch_one(
    fc: "ee.FeatureCollection",
    boundary_name: str,
    year: int,
    suffix: str,
    id_field: str,
    name_field: str,
    country_field: str | None,
    tile_scale: int = TILE_SCALE,
    simplify_error: float | None = None,
) -> "ee.batch.Task":
    stats = compute_stats_for_year(
        fc, year, id_field, name_field, country_field,
        tile_scale=tile_scale, simplify_error=simplify_error,
    )
    task_name = f"pm25_{boundary_name}_{year}{suffix}"
    task = ee.batch.Export.table.toDrive(
        collection=stats,
        description=task_name,
        folder=DRIVE_FOLDER,
        fileNamePrefix=task_name,
        fileFormat="CSV",
        selectors=_selectors(country_field),
    )
    task.start()
    return task


def launch_boundary(
    boundary_name: str,
    years: Iterable[int],
    batches: list[int] | None = None,
    tile_scale: int | None = None,
) -> list[str]:
    """Queue and start every (year, batch) export task for one boundary set.

    Returns the list of task IDs (also printed to stdout).

    Pass ``batches`` to launch only specific batch indices (useful for
    retrying timed-out batches). ``tile_scale`` can only be lowered —
    the default ``TILE_SCALE`` (16) is EE's hard maximum; for batches
    that time out at 16, split them smaller via gee_reexport_missing.py.
    """
    cfg = BOUNDARIES[boundary_name]
    boundaries = ee.FeatureCollection(cfg["asset_id"])
    batch_size = cfg.get("batch_size")
    years = list(years)
    effective_tile_scale = tile_scale if tile_scale is not None else TILE_SCALE

    task_ids: list[str] = []

    if batch_size:
        total = boundaries.size().getInfo()
        n_batches = math.ceil(total / batch_size)

        if batches is None:
            batch_indices = list(range(n_batches))
        else:
            batch_indices = sorted({b for b in batches if 0 <= b < n_batches})
            invalid = sorted({b for b in batches if b < 0 or b >= n_batches})
            if invalid:
                print(
                    f"WARN: skipping out-of-range batch indices {invalid} "
                    f"(valid: 0..{n_batches - 1})"
                )

        scope = "all" if batches is None else f"selected={batch_indices}"
        print(
            f"{boundary_name}: {total:,} features → {n_batches} batches "
            f"of {batch_size}; running {len(batch_indices)} batch(es) "
            f"× {len(years)} year(s) = "
            f"{len(batch_indices) * len(years):,} tasks "
            f"[tileScale={effective_tile_scale}, {scope}]"
        )

        for b in batch_indices:
            batch_list = boundaries.toList(batch_size, b * batch_size)
            batch_fc = ee.FeatureCollection(batch_list)
            suffix = f"_{b:03d}"
            for y in years:
                task = _launch_one(
                    batch_fc,
                    boundary_name,
                    y,
                    suffix,
                    cfg["id_field"],
                    cfg["name_field"],
                    cfg.get("country_field"),
                    tile_scale=effective_tile_scale,
                )
                task_ids.append(task.id)
                print(f"  queued {task.config['description']:<40} id={task.id}")
    else:
        if batches is not None:
            print(
                f"WARN: --batch ignored — boundary '{boundary_name}' has no "
                "batch_size configured (single-FC mode)"
            )
        print(
            f"{boundary_name}: single-FC mode × {len(years)} years "
            f"[tileScale={effective_tile_scale}]"
        )
        for y in years:
            task = _launch_one(
                boundaries,
                boundary_name,
                y,
                "",
                cfg["id_field"],
                cfg["name_field"],
                cfg.get("country_field"),
                tile_scale=effective_tile_scale,
            )
            task_ids.append(task.id)
            print(f"  queued pm25_{boundary_name}_{y:<10} id={task.id}")

    return task_ids


# ─────────────────────────────────────────────────────────────────────────
#  Status polling
# ─────────────────────────────────────────────────────────────────────────


def status_loop(boundary_name: str, poll_seconds: int = 60) -> int:
    """Poll EE task list for tasks matching pm25_<boundary_name>_*.

    Returns 0 once all matching tasks are COMPLETED, 1 if any FAILED.
    """
    prefix = f"pm25_{boundary_name}_"
    print(f"watching tasks with prefix '{prefix}' every {poll_seconds}s ...")
    print("(Ctrl-C to stop watching; tasks keep running on EE.)")

    last_summary = None
    while True:
        tasks = ee.batch.Task.list()
        matching = [
            t for t in tasks
            if (t.config.get("description", "") if hasattr(t, "config") else "")
                .startswith(prefix)
        ]

        states: dict[str, int] = {}
        for t in matching:
            state = t.status().get("state", "UNKNOWN")
            states[state] = states.get(state, 0) + 1

        summary = ", ".join(f"{k}={v}" for k, v in sorted(states.items()))
        if summary != last_summary:
            print(f"  [{time.strftime('%H:%M:%S')}] {len(matching)} tasks  {summary}")
            last_summary = summary

        terminal = {"COMPLETED", "FAILED", "CANCELLED"}
        if matching and all(
            t.status().get("state", "UNKNOWN") in terminal for t in matching
        ):
            failed = [t for t in matching if t.status().get("state") == "FAILED"]
            if failed:
                print(f"\n{len(failed)} tasks FAILED:")
                for t in failed:
                    desc = t.config.get("description", "?")
                    err = t.status().get("error_message", "(no message)")
                    print(f"  {desc}: {err}")
                return 1
            print("\nall tasks finished cleanly.")
            return 0

        time.sleep(poll_seconds)


# ─────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__.split("\n\n", 1)[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--boundary", required=True, choices=sorted(BOUNDARIES),
        help="Boundary set to export (must match a key in BOUNDARIES)",
    )
    p.add_argument(
        "--years", default=None,
        help=(
            "Year range like '2015-2022' or single year '2020'. "
            f"Default: {DEFAULT_YEARS[0]}-{DEFAULT_YEARS[-1]} "
            "(clipped at runtime to whatever PM2.5 collection actually has)."
        ),
    )
    p.add_argument(
        "--batch", default=None,
        help=(
            "Run only specific batch indices, comma-separated (e.g., '13' or "
            "'13,42,87'). Use to retry batches that hit GEE's per-operation "
            "compute timeout. Combine with --tile-scale 32. Ignored for "
            "boundaries without a batch_size config."
        ),
    )
    p.add_argument(
        "--tile-scale", default=None, type=int, dest="tile_scale",
        help=(
            f"Override the default reduceRegions tileScale ({TILE_SCALE}). "
            f"EE caps this at 16, which is already the default — for "
            "'Computation timed out' retries use gee_reexport_missing.py, "
            "which splits batches into smaller per-task chunks."
        ),
    )
    p.add_argument(
        "--status", action="store_true",
        help="Poll task list instead of launching new tasks",
    )
    p.add_argument(
        "--ee-project", default=None,
        help=(
            "Optional Earth Engine cloud project to initialize against. "
            "Defaults to whatever ee.Authenticate() saved. Use this if the "
            "EE-enabled project that owns the assets isn't your default."
        ),
    )
    return p.parse_args(argv)


def parse_years(spec: str | None) -> list[int]:
    if not spec:
        return DEFAULT_YEARS
    if "-" in spec:
        a, b = spec.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(spec)]


def parse_batches(spec: str | None) -> list[int] | None:
    if not spec:
        return None
    return [int(x.strip()) for x in spec.split(",") if x.strip()]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if ee is None:
        sys.stderr.write(
            "earthengine-api not installed. Run:\n"
            "    pip install earthengine-api\n"
            "    python -c 'import ee; ee.Authenticate()'\n"
        )
        return 2

    init_kwargs = {"project": args.ee_project} if args.ee_project else {}
    ee.Initialize(**init_kwargs)

    if args.status:
        return status_loop(args.boundary)

    years = parse_years(args.years)
    batches = parse_batches(args.batch)
    task_ids = launch_boundary(
        args.boundary, years, batches=batches, tile_scale=args.tile_scale
    )
    print(f"\nlaunched {len(task_ids)} tasks. "
          f"watch with: python {sys.argv[0]} --boundary {args.boundary} --status")
    return 0


if __name__ == "__main__":
    sys.exit(main())
