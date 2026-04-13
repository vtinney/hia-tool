#!/usr/bin/env python3
"""ETL: Download CDC Wonder national mortality data and build the HIA
baseline-rate parquet.

Runs 81 queries against the CDC Wonder XML API (9 years x 9 ICD-10
groups), caches raw XML under data/raw/cdc_wonder, and writes a
processed national-level parquet with rates per age bucket.

The CDC Wonder API only supports national-level data (no county/state
grouping). Results are grouped by ten-year age group; the consolidation
step sums into our age buckets (all, 25plus, 65plus).

Usage: python -m backend.etl.process_cdc_wonder
"""

from __future__ import annotations

import logging
import sys

from backend.etl.cdc_wonder.client import CdcWonderClient
from backend.etl.cdc_wonder.consolidate import consolidate
from backend.etl.cdc_wonder.constants import (
    AGE_BUCKETS,
    ICD_GROUPS,
    NATIONAL_PARQUET,
    RAW_DIR,
    YEARS,
    YEAR_TO_DB,
)
from backend.etl.cdc_wonder.xml_builder import build_request_xml


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger("cdc_wonder.main")

    client = CdcWonderClient(cache_root=RAW_DIR)

    # One query per (year, icd_group) — all age groups come back in each response.
    # We request all 11 ten-year age groups so the consolidation step can
    # sum into our age buckets.
    all_age_groups = AGE_BUCKETS["all"]

    combos = [
        (YEAR_TO_DB[year], year, icd_group)
        for year in YEARS
        for icd_group in ICD_GROUPS
    ]
    total = len(combos)
    logger.info("CDC Wonder fetch: %d combinations", total)

    for i, (db, year, icd_group) in enumerate(combos, start=1):
        xml = build_request_xml(
            database=db,
            year=year,
            icd_codes=ICD_GROUPS[icd_group],
            age_groups=all_age_groups,
        )
        try:
            text = client.fetch(
                database=db, year=year, icd_group=icd_group,
                xml_body=xml,
            )
        except RuntimeError as exc:
            logger.error("[%d/%d] %s %d %s — FAILED: %s",
                         i, total, db, year, icd_group, exc)
            continue
        logger.info("[%d/%d] %s %d %s — OK (%d bytes)",
                    i, total, db, year, icd_group, len(text))

    logger.info("consolidating cached XML -> %s", NATIONAL_PARQUET)
    consolidate(
        raw_root=RAW_DIR,
        output_parquet=NATIONAL_PARQUET,
    )

    _print_sanity_check()
    return 0


def _print_sanity_check() -> None:
    """Print the national 2019 all-cause mortality count for a gut-check."""
    import pandas as pd

    if not NATIONAL_PARQUET.exists():
        return
    df = pd.read_parquet(NATIONAL_PARQUET)
    subset = df[
        (df["year"] == 2019)
        & (df["icd_group"] == "all_cause")
        & (df["age_bucket"] == "all")
    ]
    if subset.empty:
        print("WARNING: no 2019 all_cause/all row found")
        return
    total_deaths = int(subset["deaths"].iloc[0])
    total_pop = int(subset["population"].iloc[0])
    rate = float(subset["rate_per_person_year"].iloc[0])
    print("-" * 60)
    print("2019 all-cause mortality sanity check:")
    print(f"  Deaths:     {total_deaths:,}")
    print(f"  Population: {total_pop:,}")
    print(f"  Rate:       {rate:.6f} per person-year")
    print(f"  NCHS publishes ~2,854,838 deaths for 2019;")
    print(f"  within +/-1% (+/-28,548) is expected.")
    print("-" * 60)


if __name__ == "__main__":
    sys.exit(main())
