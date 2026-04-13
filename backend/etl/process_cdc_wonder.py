#!/usr/bin/env python3
"""ETL: Download CDC Wonder county-level mortality and build the HIA
baseline-rate parquets.

Runs 216 queries against the CDC Wonder XML API (9 years x 8 ICD-10
groups x 3 age buckets), caches raw TSVs under data/raw/cdc_wonder,
and writes two processed parquets under data/processed/incidence/us.

Usage: python -m backend.etl.process_cdc_wonder
"""

from __future__ import annotations

import logging
import sys

from backend.etl.cdc_wonder.client import CdcWonderClient
from backend.etl.cdc_wonder.consolidate import consolidate
from backend.etl.cdc_wonder.constants import (
    AGE_BUCKETS,
    COUNTY_PARQUET,
    ICD_GROUPS,
    PROCESSED_DIR,
    RAW_DIR,
    STATE_PARQUET,
    YEARS,
    YEAR_TO_DB,
)
from backend.etl.cdc_wonder.xml_builder import build_request_xml


def _load_master_fips() -> list[str]:
    """Return the master list of 5-digit US county FIPS codes."""
    from pathlib import Path
    import pandas as pd

    candidates = [
        Path("data/processed/boundaries/us_county_fips.csv"),
        Path("data/raw/boundaries/us_county_fips.csv"),
    ]
    for cand in candidates:
        if cand.exists():
            df = pd.read_csv(cand, dtype=str)
            col = "fips" if "fips" in df.columns else df.columns[0]
            return sorted({f.zfill(5) for f in df[col].dropna()})

    import requests
    url = (
        "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt"
    )
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    fips: set[str] = set()
    for line in resp.text.splitlines():
        parts = line.split("|")
        if len(parts) < 3:
            continue
        if parts[0] == "STATE":
            continue
        state_fips = parts[1].strip()
        county_fips = parts[2].strip()
        if state_fips.isdigit() and county_fips.isdigit():
            fips.add(f"{state_fips.zfill(2)}{county_fips.zfill(3)}")
    if not fips:
        raise RuntimeError("Failed to load master county FIPS list")
    candidates[0].parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"fips": sorted(fips)}).to_csv(candidates[0], index=False)
    return sorted(fips)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger("cdc_wonder.main")

    client = CdcWonderClient(cache_root=RAW_DIR)

    combos = [
        (YEAR_TO_DB[year], year, icd_group, age_bucket)
        for year in YEARS
        for icd_group in ICD_GROUPS
        for age_bucket in AGE_BUCKETS
    ]
    total = len(combos)
    logger.info("CDC Wonder fetch: %d combinations", total)

    for i, (db, year, icd_group, age_bucket) in enumerate(combos, start=1):
        xml = build_request_xml(
            database=db,
            year=year,
            icd_codes=ICD_GROUPS[icd_group],
            age_groups=AGE_BUCKETS[age_bucket],
        )
        try:
            text = client.fetch(
                database=db, year=year, icd_group=icd_group,
                age_bucket=age_bucket, xml_body=xml,
            )
        except RuntimeError as exc:
            logger.error("[%d/%d] %s %d %s %s — FAILED: %s",
                         i, total, db, year, icd_group, age_bucket, exc)
            continue
        logger.info("[%d/%d] %s %d %s %s — OK (%d bytes)",
                    i, total, db, year, icd_group, age_bucket, len(text))

    logger.info("consolidating cached TSVs -> %s", COUNTY_PARQUET)
    master_fips = _load_master_fips()
    consolidate(
        raw_root=RAW_DIR,
        county_parquet=COUNTY_PARQUET,
        state_parquet=STATE_PARQUET,
        master_fips=master_fips,
    )

    _print_sanity_check()
    return 0


def _print_sanity_check() -> None:
    """Print the national 2019 all-cause mortality count for a gut-check."""
    import pandas as pd

    if not COUNTY_PARQUET.exists():
        return
    df = pd.read_parquet(COUNTY_PARQUET)
    subset = df[
        (df["year"] == 2019)
        & (df["icd_group"] == "all_cause")
        & (df["age_bucket"] == "all")
    ]
    total_deaths = int(subset["deaths"].sum())
    total_pop = int(subset["population"].sum())
    print("-" * 60)
    print(f"2019 all-cause mortality sanity check:")
    print(f"  Deaths:     {total_deaths:,}")
    print(f"  Population: {total_pop:,}")
    print(f"  NCHS publishes ~2,854,838 deaths for 2019;")
    print(f"  within +/-1% (+/-28,548) is expected.")
    print("-" * 60)


if __name__ == "__main__":
    sys.exit(main())
