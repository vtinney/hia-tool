#!/usr/bin/env python3
"""ETL: Process GBD baseline mortality/morbidity rate CSVs into a single
consolidated Parquet for the HIA tool.

Reads CSV exports from the IHME GBD Results Tool (one per cause),
filters to Rate rows (metric_id=3), normalizes from per-100K to
per-person-year, optionally joins the Natural Earth crosswalk, and
writes data/processed/incidence/gbd_rates.parquet.

Usage: python -m backend.etl.process_gbd_rates
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

logger = logging.getLogger("gbd_rates_etl")

# ────────────────────────────────────────────────────────────────────
#  Constants
# ────────────────────────────────────────────────────────────────────

RAW_DIR = Path("data/raw/gbd/rates")
OUTPUT_PATH = Path("data/processed/incidence/gbd_rates.parquet")
CROSSWALK_PATH = Path("data/processed/boundaries/gbd_to_ne.csv")

FILENAME_TO_SLUG: dict[str, str] = {
    "all_cause_mortality": "all_cause",
    "ihd": "ihd",
    "stroke": "stroke",
    "copd": "copd",
    "lri": "lri",
    "lung_cancer": "lung_cancer",
    "dementia": "dementia",
    "dm2": "dm2",
    "asthma": "asthma",
}

AGE_NAME_TO_GROUP: dict[str, str] = {
    "All ages": "all_ages",
    "<20 years": "under_20",
}

# Map IHME ``measure_name`` → short slug. ``Deaths`` lets the router
# distinguish mortality causes from incidence causes that share the
# same ``cause`` slug (e.g. asthma is incidence-only; lung cancer has
# both mortality and incidence in separate files).
MEASURE_SLUG: dict[str, str] = {
    "Deaths": "deaths",
    "Incidence": "incidence",
    "Prevalence": "prevalence",
    "YLDs (Years Lived with Disability)": "ylds",
    "YLLs (Years of Life Lost)": "ylls",
    "DALYs (Disability-Adjusted Life Years)": "dalys",
}

SEX_SLUG: dict[str, str] = {
    "Both": "both",
    "Male": "male",
    "Female": "female",
}

RATE_METRIC_ID = 3


# ────────────────────────────────────────────────────────────────────
#  Processing
# ────────────────────────────────────────────────────────────────────

def _process_single_csv(csv_path: Path, cause_slug: str) -> pd.DataFrame:
    """Read one GBD rate CSV, filter to Rate rows, normalize."""
    df = pd.read_csv(csv_path)

    # Keep only Rate rows
    df = df[df["metric_id"] == RATE_METRIC_ID].copy()

    if df.empty:
        logger.warning("No Rate rows in %s", csv_path.name)
        return pd.DataFrame()

    # Normalize age group
    df["age_group"] = df["age_name"].map(AGE_NAME_TO_GROUP)
    unmapped = df["age_group"].isna()
    if unmapped.any():
        unknown = df.loc[unmapped, "age_name"].unique().tolist()
        logger.warning("Unknown age groups in %s: %s — defaulting to 'all_ages'",
                       csv_path.name, unknown)
        df.loc[unmapped, "age_group"] = "all_ages"

    # Normalize measure / sex to short slugs
    df["measure"] = (
        df["measure_name"].map(MEASURE_SLUG).fillna(
            df["measure_name"].str.lower().str.replace(" ", "_")
        )
    )
    df["sex"] = (
        df["sex_name"].map(SEX_SLUG).fillna(df["sex_name"].str.lower())
    )

    # Normalize rates: IHME publishes per-100K, we store per-person-year.
    df["rate"] = df["val"] / 100_000
    df["rate_lower"] = df["lower"] / 100_000
    df["rate_upper"] = df["upper"] / 100_000

    return (
        df[[
            "location_id", "location_name", "year", "age_group",
            "measure", "sex", "rate", "rate_lower", "rate_upper",
        ]]
        .rename(columns={"location_id": "gbd_location_id"})
        .assign(cause=cause_slug)
    )


def process_gbd_rates(
    *,
    raw_dir: Path | None = None,
    output_path: Path | None = None,
    crosswalk_path: Path | None = None,
) -> Path:
    """Run the full GBD rates ETL pipeline.

    Parameters can be overridden for testing. Defaults use the
    module-level constants.
    """
    raw_dir = raw_dir or RAW_DIR
    output_path = output_path or OUTPUT_PATH
    crosswalk_path = crosswalk_path or CROSSWALK_PATH

    frames: list[pd.DataFrame] = []

    for csv_file in sorted(raw_dir.glob("*.csv")):
        stem = csv_file.stem
        slug = FILENAME_TO_SLUG.get(stem)
        if slug is None:
            logger.warning("Skipping unrecognized file: %s", csv_file.name)
            continue

        logger.info("Processing %s -> %s", csv_file.name, slug)
        frame = _process_single_csv(csv_file, slug)
        if not frame.empty:
            frames.append(frame)

    if not frames:
        raise RuntimeError(f"No data processed. Check that CSVs exist in {raw_dir}")

    df = pd.concat(frames, ignore_index=True)
    logger.info("Concatenated %d rows across %d causes", len(df), len(frames))

    # Cast types
    df["gbd_location_id"] = df["gbd_location_id"].astype("int32")
    df["year"] = df["year"].astype("int16")

    # Optionally join crosswalk
    df["ne_country_iso3"] = None
    df["ne_country_uid"] = None
    df["ne_state_uid"] = None

    if crosswalk_path.exists():
        logger.info("Joining crosswalk from %s", crosswalk_path)
        xwalk = pd.read_csv(crosswalk_path, usecols=[
            "gbd_location_id", "ne_country_iso3", "ne_country_uid", "ne_state_uid",
        ])
        df = df.drop(columns=["ne_country_iso3", "ne_country_uid", "ne_state_uid"])
        df = df.merge(xwalk, on="gbd_location_id", how="left")
    else:
        logger.info("No crosswalk found at %s — NE columns will be NULL", crosswalk_path)

    # Reorder columns
    df = df[
        ["cause", "gbd_location_id", "location_name", "year",
         "age_group", "measure", "sex",
         "rate", "rate_lower", "rate_upper",
         "ne_country_iso3", "ne_country_uid", "ne_state_uid"]
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow", index=False)
    logger.info("Wrote %d rows to %s", len(df), output_path)

    return output_path


# ────────────────────────────────────────────────────────────────────
#  CLI
# ────────────────────────────────────────────────────────────────────

def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not RAW_DIR.exists():
        logger.error("Raw GBD rates directory not found: %s", RAW_DIR)
        return 1

    try:
        process_gbd_rates()
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 1

    # Print summary
    df = pd.read_parquet(OUTPUT_PATH)
    print(f"\nGBD rates summary:")
    print(f"  Total rows: {len(df):,}")
    print(f"  Causes: {sorted(df['cause'].unique().tolist())}")
    print(f"  Locations: {df['gbd_location_id'].nunique()}")
    print(f"  Years: {sorted(df['year'].unique().tolist())}")
    has_xwalk = df["ne_country_iso3"].notna().any()
    print(f"  Crosswalk joined: {has_xwalk}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
