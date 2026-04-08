#!/usr/bin/env python3
"""ETL: Fetch American Community Survey (ACS) 5-year estimates by census tract.

Downloads race/ethnicity (B03002), median household income (B19013), and
poverty status (C17002) for all US census tracts (including Puerto Rico) for
a given ACS 5-year vintage, joins them to TIGER/Line cartographic-boundary
tract geometries, and writes a Parquet file with one row per tract.

Usage
-----
    python -m backend.etl.process_acs \
        --vintage 2022 \
        --output data/processed/demographics/us/2022.parquet

    # Process all vintages 2015-2024 in one run
    python -m backend.etl.process_acs --all

Requires CENSUS_API_KEY in the environment (or .env file).
"""

from __future__ import annotations

import logging
import time
from typing import Callable

import pandas as pd

logger = logging.getLogger("process_acs")


# ────────────────────────────────────────────────────────────────────
#  Constants
# ────────────────────────────────────────────────────────────────────

# ACS 5-year vintages we support. End year of the 5-year window.
SUPPORTED_VINTAGES: tuple[int, ...] = (
    2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024,
)

# Tract boundary era. Vintages 2015-2019 use 2010 tract geography;
# vintages 2020+ use 2020 tract geography.
def boundary_year_for_vintage(vintage: int) -> int:
    """Return the tract-boundary decennial year for an ACS vintage."""
    if vintage < 2020:
        return 2010
    return 2020


# Census sentinel values for "not available" / "not applicable"
# (see https://www.census.gov/data/developers/data-sets/acs-5year/data-notes.html)
CENSUS_SENTINELS: frozenset[int] = frozenset({
    -666666666,
    -999999999,
    -888888888,
    -222222222,
    -333333333,
    -555555555,
})

# ACS variable codes (Estimate columns — suffix "E")
ACS_VARIABLES: dict[str, str] = {
    # B03002 — Hispanic or Latino Origin by Race
    "B03002_001E": "total_pop",
    "B03002_003E": "nh_white",
    "B03002_004E": "nh_black",
    "B03002_005E": "nh_aian",
    "B03002_006E": "nh_asian",
    "B03002_007E": "nh_nhpi",
    "B03002_008E": "nh_other_alone",     # "Some other race alone"
    "B03002_009E": "nh_two_or_more",     # "Two or more races"
    "B03002_012E": "hispanic",
    # B19013 — Median household income
    "B19013_001E": "median_hh_income",
    # C17002 — Ratio of income to poverty level
    "C17002_001E": "pop_poverty_universe",
    "C17002_002E": "pop_under_050_pov",
    "C17002_003E": "pop_050_099_pov",
    "C17002_004E": "pop_100_124_pov",
    "C17002_005E": "pop_125_149_pov",
    "C17002_006E": "pop_150_184_pov",
    "C17002_007E": "pop_185_199_pov",
}

# State FIPS codes: 50 states + DC (11) + Puerto Rico (72).
# Excludes territories beyond PR (American Samoa, Guam, etc.) which ACS does
# not fully cover at the tract level.
STATE_FIPS: tuple[str, ...] = (
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
    "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
    "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
    "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
    "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
    "56", "72",
)


# ────────────────────────────────────────────────────────────────────
#  Sentinel handling
# ────────────────────────────────────────────────────────────────────


def clean_sentinels(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Replace Census sentinel values with NaN in the given columns.

    Census uses specific negative integers (e.g. -666666666) to indicate
    "not available" or "not applicable". These must be converted to NaN
    before any arithmetic, otherwise they corrupt sums and ratios.

    Parameters
    ----------
    df : pd.DataFrame
        Input frame (not mutated).
    columns : list[str]
        Columns to scan. Columns not in *df* are silently skipped.

    Returns
    -------
    pd.DataFrame
        A new DataFrame with sentinels replaced by NaN in the target columns.
    """
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            continue
        out[col] = out[col].where(~out[col].isin(CENSUS_SENTINELS), other=pd.NA)
        # Cast to float so NaN can be represented (integer columns cannot hold NaN)
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


# ────────────────────────────────────────────────────────────────────
#  Derived columns
# ────────────────────────────────────────────────────────────────────


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add percentage and aggregate columns used by the HIA tool.

    Expects the raw count columns from B03002 and C17002. Adds:
      - nh_other (nh_other_alone + nh_two_or_more)
      - pct_nh_white, pct_nh_black, pct_hispanic, pct_minority
      - pop_below_100_pov, pop_below_200_pov
      - pct_below_100_pov, pct_below_200_pov

    Division by zero yields NaN, not an exception.
    """
    out = df.copy()

    # Aggregate "other" race
    out["nh_other"] = out["nh_other_alone"].fillna(0) + out["nh_two_or_more"].fillna(0)

    total = out["total_pop"].astype("float64").replace(0, float("nan"))
    out["pct_nh_white"] = out["nh_white"] / total
    out["pct_nh_black"] = out["nh_black"] / total
    out["pct_hispanic"] = out["hispanic"] / total
    out["pct_minority"] = 1.0 - out["pct_nh_white"]

    # Poverty aggregates (C17002 ratios:
    #  _002 = <0.50, _003 = 0.50-0.99, _004 = 1.00-1.24, _005 = 1.25-1.49,
    #  _006 = 1.50-1.84, _007 = 1.85-1.99)
    out["pop_below_100_pov"] = (
        out["pop_under_050_pov"].fillna(0) + out["pop_050_099_pov"].fillna(0)
    )
    out["pop_below_200_pov"] = (
        out["pop_below_100_pov"]
        + out["pop_100_124_pov"].fillna(0)
        + out["pop_125_149_pov"].fillna(0)
        + out["pop_150_184_pov"].fillna(0)
        + out["pop_185_199_pov"].fillna(0)
    )

    pov_universe = out["pop_poverty_universe"].astype("float64").replace(0, float("nan"))
    out["pct_below_100_pov"] = out["pop_below_100_pov"] / pov_universe
    out["pct_below_200_pov"] = out["pop_below_200_pov"] / pov_universe

    return out


# ────────────────────────────────────────────────────────────────────
#  ACS table fetching
# ────────────────────────────────────────────────────────────────────


def fetch_acs_tables(
    vintage: int,
    state_fips_list: tuple[str, ...],
    fetch_fn: Callable[[int, str], pd.DataFrame],
    max_retries: int = 3,
    retry_sleep: float = 2.0,
) -> pd.DataFrame:
    """Fetch ACS variables for all tracts in every state, concatenate into one frame.

    Parameters
    ----------
    vintage : int
        ACS 5-year end year (e.g., 2022).
    state_fips_list : tuple[str, ...]
        Two-character state FIPS codes to query.
    fetch_fn : Callable[[int, str], pd.DataFrame]
        Function that fetches one state's worth of data. Production code
        passes a cenpy-backed wrapper; tests pass a fake.
    max_retries : int
        Number of attempts per state before giving up (default 3).
    retry_sleep : float
        Base sleep seconds between retries; doubled each attempt.

    Returns
    -------
    pd.DataFrame
        Concatenated results across all states.

    Raises
    ------
    ConnectionError
        If any state fails all retry attempts. The whole call fails so the
        script never writes a partial file.
    """
    frames: list[pd.DataFrame] = []
    for state in state_fips_list:
        logger.info("Fetching ACS %d tables for state %s...", vintage, state)
        last_err: Exception | None = None
        for attempt in range(1, max_retries + 1):
            try:
                df = fetch_fn(vintage, state)
                frames.append(df)
                break
            except Exception as err:  # noqa: BLE001 — we rethrow if attempts exhausted
                last_err = err
                if attempt < max_retries:
                    sleep_s = retry_sleep * (2 ** (attempt - 1))
                    logger.warning(
                        "State %s attempt %d failed: %s — retrying in %.1fs",
                        state, attempt, err, sleep_s,
                    )
                    time.sleep(sleep_s)
        else:
            # Loop completed without break → all retries exhausted
            raise ConnectionError(
                f"Failed to fetch ACS {vintage} for state {state} after "
                f"{max_retries} attempts: {last_err}"
            ) from last_err

    return pd.concat(frames, ignore_index=True)
