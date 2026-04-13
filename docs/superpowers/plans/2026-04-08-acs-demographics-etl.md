# ACS Demographics ETL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Build a local ETL pipeline that downloads ACS 5-year estimates (race/ethnicity, median income, poverty) at the census-tract level for vintages 2015–2024 nationwide + Puerto Rico and writes one Parquet file per vintage under `data/processed/demographics/us/`, plus a backend endpoint to serve the data.

**Architecture:** Single Python script `backend/etl/process_acs.py` following the same shape as `backend/etl/process_pm25.py`. Uses `cenpy` to pull ACS tables from the Census API and `pygris` to fetch TIGER/Line cartographic-boundary tract shapefiles. Joins on 11-character GEOID, derives percentage columns, writes Parquet with geometry as WKT. Backend endpoint `GET /api/data/demographics/{country}/{year}` serves the joined data via the existing `data.py` router.

**Tech Stack:** Python 3.11, cenpy, pygris, geopandas, pandas, pyarrow, shapely, pytest, FastAPI.

**Spec:** `docs/superpowers/specs/2026-04-08-acs-demographics-etl-design.md`

---

## File Structure

**Files to create:**
- `backend/etl/process_acs.py` — main ETL script with library functions and CLI
- `backend/tests/test_process_acs.py` — pytest unit tests

**Files to modify:**
- `requirements.txt` — add `cenpy`, `pygris`
- `.env.example` — add `CENSUS_API_KEY` entry
- `README.md` — document `CENSUS_API_KEY` in the environment variables table and add an "ACS demographics" section under "Adding Built-in Data"
- `backend/routers/data.py` — add `/demographics/{country}/{year}` endpoint, extend `_scan_datasets` to handle demographics, skip `"demographics"` in the concentration loop

**Module layout inside `process_acs.py`:**

The file is organized by responsibility, each section a small group of pure functions:

1. Constants — ACS variable codes, vintage/boundary-year mapping, state FIPS list
2. Sentinel-value handling — `clean_sentinels()`
3. ACS table fetcher — `fetch_acs_tables()` with injectable `fetch_fn` for testing
4. TIGER geometry fetcher — `fetch_tract_geometry()`
5. Schema assembly — `build_demographics_frame()` (joins + derived columns + boundary tagging)
6. Parquet writer — `write_parquet_atomic()`
7. Per-vintage orchestrator — `process_vintage()`
8. CLI — `parse_args()`, `main()`

Pure functions (no I/O) get direct unit tests. Functions with I/O (`fetch_acs_tables`, `fetch_tract_geometry`) get dependency injection so tests can pass fake fetchers.

---

## Task 1: Add dependencies and environment configuration

**Files:**
- Modify: `requirements.txt`
- Modify: `.env.example`
- Modify: `README.md`

- [x] **Step 1: Add cenpy and pygris to requirements.txt**

Open `requirements.txt` and append two lines under the `# Geospatial` block (after line 18, after `pyarrow>=15.0.0`):

```
# ACS demographics ETL
cenpy>=1.0.1
pygris>=0.1.6
```

- [x] **Step 2: Install the new dependencies**

Run from project root with the `hia` conda env active:

```bash
pip install cenpy pygris
```

Expected: successful install, no dependency conflicts. If conflicts appear, report them — do not force.

- [x] **Step 3: Verify imports work**

Run:

```bash
python -c "import cenpy; import pygris; print('cenpy', cenpy.__version__); print('pygris', pygris.__version__)"
```

Expected: both versions print without error.

- [x] **Step 4: Add CENSUS_API_KEY to .env.example**

Open `.env.example` and append this block to the end of the file (after the `BASE_URL` line):

```

# Census Bureau API key for the ACS demographics ETL script (required to run process_acs.py)
# Get one free at https://api.census.gov/data/key_signup.html
# Not read by the backend API at runtime — only by the ETL script.
CENSUS_API_KEY=
```

- [x] **Step 5: Document CENSUS_API_KEY in README.md**

Open `README.md`. Find the environment variables table (around line 86). Add this row to the table, immediately after the `MAPBOX_TOKEN` row:

```
| `CENSUS_API_KEY` | *(none)* | Census Bureau API key. Required when running `backend/etl/process_acs.py`. Not read by the backend at runtime. Free at https://api.census.gov/data/key_signup.html |
```

- [x] **Step 6: Commit**

```bash
git add requirements.txt .env.example README.md
git commit -m "deps: add cenpy and pygris for ACS demographics ETL"
```

---

## Task 2: Create process_acs.py skeleton with constants

**Files:**
- Create: `backend/etl/process_acs.py`

- [x] **Step 1: Create the file with module docstring and constants**

Create `backend/etl/process_acs.py` with this content:

```python
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

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Callable

import geopandas as gpd
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
```

- [x] **Step 2: Verify the file parses**

```bash
python -c "from backend.etl import process_acs; print(len(process_acs.STATE_FIPS), 'states'); print(process_acs.boundary_year_for_vintage(2019), process_acs.boundary_year_for_vintage(2020))"
```

Expected output:
```
52 states
2010 2020
```

- [x] **Step 3: Commit**

```bash
git add backend/etl/process_acs.py
git commit -m "feat(etl): add process_acs skeleton with constants"
```

---

## Task 3: Sentinel value cleaning (TDD)

**Files:**
- Create: `backend/tests/test_process_acs.py`
- Modify: `backend/etl/process_acs.py`

- [x] **Step 1: Write the failing test**

Create `backend/tests/test_process_acs.py`:

```python
"""Tests for backend/etl/process_acs.py."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from backend.etl import process_acs


# ────────────────────────────────────────────────────────────────────
#  clean_sentinels
# ────────────────────────────────────────────────────────────────────


def test_clean_sentinels_replaces_known_sentinels_with_nan():
    df = pd.DataFrame({
        "median_hh_income": [50000, -666666666, 75000, -999999999],
        "total_pop": [1200, 3400, 0, 5000],
    })

    cleaned = process_acs.clean_sentinels(df, ["median_hh_income", "total_pop"])

    assert cleaned["median_hh_income"].tolist()[0] == 50000
    assert math.isnan(cleaned["median_hh_income"].tolist()[1])
    assert cleaned["median_hh_income"].tolist()[2] == 75000
    assert math.isnan(cleaned["median_hh_income"].tolist()[3])
    # total_pop had no sentinels — unchanged
    assert cleaned["total_pop"].tolist() == [1200, 3400, 0, 5000]


def test_clean_sentinels_leaves_untouched_columns_alone():
    df = pd.DataFrame({
        "a": [-666666666, 1, 2],
        "b": [-666666666, 1, 2],
    })
    cleaned = process_acs.clean_sentinels(df, ["a"])
    assert math.isnan(cleaned["a"].iloc[0])
    # "b" was not in the target list — still has the sentinel
    assert cleaned["b"].iloc[0] == -666666666


def test_clean_sentinels_returns_new_dataframe_not_mutating_input():
    df = pd.DataFrame({"x": [-666666666, 100]})
    original = df.copy()
    _ = process_acs.clean_sentinels(df, ["x"])
    pd.testing.assert_frame_equal(df, original)
```

- [x] **Step 2: Run test to verify it fails**

```bash
python -m pytest backend/tests/test_process_acs.py -v
```

Expected: All three `clean_sentinels` tests FAIL with `AttributeError: module 'backend.etl.process_acs' has no attribute 'clean_sentinels'`.

- [x] **Step 3: Implement clean_sentinels**

Append to `backend/etl/process_acs.py`, after the constants block:

```python
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
```

- [x] **Step 4: Run test to verify it passes**

```bash
python -m pytest backend/tests/test_process_acs.py -v
```

Expected: All three `clean_sentinels` tests PASS.

- [x] **Step 5: Commit**

```bash
git add backend/etl/process_acs.py backend/tests/test_process_acs.py
git commit -m "feat(etl): add Census sentinel-value cleaning"
```

---

## Task 4: Derived percentage columns (TDD)

**Files:**
- Modify: `backend/tests/test_process_acs.py`
- Modify: `backend/etl/process_acs.py`

- [x] **Step 1: Write the failing test**

Append to `backend/tests/test_process_acs.py`:

```python
# ────────────────────────────────────────────────────────────────────
#  add_derived_columns
# ────────────────────────────────────────────────────────────────────


def test_add_derived_columns_computes_percentages():
    df = pd.DataFrame({
        "total_pop": [1000, 2000],
        "nh_white": [600, 1000],
        "nh_black": [200, 500],
        "nh_aian": [0, 0],
        "nh_asian": [50, 200],
        "nh_nhpi": [0, 0],
        "nh_other_alone": [0, 0],
        "nh_two_or_more": [0, 0],
        "hispanic": [150, 300],
        "pop_poverty_universe": [950, 1900],
        "pop_under_050_pov": [50, 100],
        "pop_050_099_pov": [50, 100],
        "pop_100_124_pov": [50, 100],
        "pop_125_149_pov": [0, 0],
        "pop_150_184_pov": [0, 0],
        "pop_185_199_pov": [0, 0],
    })

    result = process_acs.add_derived_columns(df)

    assert result.loc[0, "pct_nh_white"] == pytest.approx(0.6)
    assert result.loc[0, "pct_nh_black"] == pytest.approx(0.2)
    assert result.loc[0, "pct_hispanic"] == pytest.approx(0.15)
    assert result.loc[0, "pct_minority"] == pytest.approx(0.4)
    # nh_other aggregates alone + two_or_more
    assert result.loc[0, "nh_other"] == 0
    # 100 of 950 below poverty
    assert result.loc[0, "pop_below_100_pov"] == 100
    assert result.loc[0, "pct_below_100_pov"] == pytest.approx(100 / 950)
    # 200 of 950 below 2x poverty (under_050 + 050_099 + 100_124)
    assert result.loc[0, "pop_below_200_pov"] == 200
    assert result.loc[0, "pct_below_200_pov"] == pytest.approx(200 / 950)


def test_add_derived_columns_handles_zero_denominator():
    df = pd.DataFrame({
        "total_pop": [0],
        "nh_white": [0], "nh_black": [0], "nh_aian": [0], "nh_asian": [0],
        "nh_nhpi": [0], "nh_other_alone": [0], "nh_two_or_more": [0],
        "hispanic": [0],
        "pop_poverty_universe": [0],
        "pop_under_050_pov": [0], "pop_050_099_pov": [0],
        "pop_100_124_pov": [0], "pop_125_149_pov": [0],
        "pop_150_184_pov": [0], "pop_185_199_pov": [0],
    })
    result = process_acs.add_derived_columns(df)
    # Division by zero → NaN, not an exception
    assert math.isnan(result.loc[0, "pct_nh_white"])
    assert math.isnan(result.loc[0, "pct_below_100_pov"])
```

- [x] **Step 2: Run tests to verify they fail**

```bash
python -m pytest backend/tests/test_process_acs.py::test_add_derived_columns_computes_percentages backend/tests/test_process_acs.py::test_add_derived_columns_handles_zero_denominator -v
```

Expected: Both tests FAIL with `AttributeError: module 'backend.etl.process_acs' has no attribute 'add_derived_columns'`.

- [x] **Step 3: Implement add_derived_columns**

Append to `backend/etl/process_acs.py` after `clean_sentinels`:

```python
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

    total = out["total_pop"].replace(0, pd.NA)
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

    pov_universe = out["pop_poverty_universe"].replace(0, pd.NA)
    out["pct_below_100_pov"] = out["pop_below_100_pov"] / pov_universe
    out["pct_below_200_pov"] = out["pop_below_200_pov"] / pov_universe

    return out
```

- [x] **Step 4: Run tests to verify they pass**

```bash
python -m pytest backend/tests/test_process_acs.py -v
```

Expected: All tests in the file PASS (5 so far).

- [x] **Step 5: Commit**

```bash
git add backend/etl/process_acs.py backend/tests/test_process_acs.py
git commit -m "feat(etl): add derived percentage columns for ACS"
```

---

## Task 5: ACS table fetcher with injectable fetcher (TDD)

**Files:**
- Modify: `backend/tests/test_process_acs.py`
- Modify: `backend/etl/process_acs.py`

The fetcher function takes a `fetch_fn` callable so tests can pass a fake that returns a canned DataFrame. The production `fetch_fn` wraps `cenpy`.

- [x] **Step 1: Write the failing test**

Append to `backend/tests/test_process_acs.py`:

```python
# ────────────────────────────────────────────────────────────────────
#  fetch_acs_tables
# ────────────────────────────────────────────────────────────────────


def test_fetch_acs_tables_calls_fetcher_per_state_and_concatenates():
    """The fetcher is called once per state; results are concatenated."""
    calls: list[tuple[int, str]] = []

    def fake_fetch(vintage: int, state_fips: str) -> pd.DataFrame:
        calls.append((vintage, state_fips))
        # Return a minimal frame with one synthetic tract per state
        return pd.DataFrame({
            "state": [state_fips],
            "county": ["001"],
            "tract": ["000100"],
            "B03002_001E": [1000],
            "B03002_003E": [500],
            "B03002_004E": [200],
            "B03002_005E": [0],
            "B03002_006E": [50],
            "B03002_007E": [0],
            "B03002_008E": [0],
            "B03002_009E": [0],
            "B03002_012E": [250],
            "B19013_001E": [55000],
            "C17002_001E": [950],
            "C17002_002E": [50],
            "C17002_003E": [50],
            "C17002_004E": [50],
            "C17002_005E": [0],
            "C17002_006E": [0],
            "C17002_007E": [0],
        })

    result = process_acs.fetch_acs_tables(
        vintage=2022,
        state_fips_list=("06", "36"),  # California, New York
        fetch_fn=fake_fetch,
    )

    # Two states → two calls → two rows
    assert len(calls) == 2
    assert calls[0] == (2022, "06")
    assert calls[1] == (2022, "36")
    assert len(result) == 2
    assert set(result["state"]) == {"06", "36"}


def test_fetch_acs_tables_retries_on_transient_failure():
    """Transient failures should be retried up to 3 times."""
    attempts = {"count": 0}

    def flaky_fetch(vintage: int, state_fips: str) -> pd.DataFrame:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ConnectionError("transient network error")
        return pd.DataFrame({
            "state": [state_fips], "county": ["001"], "tract": ["000100"],
            "B03002_001E": [100], "B03002_003E": [50], "B03002_004E": [20],
            "B03002_005E": [0], "B03002_006E": [5], "B03002_007E": [0],
            "B03002_008E": [0], "B03002_009E": [0], "B03002_012E": [25],
            "B19013_001E": [50000], "C17002_001E": [95],
            "C17002_002E": [5], "C17002_003E": [5], "C17002_004E": [5],
            "C17002_005E": [0], "C17002_006E": [0], "C17002_007E": [0],
        })

    result = process_acs.fetch_acs_tables(
        vintage=2022, state_fips_list=("06",), fetch_fn=flaky_fetch, retry_sleep=0,
    )
    assert attempts["count"] == 3
    assert len(result) == 1


def test_fetch_acs_tables_aborts_after_exhausting_retries():
    """After 3 failed attempts for one state, the whole call should raise."""
    def always_fail(vintage: int, state_fips: str) -> pd.DataFrame:
        raise ConnectionError("permanent failure")

    with pytest.raises(ConnectionError):
        process_acs.fetch_acs_tables(
            vintage=2022, state_fips_list=("06",), fetch_fn=always_fail, retry_sleep=0,
        )
```

- [x] **Step 2: Run tests to verify they fail**

```bash
python -m pytest backend/tests/test_process_acs.py -v -k fetch_acs_tables
```

Expected: All three `fetch_acs_tables` tests FAIL with `AttributeError: module 'backend.etl.process_acs' has no attribute 'fetch_acs_tables'`.

- [x] **Step 3: Implement fetch_acs_tables**

Append to `backend/etl/process_acs.py`:

```python
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
```

- [x] **Step 4: Run tests to verify they pass**

```bash
python -m pytest backend/tests/test_process_acs.py -v -k fetch_acs_tables
```

Expected: All three `fetch_acs_tables` tests PASS.

- [x] **Step 5: Commit**

```bash
git add backend/etl/process_acs.py backend/tests/test_process_acs.py
git commit -m "feat(etl): add ACS table fetcher with retry and DI"
```

---

## Task 6: Production cenpy-backed fetcher

**Files:**
- Modify: `backend/etl/process_acs.py`

This is the real fetcher that wraps `cenpy`. Not unit-testable without hitting the network; verified manually in the smoke test task (Task 12).

> **Note:** `cenpy`'s public API has shifted across versions. The implementation below targets the modern `cenpy.remote.APIConnection(...).query(...)` idiom. If the smoke test in Task 12 fails with an API-shape error, adjust this function only — every other function in this plan uses dependency injection and does not import cenpy. Check `cenpy`'s current README for the right call shape (e.g., `cenpy.products.ACS(year).from_state(...)`) and update `cenpy_fetch` to return the same DataFrame shape documented in its docstring.

- [x] **Step 1: Implement cenpy_fetch**

Append to `backend/etl/process_acs.py`:

```python
def cenpy_fetch(vintage: int, state_fips: str) -> pd.DataFrame:
    """Fetch ACS 5-year tables B03002, B19013, C17002 for all tracts in one state.

    Uses cenpy's ACSDetail API. Requires ``CENSUS_API_KEY`` in the environment;
    cenpy reads it automatically if set.

    Returns a DataFrame with columns: state, county, tract, and one column per
    variable in ``ACS_VARIABLES`` (using the Census codes, e.g. "B03002_001E").
    """
    import cenpy  # imported lazily so tests can run without the dep installed

    dataset_name = f"ACSDT5Y{vintage}"
    conn = cenpy.remote.APIConnection(dataset_name)

    variables = list(ACS_VARIABLES.keys())

    df = conn.query(
        cols=variables,
        geo_unit="tract:*",
        geo_filter={"state": state_fips},
    )

    # cenpy returns object-dtype columns — coerce numeric ones
    for var in variables:
        if var in df.columns:
            df[var] = pd.to_numeric(df[var], errors="coerce")

    # Normalize key columns to strings with zero-padding
    df["state"] = df["state"].astype(str).str.zfill(2)
    df["county"] = df["county"].astype(str).str.zfill(3)
    df["tract"] = df["tract"].astype(str).str.zfill(6)

    return df[["state", "county", "tract"] + variables]
```

- [x] **Step 2: Verify the module still imports**

```bash
python -c "from backend.etl.process_acs import cenpy_fetch; print('ok')"
```

Expected: `ok` (lazy `import cenpy` keeps this fast even if the library is broken).

- [x] **Step 3: Commit**

```bash
git add backend/etl/process_acs.py
git commit -m "feat(etl): add production cenpy-backed ACS fetcher"
```

---

## Task 7: TIGER tract geometry fetcher

**Files:**
- Modify: `backend/tests/test_process_acs.py`
- Modify: `backend/etl/process_acs.py`

- [x] **Step 1: Write the failing test (with injected fetcher)**

Append to `backend/tests/test_process_acs.py`:

```python
# ────────────────────────────────────────────────────────────────────
#  fetch_tract_geometry
# ────────────────────────────────────────────────────────────────────


def test_fetch_tract_geometry_reprojects_to_4326_and_builds_geoid():
    """The fetcher must return EPSG:4326 geometries with an 11-char GEOID column."""
    from shapely.geometry import Polygon
    import geopandas as gpd

    # Build a fake TIGER response in a non-4326 CRS
    fake_gdf = gpd.GeoDataFrame(
        {
            "STATEFP": ["06", "06"],
            "COUNTYFP": ["001", "075"],
            "TRACTCE": ["400100", "020100"],
            "geometry": [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
            ],
        },
        crs="EPSG:4269",  # NAD83 — TIGER default
    )

    def fake_pygris_fetch(year: int, cb: bool):
        assert cb is True
        return fake_gdf

    result = process_acs.fetch_tract_geometry(
        vintage=2022, fetch_fn=fake_pygris_fetch,
    )

    assert result.crs == "EPSG:4326"
    assert "geoid" in result.columns
    # 11-char GEOID = state (2) + county (3) + tract (6)
    assert result["geoid"].tolist() == ["06001400100", "06075020100"]
    # state_fips / county_fips / tract_code columns exist for downstream use
    assert result["state_fips"].tolist() == ["06", "06"]
    assert result["county_fips"].tolist() == ["001", "075"]
    assert result["tract_code"].tolist() == ["400100", "020100"]
```

- [x] **Step 2: Run test to verify it fails**

```bash
python -m pytest backend/tests/test_process_acs.py::test_fetch_tract_geometry_reprojects_to_4326_and_builds_geoid -v
```

Expected: FAIL with `AttributeError: module 'backend.etl.process_acs' has no attribute 'fetch_tract_geometry'`.

- [x] **Step 3: Implement fetch_tract_geometry**

Append to `backend/etl/process_acs.py`:

```python
# ────────────────────────────────────────────────────────────────────
#  TIGER geometry fetching
# ────────────────────────────────────────────────────────────────────


def _pygris_fetch(year: int, cb: bool) -> gpd.GeoDataFrame:
    """Production fetcher — calls pygris.tracts for all US tracts."""
    import pygris  # lazy import
    return pygris.tracts(year=year, cb=cb, cache=True)


def fetch_tract_geometry(
    vintage: int,
    fetch_fn: Callable[[int, bool], gpd.GeoDataFrame] = _pygris_fetch,
) -> gpd.GeoDataFrame:
    """Download all US tract cartographic-boundary shapefiles for a vintage.

    Returns a GeoDataFrame in EPSG:4326 with these normalized columns:
      - ``geoid`` (11-char tract GEOID)
      - ``state_fips`` (2 chars)
      - ``county_fips`` (3 chars)
      - ``tract_code`` (6 chars)
      - ``geometry`` (shapely polygon in EPSG:4326)

    Any other columns from the raw TIGER file are dropped.
    """
    logger.info("Fetching TIGER cb tract geometry for vintage %d...", vintage)
    gdf = fetch_fn(vintage, True)

    if gdf.crs is None:
        raise ValueError(
            f"TIGER tract shapefile for {vintage} has no CRS — cannot reproject"
        )
    if not gdf.crs.equals("EPSG:4326"):
        logger.info("Reprojecting tract geometry from %s to EPSG:4326", gdf.crs)
        gdf = gdf.to_crs("EPSG:4326")

    # Normalize column names — TIGER uses STATEFP, COUNTYFP, TRACTCE
    gdf = gdf.rename(
        columns={"STATEFP": "state_fips", "COUNTYFP": "county_fips", "TRACTCE": "tract_code"}
    )

    # Ensure zero-padded strings
    gdf["state_fips"] = gdf["state_fips"].astype(str).str.zfill(2)
    gdf["county_fips"] = gdf["county_fips"].astype(str).str.zfill(3)
    gdf["tract_code"] = gdf["tract_code"].astype(str).str.zfill(6)

    gdf["geoid"] = gdf["state_fips"] + gdf["county_fips"] + gdf["tract_code"]

    return gdf[["geoid", "state_fips", "county_fips", "tract_code", "geometry"]]
```

- [x] **Step 4: Run tests to verify they pass**

```bash
python -m pytest backend/tests/test_process_acs.py -v
```

Expected: All tests so far PASS.

- [x] **Step 5: Commit**

```bash
git add backend/etl/process_acs.py backend/tests/test_process_acs.py
git commit -m "feat(etl): add TIGER tract geometry fetcher"
```

---

## Task 8: Schema assembly — build_demographics_frame (TDD)

**Files:**
- Modify: `backend/tests/test_process_acs.py`
- Modify: `backend/etl/process_acs.py`

This is the "join and shape" step: takes the raw ACS frame + geometry frame, renames Census codes to friendly column names, joins on GEOID, adds vintage + boundary_year columns, runs sentinel cleaning and derived columns.

- [x] **Step 1: Write the failing test**

Append to `backend/tests/test_process_acs.py`:

```python
# ────────────────────────────────────────────────────────────────────
#  build_demographics_frame
# ────────────────────────────────────────────────────────────────────


def test_build_demographics_frame_joins_renames_and_tags_vintage():
    from shapely.geometry import Polygon
    import geopandas as gpd

    acs_raw = pd.DataFrame({
        "state": ["06", "06"],
        "county": ["001", "075"],
        "tract": ["400100", "020100"],
        "B03002_001E": [1000, 2000],
        "B03002_003E": [600, 1000],
        "B03002_004E": [200, 500],
        "B03002_005E": [0, 0],
        "B03002_006E": [50, 200],
        "B03002_007E": [0, 0],
        "B03002_008E": [0, 0],
        "B03002_009E": [0, 0],
        "B03002_012E": [150, 300],
        "B19013_001E": [55000, -666666666],  # second tract has sentinel
        "C17002_001E": [950, 1900],
        "C17002_002E": [50, 100],
        "C17002_003E": [50, 100],
        "C17002_004E": [50, 100],
        "C17002_005E": [0, 0],
        "C17002_006E": [0, 0],
        "C17002_007E": [0, 0],
    })

    geom = gpd.GeoDataFrame(
        {
            "geoid": ["06001400100", "06075020100"],
            "state_fips": ["06", "06"],
            "county_fips": ["001", "075"],
            "tract_code": ["400100", "020100"],
            "geometry": [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
            ],
        },
        crs="EPSG:4326",
    )

    result = process_acs.build_demographics_frame(
        acs_raw=acs_raw, geometry=geom, vintage=2022,
    )

    # 2 rows, joined successfully
    assert len(result) == 2
    # Friendly column names
    assert "total_pop" in result.columns
    assert "median_hh_income" in result.columns
    # Vintage + boundary tagging
    assert (result["vintage"] == 2022).all()
    assert (result["boundary_year"] == 2020).all()
    # Sentinel became NaN
    assert result.loc[result["geoid"] == "06075020100", "median_hh_income"].isna().iloc[0]
    # Derived columns exist
    assert "pct_nh_white" in result.columns
    assert result.loc[result["geoid"] == "06001400100", "pct_nh_white"].iloc[0] == pytest.approx(0.6)
    # Geometry preserved
    assert "geometry" in result.columns
    # GEOID parts
    assert result["geoid"].tolist() == ["06001400100", "06075020100"]


def test_build_demographics_frame_uses_2010_boundary_year_for_2019_vintage():
    import geopandas as gpd
    from shapely.geometry import Polygon

    acs_raw = pd.DataFrame({
        "state": ["06"], "county": ["001"], "tract": ["400100"],
        "B03002_001E": [100], "B03002_003E": [50], "B03002_004E": [20],
        "B03002_005E": [0], "B03002_006E": [5], "B03002_007E": [0],
        "B03002_008E": [0], "B03002_009E": [0], "B03002_012E": [25],
        "B19013_001E": [50000], "C17002_001E": [95],
        "C17002_002E": [5], "C17002_003E": [5], "C17002_004E": [5],
        "C17002_005E": [0], "C17002_006E": [0], "C17002_007E": [0],
    })
    geom = gpd.GeoDataFrame(
        {"geoid": ["06001400100"], "state_fips": ["06"], "county_fips": ["001"],
         "tract_code": ["400100"],
         "geometry": [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])]},
        crs="EPSG:4326",
    )
    result = process_acs.build_demographics_frame(acs_raw=acs_raw, geometry=geom, vintage=2019)
    assert result["boundary_year"].iloc[0] == 2010
```

- [x] **Step 2: Run tests to verify they fail**

```bash
python -m pytest backend/tests/test_process_acs.py -v -k build_demographics_frame
```

Expected: Both tests FAIL with `AttributeError`.

- [x] **Step 3: Implement build_demographics_frame**

Append to `backend/etl/process_acs.py`:

```python
# ────────────────────────────────────────────────────────────────────
#  Schema assembly
# ────────────────────────────────────────────────────────────────────


def build_demographics_frame(
    acs_raw: pd.DataFrame,
    geometry: gpd.GeoDataFrame,
    vintage: int,
) -> gpd.GeoDataFrame:
    """Join ACS raw counts to tract geometry, rename, clean, and derive columns.

    Parameters
    ----------
    acs_raw : pd.DataFrame
        Output of ``fetch_acs_tables``. Must contain ``state``, ``county``,
        ``tract`` and all keys in ``ACS_VARIABLES``.
    geometry : gpd.GeoDataFrame
        Output of ``fetch_tract_geometry``. Must contain ``geoid`` and
        ``geometry`` (EPSG:4326).
    vintage : int
        ACS 5-year end year; used to tag rows and pick the boundary era.

    Returns
    -------
    gpd.GeoDataFrame
        Tract-level frame with all columns defined in the spec.
    """
    # Build GEOID on the ACS frame
    df = acs_raw.copy()
    df["geoid"] = (
        df["state"].astype(str).str.zfill(2)
        + df["county"].astype(str).str.zfill(3)
        + df["tract"].astype(str).str.zfill(6)
    )

    # Rename Census variable codes to friendly names
    df = df.rename(columns=ACS_VARIABLES)

    # Clean sentinels on all numeric variables
    numeric_cols = list(ACS_VARIABLES.values())
    df = clean_sentinels(df, numeric_cols)

    # Add derived columns
    df = add_derived_columns(df)

    # Tag vintage + boundary era
    df["vintage"] = vintage
    df["boundary_year"] = boundary_year_for_vintage(vintage)

    # Join to geometry (inner join on GEOID — drops ACS rows without geometry
    # and vice versa, which should not happen in practice)
    merged = geometry.merge(
        df.drop(columns=["state", "county", "tract"]),
        on="geoid",
        how="inner",
    )

    # Reorder columns to match spec
    column_order = [
        "geoid", "state_fips", "county_fips", "tract_code",
        "vintage", "boundary_year",
        "total_pop",
        "nh_white", "nh_black", "nh_aian", "nh_asian", "nh_nhpi", "nh_other", "hispanic",
        "pct_nh_white", "pct_nh_black", "pct_hispanic", "pct_minority",
        "median_hh_income",
        "pop_poverty_universe", "pop_below_100_pov", "pop_below_200_pov",
        "pct_below_100_pov", "pct_below_200_pov",
        "geometry",
    ]
    # Keep only columns that exist (defensive — all should exist)
    column_order = [c for c in column_order if c in merged.columns]
    merged = merged[column_order]

    return merged
```

- [x] **Step 4: Run tests to verify they pass**

```bash
python -m pytest backend/tests/test_process_acs.py -v
```

Expected: All tests PASS.

- [x] **Step 5: Commit**

```bash
git add backend/etl/process_acs.py backend/tests/test_process_acs.py
git commit -m "feat(etl): add ACS schema assembly with join and derived columns"
```

---

## Task 9: Atomic Parquet writer (TDD)

**Files:**
- Modify: `backend/tests/test_process_acs.py`
- Modify: `backend/etl/process_acs.py`

- [x] **Step 1: Write the failing test**

Append to `backend/tests/test_process_acs.py`:

```python
# ────────────────────────────────────────────────────────────────────
#  write_parquet_atomic
# ────────────────────────────────────────────────────────────────────


def test_write_parquet_atomic_writes_wkt_geometry(tmp_path):
    from shapely.geometry import Polygon
    import geopandas as gpd

    gdf = gpd.GeoDataFrame(
        {
            "geoid": ["06001400100"],
            "vintage": [2022],
            "total_pop": [1000],
            "geometry": [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        },
        crs="EPSG:4326",
    )

    out_path = tmp_path / "sub" / "dir" / "2022.parquet"
    process_acs.write_parquet_atomic(gdf, out_path)

    assert out_path.exists()
    # Read it back — geometry should be stored as WKT string
    roundtrip = pd.read_parquet(out_path)
    assert roundtrip["geometry"].iloc[0].startswith("POLYGON")
    assert roundtrip["geoid"].iloc[0] == "06001400100"
    assert roundtrip["total_pop"].iloc[0] == 1000


def test_write_parquet_atomic_does_not_leave_tmp_file_on_success(tmp_path):
    import geopandas as gpd
    from shapely.geometry import Polygon

    gdf = gpd.GeoDataFrame(
        {"geoid": ["1"], "geometry": [Polygon([(0, 0), (1, 0), (1, 1)])]},
        crs="EPSG:4326",
    )
    out_path = tmp_path / "out.parquet"
    process_acs.write_parquet_atomic(gdf, out_path)

    # No stray .tmp file
    tmp_files = list(tmp_path.glob("*.tmp"))
    assert tmp_files == []
```

- [x] **Step 2: Run tests to verify they fail**

```bash
python -m pytest backend/tests/test_process_acs.py -v -k write_parquet_atomic
```

Expected: FAIL with `AttributeError`.

- [x] **Step 3: Implement write_parquet_atomic**

Append to `backend/etl/process_acs.py`:

```python
# ────────────────────────────────────────────────────────────────────
#  Parquet writer
# ────────────────────────────────────────────────────────────────────


def write_parquet_atomic(gdf: gpd.GeoDataFrame, output_path: Path) -> None:
    """Write a GeoDataFrame to Parquet atomically, with WKT geometry.

    Writes to ``<output_path>.tmp`` first, then renames into place so a
    reader never sees a half-written file.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Data to write. Its geometry column is serialized as WKT strings.
    output_path : Path
        Destination. Parent directories are created if they do not exist.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Serialize geometry to WKT (matches process_pm25.py convention)
    out = pd.DataFrame(gdf.drop(columns=["geometry"]))
    out["geometry"] = gdf.geometry.apply(lambda g: g.wkt if g is not None else None)

    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    out.to_parquet(tmp_path, index=False, engine="pyarrow")
    tmp_path.replace(output_path)

    logger.info(
        "Wrote %d rows to %s (%.1f MB)",
        len(out),
        output_path,
        output_path.stat().st_size / (1024 * 1024),
    )
```

- [x] **Step 4: Run tests to verify they pass**

```bash
python -m pytest backend/tests/test_process_acs.py -v
```

Expected: All tests PASS.

- [x] **Step 5: Commit**

```bash
git add backend/etl/process_acs.py backend/tests/test_process_acs.py
git commit -m "feat(etl): add atomic Parquet writer with WKT geometry"
```

---

## Task 10: Per-vintage orchestrator and CLI

**Files:**
- Modify: `backend/etl/process_acs.py`

This wires everything together and adds the argparse CLI. No unit tests for the orchestrator itself (it is pure glue); the smoke test in Task 12 exercises it end-to-end.

- [x] **Step 1: Add process_vintage orchestrator**

Append to `backend/etl/process_acs.py`:

```python
# ────────────────────────────────────────────────────────────────────
#  Orchestrator
# ────────────────────────────────────────────────────────────────────


def process_vintage(
    vintage: int,
    output_path: Path,
    state_fips_list: tuple[str, ...] = STATE_FIPS,
    acs_fetch_fn: Callable[[int, str], pd.DataFrame] = cenpy_fetch,
    geom_fetch_fn: Callable[[int, bool], gpd.GeoDataFrame] = _pygris_fetch,
) -> None:
    """Run the full ETL for one ACS vintage and write the Parquet file."""
    t0 = time.perf_counter()
    logger.info("=== Processing ACS %d 5-year ===", vintage)

    acs_raw = fetch_acs_tables(
        vintage=vintage, state_fips_list=state_fips_list, fetch_fn=acs_fetch_fn,
    )
    logger.info("Fetched %d raw ACS rows", len(acs_raw))

    geometry = fetch_tract_geometry(vintage=vintage, fetch_fn=geom_fetch_fn)
    logger.info("Fetched %d tract geometries", len(geometry))

    merged = build_demographics_frame(acs_raw=acs_raw, geometry=geometry, vintage=vintage)
    logger.info("Joined frame: %d rows", len(merged))

    write_parquet_atomic(merged, output_path)
    logger.info("Vintage %d complete in %.1f s", vintage, time.perf_counter() - t0)
```

- [x] **Step 2: Add CLI entry point**

Append to `backend/etl/process_acs.py`:

```python
# ────────────────────────────────────────────────────────────────────
#  CLI
# ────────────────────────────────────────────────────────────────────


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download and process ACS 5-year demographics by census tract.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--vintage", type=int, choices=SUPPORTED_VINTAGES,
        help="Process a single ACS 5-year vintage (end year).",
    )
    group.add_argument(
        "--all", action="store_true",
        help=f"Process all supported vintages: {SUPPORTED_VINTAGES}.",
    )
    parser.add_argument(
        "--output", type=Path, default=None,
        help="Output Parquet file path (required with --vintage, ignored with --all).",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data/processed/demographics/us"),
        help="Output directory for --all mode (default: data/processed/demographics/us).",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable debug logging.",
    )
    args = parser.parse_args(argv)

    if args.vintage and args.output is None:
        parser.error("--output is required when --vintage is specified")

    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Load .env so CENSUS_API_KEY is picked up
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    if not os.getenv("CENSUS_API_KEY"):
        logger.warning(
            "CENSUS_API_KEY is not set. cenpy may fail or throttle heavily. "
            "Get a free key at https://api.census.gov/data/key_signup.html"
        )

    t_start = time.perf_counter()

    if args.all:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        for vintage in SUPPORTED_VINTAGES:
            out = args.output_dir / f"{vintage}.parquet"
            process_vintage(vintage=vintage, output_path=out)
    else:
        process_vintage(vintage=args.vintage, output_path=args.output)

    logger.info("All done in %.1f s", time.perf_counter() - t_start)


if __name__ == "__main__":
    main()
```

- [x] **Step 3: Verify argparse wiring**

```bash
python -m backend.etl.process_acs --help
```

Expected: help text lists `--vintage`, `--all`, `--output`, `--output-dir`, `--verbose`.

- [x] **Step 4: Verify argparse rejects missing --output with --vintage**

```bash
python -m backend.etl.process_acs --vintage 2022
```

Expected: exits with error `argument --output is required when --vintage is specified`.

- [x] **Step 5: Run full test suite**

```bash
python -m pytest backend/tests/test_process_acs.py -v
```

Expected: All tests PASS.

- [x] **Step 6: Commit**

```bash
git add backend/etl/process_acs.py
git commit -m "feat(etl): add ACS orchestrator and CLI with --all mode"
```

---

## Task 11: Backend endpoint for demographics

**Files:**
- Modify: `backend/routers/data.py`

The existing `_scan_datasets` function is NOT recursive in a general way — it hard-codes branches for `"population"` and `"incidence"` and treats everything else top-level as a concentration dataset. Without this task, a new `demographics/` directory would be misclassified as a pollutant. This task:

1. Skips `"demographics"` in the concentration loop.
2. Adds a demographics branch to `_scan_datasets`.
3. Adds a `GET /api/data/demographics/{country}/{year}` endpoint that returns GeoJSON.

- [x] **Step 1: Skip "demographics" in the concentration loop**

In `backend/routers/data.py`, find the `_scan_datasets` function (around line 228). Locate this block:

```python
        if key in ("population", "incidence"):
            continue
```

Change it to:

```python
        if key in ("population", "incidence", "demographics"):
            continue
```

- [x] **Step 2: Add the demographics scan branch**

In the same `_scan_datasets` function, after the incidence scan block ends (around line 304, just before `return datasets`), add:

```python
    # Demographics datasets: demographics/{country}/{year}.parquet
    demo_dir = DATA_ROOT / "demographics"
    if demo_dir.exists():
        for country_dir in sorted(demo_dir.iterdir()):
            if not country_dir.is_dir():
                continue
            years = sorted(
                int(f.stem) for f in country_dir.iterdir()
                if f.suffix in (".parquet", ".csv") and f.stem.isdigit()
            )
            if years:
                datasets.append({
                    "type": "demographics",
                    "country": country_dir.name,
                    "years": years,
                    "source": "ACS 5-year estimates (B03002, B19013, C17002)",
                })
```

- [x] **Step 3: Add the GET /api/data/demographics endpoint**

In `backend/routers/data.py`, after the incidence endpoint (after `get_incidence` ends, around line 220), add:

```python
# ────────────────────────────────────────────────────────────────────
#  4. Demographics (ACS)
# ────────────────────────────────────────────────────────────────────


@router.get("/demographics/{country}/{year}")
async def get_demographics(country: str, year: int):
    """Return GeoJSON with ACS 5-year demographics by census tract.

    Reads from ``data/processed/demographics/{country}/{year}.parquet``
    (output of ``backend/etl/process_acs.py``).
    """
    try:
        directory = _resolve_path("demographics", country)
        file_path = _find_file(directory, str(year))
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"No demographics data for {country}/{year}",
        )

    df = _read_table(file_path)
    return _df_to_geojson(df)
```

- [x] **Step 4: Run existing backend tests to make sure nothing regressed**

```bash
python -m pytest backend/tests/ -v
```

Expected: All existing tests PASS. The new endpoint has no automated test (it requires data on disk — the smoke test in Task 12 exercises it manually).

- [x] **Step 5: Verify the router still imports cleanly**

```bash
python -c "from backend.routers import data; print('ok'); print([r.path for r in data.router.routes])"
```

Expected: `ok` followed by a list of routes that includes `/api/data/demographics/{country}/{year}`.

- [x] **Step 6: Commit**

```bash
git add backend/routers/data.py
git commit -m "feat(api): serve ACS demographics at /api/data/demographics/{country}/{year}"
```

---

## Task 12: Manual smoke test — one small state, one vintage

**Files:** None created; this task verifies the pipeline end-to-end.

This is a manual test because it requires the Census API key and hits the network. Rhode Island (FIPS `44`) has ~250 tracts — the smallest state, safe and fast to test.

- [x] **Step 1: Ensure CENSUS_API_KEY is set**

Make sure your `.env` has a real key. Then verify:

```bash
python -c "from dotenv import load_dotenv; load_dotenv(); import os; print('key present:', bool(os.getenv('CENSUS_API_KEY')))"
```

Expected: `key present: True`.

- [x] **Step 2: Run a single-state smoke test via a short Python snippet**

Create a temporary file `backend/etl/_smoke_acs.py` (do not commit — deleted in step 5):

```python
"""One-off smoke test: fetch Rhode Island 2022 ACS and write it."""
from pathlib import Path
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

from backend.etl.process_acs import process_vintage

process_vintage(
    vintage=2022,
    output_path=Path("data/processed/demographics/us/_smoke_ri_2022.parquet"),
    state_fips_list=("44",),  # Rhode Island only
)
```

Run:

```bash
python -m backend.etl._smoke_acs
```

Expected: log lines showing ACS fetch for state 44, TIGER geometry download, join, and a final line reporting ~250 rows written. No exceptions.

- [x] **Step 3: Inspect the output Parquet**

```bash
python -c "
import pandas as pd
df = pd.read_parquet('data/processed/demographics/us/_smoke_ri_2022.parquet')
print('rows:', len(df))
print('columns:', list(df.columns))
print(df[['geoid', 'total_pop', 'pct_nh_white', 'median_hh_income', 'pct_below_100_pov']].head())
print('geometry sample:', df['geometry'].iloc[0][:80])
"
```

Expected:
- Row count approximately 240–260 (Rhode Island tract count; exact number depends on the 2020 redefinition).
- All spec columns present.
- `geometry` column contains WKT strings starting with `POLYGON` or `MULTIPOLYGON`.
- `pct_nh_white` values are between 0 and 1.
- `median_hh_income` has plausible values (most between 20000 and 200000; some may be NaN).

- [x] **Step 4: Hit the backend endpoint**

Start the backend in one terminal:

```bash
python -m uvicorn backend.main:app --port 8000
```

In another terminal, with the smoke-test file renamed so the endpoint finds it:

```bash
# Endpoint expects the file named {year}.parquet. Copy it so we don't clobber
# a real 2022 build.
cp data/processed/demographics/us/_smoke_ri_2022.parquet data/processed/demographics/us/2022.parquet

curl -s http://localhost:8000/api/data/demographics/us/2022 | python -c "
import sys, json
data = json.load(sys.stdin)
print('type:', data.get('type'))
print('feature count:', len(data.get('features', [])))
print('first props keys:', list(data['features'][0]['properties'].keys())[:6])
"

# Also verify dataset listing picks it up
curl -s 'http://localhost:8000/api/data/datasets?type=demographics' | python -m json.tool
```

Expected:
- `type: FeatureCollection`
- feature count matches the row count from Step 3
- Listing endpoint returns a `demographics` entry with `country: "us"` and `years: [2022]`.

- [x] **Step 5: Clean up the smoke-test artifacts**

```bash
rm data/processed/demographics/us/_smoke_ri_2022.parquet
rm data/processed/demographics/us/2022.parquet
rm backend/etl/_smoke_acs.py
```

- [x] **Step 6: Commit (if any) or skip**

Nothing to commit from this task — it is verification only. If you added `_smoke_acs.py` to git by accident, unstage it:

```bash
git status          # confirm no unwanted files staged
```

---

## Summary

Task | What it delivers
---|---
1 | Deps, env config, README docs
2 | `process_acs.py` skeleton with constants
3 | Sentinel cleaning (pure function + tests)
4 | Derived percentage columns (pure function + tests)
5 | ACS fetcher with retry and DI (pure-ish + tests)
6 | Production cenpy fetcher (uninspected by tests)
7 | TIGER geometry fetcher with CRS normalization (tested with DI)
8 | Schema assembly — join, rename, clean, derive, tag (tested)
9 | Atomic Parquet writer with WKT (tested)
10 | Orchestrator + CLI (`--vintage` and `--all`)
11 | Backend `/api/data/demographics` endpoint + dataset listing
12 | End-to-end manual smoke test with Rhode Island

Running `python -m backend.etl.process_acs --all --verbose` after Task 10 produces all ten vintage Parquet files. The backend serves them immediately after Task 11.
