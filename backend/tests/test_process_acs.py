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
    # 150 of 950 below 2x poverty (under_050 + 050_099 + 100_124)
    assert result.loc[0, "pop_below_200_pov"] == 150
    assert result.loc[0, "pct_below_200_pov"] == pytest.approx(150 / 950)


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


# ────────────────────────────────────────────────────────────────────
#  fetch_acs_tables
# ────────────────────────────────────────────────────────────────────


def test_fetch_acs_tables_calls_fetcher_per_state_and_concatenates():
    """The fetcher is called once per state; results are concatenated."""
    calls: list[tuple[int, str]] = []

    def fake_fetch(vintage: int, state_fips: str) -> pd.DataFrame:
        calls.append((vintage, state_fips))
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
