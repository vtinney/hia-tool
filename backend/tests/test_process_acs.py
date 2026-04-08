"""Tests for backend/etl/process_acs.py."""

from __future__ import annotations

import math

import pandas as pd

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
