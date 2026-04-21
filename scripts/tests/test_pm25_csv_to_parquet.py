"""Tests for pm25_csv_to_parquet."""
from pathlib import Path

import pandas as pd
import pytest

from scripts.pm25_csv_to_parquet import (
    AGE_COLUMNS,
    compute_popweighted,
    group_csvs,
    load_csv,
    write_parquet,
)

FIXTURE = Path(__file__).parent / "fixtures" / "sample_pm25.csv"


def test_load_csv_returns_expected_columns():
    df = load_csv(FIXTURE)
    expected_cols = {
        "feature_id", "name", "year", "pop_source_year",
        "pop_total", "pm25_x_pop", "pm25_mean",
    } | set(AGE_COLUMNS)
    assert expected_cols.issubset(df.columns)


def test_load_csv_dtypes():
    df = load_csv(FIXTURE)
    assert df["year"].dtype.kind == "i"
    assert df["pop_source_year"].dtype.kind == "i"
    assert df["pop_total"].dtype.kind == "f"
    assert df["pm25_mean"].dtype.kind == "f"
    assert df["feature_id"].dtype == object


def test_compute_popweighted_matches_manual_ratio():
    df = load_csv(FIXTURE)
    out = compute_popweighted(df)
    # India 2015: 78_000_000_000 / 1_300_000_000 = 60.0
    india_2015 = out[(out.feature_id == "IND") & (out.year == 2015)].iloc[0]
    assert india_2015["pm25_popweighted"] == pytest.approx(60.0, rel=1e-6)
    # USA 2015: 3_840_000_000 / 320_000_000 = 12.0
    usa_2015 = out[(out.feature_id == "USA") & (out.year == 2015)].iloc[0]
    assert usa_2015["pm25_popweighted"] == pytest.approx(12.0, rel=1e-6)


def test_compute_popweighted_drops_intermediate():
    out = compute_popweighted(load_csv(FIXTURE))
    assert "pm25_x_pop" not in out.columns
    assert "pm25_popweighted" in out.columns


def test_group_csvs_strips_year_and_batch_suffixes():
    paths = [
        Path("pm25_ne_countries_2015.csv"),
        Path("pm25_ne_countries_2016.csv"),
        Path("pm25_ghs_smod_2015_000.csv"),
        Path("pm25_ghs_smod_2015_001.csv"),
        Path("pm25_ghs_smod_2016_000.csv"),
        Path("pm25_ne_states.csv"),
    ]
    groups = group_csvs(paths)
    assert set(groups.keys()) == {
        "pm25_ne_countries", "pm25_ghs_smod", "pm25_ne_states"
    }
    assert len(groups["pm25_ne_countries"]) == 2
    assert len(groups["pm25_ghs_smod"]) == 3
    assert len(groups["pm25_ne_states"]) == 1


def test_write_parquet_roundtrip(tmp_path):
    df = compute_popweighted(load_csv(FIXTURE))
    out_path = tmp_path / "pm25_sample.parquet"
    write_parquet(df, out_path)
    assert out_path.exists()
    roundtrip = pd.read_parquet(out_path)
    assert len(roundtrip) == len(df)
    assert list(roundtrip.columns) == list(df.columns)
    assert roundtrip["pm25_popweighted"].iloc[0] == pytest.approx(
        df["pm25_popweighted"].iloc[0]
    )
