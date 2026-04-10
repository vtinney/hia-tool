"""Tests for the three GBD pollution CSV parsers."""

from pathlib import Path

import pandas as pd
import pytest

from backend.etl.gbd_pollution.parsers import (
    parse_no2_csv,
    parse_ozone_csv,
    parse_pm25_csv,
)

FIXTURES = Path(__file__).parent / "fixtures" / "gbd_pollution"

NORMALIZED_COLUMNS = {
    "pollutant", "gbd_location_id", "ihme_loc_id", "location_name",
    "year", "mean", "lower", "upper", "unit", "release",
}


def test_parse_no2_columns_and_year_filter():
    df = parse_no2_csv(FIXTURES / "sample_no2.csv")
    assert set(df.columns) == NORMALIZED_COLUMNS
    # 2014 row is below YEAR_MIN and must be dropped
    assert (df["year"] >= 2015).all()
    # 3 rows remain (2019 Global, 2019 China, 2023 Indonesia)
    assert len(df) == 3


def test_parse_no2_fields():
    df = parse_no2_csv(FIXTURES / "sample_no2.csv")
    china = df[df["gbd_location_id"] == 6].iloc[0]
    assert china["pollutant"] == "no2"
    assert china["ihme_loc_id"] == "CHN"
    assert china["location_name"] == "China"
    assert china["year"] == 2019
    assert abs(china["mean"] - 16.2) < 1e-6
    assert china["unit"] == "ppb"
    assert china["release"] == "gbd_2023"


def test_parse_ozone_columns_and_year_filter():
    df = parse_ozone_csv(FIXTURES / "sample_ozone.csv")
    assert set(df.columns) == NORMALIZED_COLUMNS
    assert (df["year"] >= 2015).all()
    # 3 rows remain: 2019 Global, 2019 China, 2021 China
    assert len(df) == 3


def test_parse_ozone_release_and_ihme_is_null():
    df = parse_ozone_csv(FIXTURES / "sample_ozone.csv")
    assert (df["release"] == "gbd_2021").all()
    # Ozone source file has no ihme_loc_id column — must be None
    assert df["ihme_loc_id"].isna().all()


def test_parse_pm25_columns_and_year_filter():
    df = parse_pm25_csv(FIXTURES / "sample_pm25.csv")
    assert set(df.columns) == NORMALIZED_COLUMNS
    assert (df["year"] >= 2015).all()
    # 3 rows remain: 2019 Global, 2019 China, 2023 Indonesia
    assert len(df) == 3


def test_parse_pm25_unit_normalization_and_missing_name():
    df = parse_pm25_csv(FIXTURES / "sample_pm25.csv")
    # All PM2.5 rows are ug/m3, not the raw "micrograms per cubic meter"
    assert (df["unit"] == "ug_m3").all()
    # PM source has no location_name column — must be None
    assert df["location_name"].isna().all()
    assert df["ihme_loc_id"].isna().all()
    assert (df["release"] == "gbd_2023").all()
