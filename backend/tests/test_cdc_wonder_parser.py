"""Tests for the CDC Wonder TSV parser."""

from pathlib import Path

import pandas as pd

from backend.etl.cdc_wonder.parser import parse_response

FIXTURE = Path(__file__).parent / "fixtures" / "cdc_wonder" / "sample_response.tsv"


def test_parse_response_returns_dataframe():
    df = parse_response(FIXTURE.read_text())
    assert isinstance(df, pd.DataFrame)


def test_parse_response_row_count_excludes_footer():
    df = parse_response(FIXTURE.read_text())
    assert len(df) == 3


def test_parse_response_columns():
    df = parse_response(FIXTURE.read_text())
    assert set(df.columns) >= {"fips", "deaths", "population"}


def test_parse_response_suppressed_becomes_zero():
    df = parse_response(FIXTURE.read_text())
    row = df[df["fips"] == "01005"].iloc[0]
    assert row["deaths"] == 0


def test_parse_response_fips_is_five_digit_string():
    df = parse_response(FIXTURE.read_text())
    assert df["fips"].dtype == object
    for fips in df["fips"]:
        assert isinstance(fips, str)
        assert len(fips) == 5


def test_parse_response_numeric_types():
    df = parse_response(FIXTURE.read_text())
    assert pd.api.types.is_integer_dtype(df["deaths"])
    assert pd.api.types.is_integer_dtype(df["population"])
