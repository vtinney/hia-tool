"""Tests for the CDC Wonder XML parser."""

from pathlib import Path

import pandas as pd

from backend.etl.cdc_wonder.parser import parse_response

FIXTURE = Path(__file__).parent / "fixtures" / "cdc_wonder" / "sample_response.xml"


def test_parse_response_returns_dataframe():
    df = parse_response(FIXTURE.read_text())
    assert isinstance(df, pd.DataFrame)


def test_parse_response_row_count_excludes_not_stated():
    df = parse_response(FIXTURE.read_text())
    assert len(df) == 7


def test_parse_response_columns():
    df = parse_response(FIXTURE.read_text())
    assert set(df.columns) == {"age_group", "deaths", "population"}


def test_parse_response_handles_commas_in_numbers():
    df = parse_response(FIXTURE.read_text())
    row = df[df["age_group"] == "75-84 years"].iloc[0]
    assert row["deaths"] == 214683
    assert row["population"] == 15969872


def test_parse_response_numeric_types():
    df = parse_response(FIXTURE.read_text())
    assert pd.api.types.is_integer_dtype(df["deaths"])
    assert pd.api.types.is_integer_dtype(df["population"])


def test_parse_response_empty_on_bad_xml():
    df = parse_response("not valid xml at all")
    assert df.empty
    assert list(df.columns) == ["age_group", "deaths", "population"]
