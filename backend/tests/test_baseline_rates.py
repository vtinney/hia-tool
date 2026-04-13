"""Tests for backend.services.baseline_rates."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services import baseline_rates


@pytest.fixture
def tiny_parquet(tmp_path: Path, monkeypatch) -> Path:
    """Write a 3-county parquet and point the service at it."""
    rows = [
        ("06037", "06", 2019, "cvd", "25plus", 20000, 6_000_000, 20000 / 6_000_000),
        ("06037", "06", 2019, "ihd", "25plus",  8000, 6_000_000,  8000 / 6_000_000),
        ("36061", "36", 2019, "cvd", "25plus", 10000, 1_200_000, 10000 / 1_200_000),
        ("01001", "01", 2019, "cvd", "25plus",     0,         0, 0.0),
    ]
    df = pd.DataFrame(rows, columns=[
        "fips", "state_fips", "year", "icd_group",
        "age_bucket", "deaths", "population", "rate_per_person_year",
    ])
    df["year"] = df["year"].astype("int16")
    df["icd_group"] = df["icd_group"].astype("category")
    df["age_bucket"] = df["age_bucket"].astype("category")
    df["deaths"] = df["deaths"].astype("int32")
    df["population"] = df["population"].astype("int32")
    df["rate_per_person_year"] = df["rate_per_person_year"].astype("float32")

    out = tmp_path / "cdc_wonder_mortality.parquet"
    df.to_parquet(out, index=False)
    monkeypatch.setattr(baseline_rates, "_PARQUET_PATH", out)
    baseline_rates._clear_cache()
    return out


def test_non_mortality_endpoint_returns_none(tiny_parquet):
    assert baseline_rates.get_baseline_rate(
        crf_endpoint="Asthma ED visits", year=2019, fips="06037"
    ) is None


def test_none_fips_returns_none(tiny_parquet):
    assert baseline_rates.get_baseline_rate(
        crf_endpoint="Cardiovascular mortality", year=2019, fips=None
    ) is None


def test_scalar_fips_returns_float(tiny_parquet):
    rate = baseline_rates.get_baseline_rate(
        crf_endpoint="Cardiovascular mortality", year=2019, fips="06037",
    )
    assert isinstance(rate, float)
    assert abs(rate - 20000 / 6_000_000) < 1e-4


def test_missing_county_returns_zero(tiny_parquet):
    rate = baseline_rates.get_baseline_rate(
        crf_endpoint="Cardiovascular mortality", year=2019, fips="99999",
    )
    assert rate == 0.0


def test_county_with_zero_population_returns_zero(tiny_parquet):
    rate = baseline_rates.get_baseline_rate(
        crf_endpoint="Cardiovascular mortality", year=2019, fips="01001",
    )
    assert rate == 0.0


def test_list_fips_returns_array(tiny_parquet):
    rates = baseline_rates.get_baseline_rate(
        crf_endpoint="Cardiovascular mortality",
        year=2019,
        fips=["06037", "36061", "99999"],
    )
    assert isinstance(rates, np.ndarray)
    assert rates.shape == (3,)
    assert abs(rates[0] - 20000 / 6_000_000) < 1e-4
    assert abs(rates[1] - 10000 / 1_200_000) < 1e-4
    assert rates[2] == 0.0


def test_ihd_uses_25plus_bucket(tiny_parquet):
    rate = baseline_rates.get_baseline_rate(
        crf_endpoint="Ischemic heart disease", year=2019, fips="06037",
    )
    assert abs(rate - 8000 / 6_000_000) < 1e-4
