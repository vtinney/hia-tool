"""Tests for backend.services.baseline_rates."""

from pathlib import Path

import pandas as pd
import pytest

from backend.services import baseline_rates


@pytest.fixture
def tiny_parquet(tmp_path: Path, monkeypatch) -> Path:
    """Write a national-level parquet and point the service at it."""
    rows = [
        (2019, "cvd", "25plus", 874569, 224406523, 874569 / 224406523),
        (2019, "ihd", "25plus", 365744, 224406523, 365744 / 224406523),
        (2019, "cvd", "all", 874613, 328239523, 874613 / 328239523),
        (2019, "respiratory", "25plus", 156979, 224406523, 156979 / 224406523),
    ]
    df = pd.DataFrame(rows, columns=[
        "year", "icd_group", "age_bucket",
        "deaths", "population", "rate_per_person_year",
    ])
    df["year"] = df["year"].astype("int16")
    df["icd_group"] = df["icd_group"].astype("category")
    df["age_bucket"] = df["age_bucket"].astype("category")

    out = tmp_path / "cdc_wonder_mortality_national.parquet"
    df.to_parquet(out, index=False)
    monkeypatch.setattr(baseline_rates, "_PARQUET_PATH", out)
    baseline_rates._clear_cache()
    return out


def test_non_mortality_endpoint_returns_none(tiny_parquet):
    assert baseline_rates.get_baseline_rate(
        crf_endpoint="Asthma ED visits", year=2019, country_code="US"
    ) is None


def test_non_us_returns_none(tiny_parquet):
    assert baseline_rates.get_baseline_rate(
        crf_endpoint="Cardiovascular mortality", year=2019, country_code=None
    ) is None


def test_us_cvd_returns_float(tiny_parquet):
    rate = baseline_rates.get_baseline_rate(
        crf_endpoint="Cardiovascular mortality", year=2019, country_code="US",
    )
    assert isinstance(rate, float)
    assert abs(rate - 874569 / 224406523) < 1e-6


def test_ihd_uses_25plus_bucket(tiny_parquet):
    rate = baseline_rates.get_baseline_rate(
        crf_endpoint="Ischemic heart disease", year=2019, country_code="US",
    )
    assert abs(rate - 365744 / 224406523) < 1e-6


def test_missing_year_returns_none(tiny_parquet):
    rate = baseline_rates.get_baseline_rate(
        crf_endpoint="Cardiovascular mortality", year=2025, country_code="US",
    )
    assert rate is None


def test_respiratory_maps_correctly(tiny_parquet):
    rate = baseline_rates.get_baseline_rate(
        crf_endpoint="Respiratory mortality", year=2019, country_code="US",
    )
    assert isinstance(rate, float)
    assert rate > 0
