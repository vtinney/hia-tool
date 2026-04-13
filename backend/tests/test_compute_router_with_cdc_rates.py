"""Tests that the compute router stamps US national baseline rates onto CRFs."""

from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from backend.main import app
from backend.services import baseline_rates


@pytest.fixture
def patched_parquet(tmp_path: Path, monkeypatch):
    rows = [
        (2019, "cvd", "25plus", 874569, 224406523, 874569 / 224406523),
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
    yield out


def _base_request() -> dict:
    return {
        "baselineConcentration": 12.0,
        "controlConcentration": 8.0,
        "baselineIncidence": 0.008,
        "population": 1_000_000,
        "selectedCRFs": [
            {
                "id": "krewski",
                "source": "Krewski et al. 2009",
                "endpoint": "Cardiovascular mortality",
                "beta": 0.005827,
                "betaLow": 0.003922,
                "betaHigh": 0.007716,
                "functionalForm": "log-linear",
                "defaultRate": 0.002,
            }
        ],
        "monteCarloIterations": 100,
    }


def test_scalar_request_without_country_code_uses_default_rate(patched_parquet):
    client = TestClient(app)
    resp = client.post("/api/compute", json=_base_request())
    assert resp.status_code == 200
    data = resp.json()
    assert data["results"][0]["attributableCases"]["mean"] > 0


def test_scalar_request_with_us_uses_national_rate(patched_parquet):
    client = TestClient(app)
    req = _base_request()
    req["countryCode"] = "US"
    req["year"] = 2019
    resp = client.post("/api/compute", json=req)
    assert resp.status_code == 200
    data = resp.json()
    assert data["results"][0]["attributableCases"]["mean"] > 0


def test_scalar_request_non_mortality_crf_unchanged(patched_parquet):
    client = TestClient(app)
    req = _base_request()
    req["countryCode"] = "US"
    req["year"] = 2019
    req["selectedCRFs"][0]["endpoint"] = "Asthma ED visits"
    resp = client.post("/api/compute", json=req)
    assert resp.status_code == 200
    assert resp.json()["results"][0]["attributableCases"]["mean"] > 0
