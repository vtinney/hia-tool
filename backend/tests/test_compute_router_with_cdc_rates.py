"""Tests that the compute router stamps per-county baseline rates onto CRFs."""

from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from backend.main import app
from backend.services import baseline_rates


@pytest.fixture
def patched_parquet(tmp_path: Path, monkeypatch):
    rows = [
        ("06037", "06", 2019, "cvd", "25plus",
         20000, 6_000_000, 20000 / 6_000_000),
        ("36061", "36", 2019, "cvd", "25plus",
         10000, 1_200_000, 10000 / 1_200_000),
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
        "monteCarloIterations": 0,
    }


def test_scalar_request_without_country_code_uses_default_rate(patched_parquet):
    client = TestClient(app)
    resp = client.post("/api/compute", json=_base_request())
    assert resp.status_code == 200
    data = resp.json()
    assert data["results"][0]["attributableCases"]["mean"] > 0


def test_scalar_request_with_us_fips_uses_county_rate(patched_parquet):
    client = TestClient(app)
    req = _base_request()
    req["countryCode"] = "US"
    req["fipsCodes"] = ["06037"]
    req["year"] = 2019
    resp = client.post("/api/compute", json=req)
    assert resp.status_code == 200
    data = resp.json()

    req["fipsCodes"] = ["36061"]
    resp2 = client.post("/api/compute", json=req)
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert (
        data2["results"][0]["attributableCases"]["mean"]
        > data["results"][0]["attributableCases"]["mean"]
    )


def test_scalar_request_non_mortality_crf_unchanged(patched_parquet):
    client = TestClient(app)
    req = _base_request()
    req["countryCode"] = "US"
    req["fipsCodes"] = ["06037"]
    req["year"] = 2019
    req["selectedCRFs"][0]["endpoint"] = "Asthma ED visits"
    resp = client.post("/api/compute", json=req)
    assert resp.status_code == 200
    assert resp.json()["results"][0]["attributableCases"]["mean"] > 0
