"""End-to-end tests for /api/compute/spatial with the new modes."""
import numpy as np
import pandas as pd
import pytest
from pathlib import Path
from fastapi.testclient import TestClient

from backend.main import app


def _setup_data(tmp_path: Path) -> None:
    # ACS tracts for CA
    df = pd.DataFrame({
        "geoid":       ["06001000100", "06001000200", "06003000100"],
        "state_fips":  ["06", "06", "06"],
        "county_fips": ["001", "001", "003"],
        "total_pop":   [3500, 4200, 500],
        "geometry":    ["POLYGON((0 0,1 0,1 1,0 1,0 0))"] * 3,
    })
    p = tmp_path / "processed" / "demographics" / "us"
    p.mkdir(parents=True)
    df.to_parquet(p / "2022.parquet")

    # EPA AQS state-level PM2.5
    aqs = pd.DataFrame({"admin_id": ["US-06"], "mean_pm25": [11.4]})
    a = tmp_path / "processed" / "epa_aqs" / "pm25" / "ne_states"
    a.mkdir(parents=True)
    aqs.to_parquet(a / "2022.parquet")


def _ihd_crf() -> dict:
    return {
        "id": "epa_pm25_ihd_adult", "source": "Pope 2004",
        "endpoint": "Ischemic heart disease",
        "beta": 0.015, "betaLow": 0.01, "betaHigh": 0.02,
        "functionalForm": "log-linear", "defaultRate": 0.0025,
        "cause": "ihd", "endpointType": "mortality",
    }


def _acm_crf() -> dict:
    return {
        "id": "epa_pm25_acm_adult", "source": "Turner 2016",
        "endpoint": "All-cause mortality",
        "beta": 0.00583, "betaLow": 0.00396, "betaHigh": 0.00769,
        "functionalForm": "log-linear", "defaultRate": 0.008,
        "cause": "all_cause", "endpointType": "mortality",
    }


def test_builtin_mode_returns_per_tract_results(tmp_path, monkeypatch):
    _setup_data(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))
    client = TestClient(app)
    r = client.post("/api/compute/spatial", json={
        "mode": "builtin",
        "pollutant": "pm25", "country": "us", "year": 2022,
        "analysisLevel": "tract", "stateFilter": "06",
        "controlMode": "benchmark", "controlConcentration": 5.0,
        "selectedCRFs": [_ihd_crf()],
        "monteCarloIterations": 200,
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["zones"]) == 3
    assert body["provenance"]["concentration"]["grain"] == "state"
    assert "broadcast_to" in body["provenance"]["concentration"]
    assert len(body["causeRollups"]) == 1
    assert body["causeRollups"][0]["cause"] == "ihd"
    assert body["allCauseDeaths"] is None
    assert any("broadcast" in w.lower() for w in body["warnings"])


def test_builtin_mode_splits_all_cause_and_cause_specific(tmp_path, monkeypatch):
    _setup_data(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))
    client = TestClient(app)
    r = client.post("/api/compute/spatial", json={
        "mode": "builtin",
        "pollutant": "pm25", "country": "us", "year": 2022,
        "analysisLevel": "state", "stateFilter": "06",
        "controlMode": "benchmark", "controlConcentration": 5.0,
        "selectedCRFs": [_ihd_crf(), _acm_crf()],
        "monteCarloIterations": 200,
    })
    assert r.status_code == 200
    body = r.json()
    assert body["allCauseDeaths"] is not None
    assert body["totalDeaths"]["mean"] > 0  # cause-specific (IHD only)
    assert body["allCauseDeaths"]["mean"] > 0
    assert body["totalDeaths"]["mean"] != body["allCauseDeaths"]["mean"]
