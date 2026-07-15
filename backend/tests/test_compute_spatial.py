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


def _setup_data_abbr(tmp_path: Path) -> None:
    """Like _setup_data, but the EPA AQS state file keys states by USPS
    abbreviation (US-CA) — the real production data format — rather than by
    FIPS (US-06). The state→tract concentration broadcast must still resolve."""
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
    aqs = pd.DataFrame({"admin_id": ["US-CA"], "mean_pm25": [11.4]})
    a = tmp_path / "processed" / "epa_aqs" / "pm25" / "ne_states"
    a.mkdir(parents=True)
    aqs.to_parquet(a / "2022.parquet")


def test_builtin_analytical_default_zero_iterations(tmp_path, monkeypatch):
    """monteCarloIterations=0 → analytical CIs (no Monte Carlo): point estimate
    from beta, bounds from betaLow/betaHigh, no random sampling."""
    _setup_data(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))
    client = TestClient(app)
    r = client.post("/api/compute/spatial", json={
        "mode": "builtin",
        "pollutant": "pm25", "country": "us", "year": 2022,
        "analysisLevel": "tract", "stateFilter": "06",
        "controlMode": "benchmark", "controlConcentration": 0.0,
        "selectedCRFs": [_ihd_crf()],
        "monteCarloIterations": 0,
    })
    assert r.status_code == 200, r.text
    agg = r.json()["causeRollups"][0]["attributableCases"]
    assert agg["mean"] > 0
    assert agg["lower95"] < agg["mean"] < agg["upper95"]


def test_builtin_tract_broadcasts_concentration_with_abbreviation_admin_id(tmp_path, monkeypatch):
    _setup_data_abbr(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))
    client = TestClient(app)
    r = client.post("/api/compute/spatial", json={
        "mode": "builtin",
        "pollutant": "pm25", "country": "us", "year": 2022,
        "analysisLevel": "tract", "stateFilter": "06",
        "controlMode": "benchmark", "controlConcentration": 0.0,
        "selectedCRFs": [_ihd_crf()],
        "monteCarloIterations": 200,
    })
    assert r.status_code == 200, r.text
    body = r.json()
    # The state mean (11.4) must broadcast to every tract, not resolve to NaN.
    assert body["zones"][0]["baselineConcentration"] == pytest.approx(11.4)
    assert body["causeRollups"][0]["attributableCases"]["mean"] > 0


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


def test_builtin_urban_routes_to_urban_resolver(monkeypatch):
    """analysisLevel='urban' must call prepare_urban_centre_inputs with cityIds,
    dispatched via the name it's bound to in backend.routers.compute."""
    calls = {}

    def fake_urban(pollutant, country, year, control_mode,
                    city_ids=None, control_value=None, rollback_percent=None):
        calls.update(dict(pollutant=pollutant, country=country, year=year,
                           city_ids=city_ids))
        from backend.services.resolver import ResolvedInputs, Provenance
        return ResolvedInputs(
            zone_ids=["101"], zone_names=["Ciudad Uno"], parent_ids=["MEX"],
            geometries=[None],
            c_baseline=np.array([22.0]), c_control=np.array([0.0]),
            population=np.array([1_000_000.0]),
            provenance=Provenance(
                concentration={"grain": "urban_centre"},
                population={"grain": "urban_centre"},
                incidence={"grain": "crf_default"},
            ),
        )

    import backend.routers.compute as compute_mod
    monkeypatch.setattr(compute_mod, "prepare_urban_centre_inputs", fake_urban)

    client = TestClient(app)
    r = client.post("/api/compute/spatial", json={
        "mode": "builtin",
        "pollutant": "pm25", "country": "MEX", "year": 2020,
        "analysisLevel": "urban", "cityIds": ["101"],
        "controlMode": "benchmark", "controlConcentration": 0.0,
        "selectedCRFs": [_acm_crf()],
        "monteCarloIterations": 0,
    })
    assert r.status_code == 200, r.text
    assert calls["city_ids"] == ["101"]
    assert calls["country"] == "MEX"
    body = r.json()
    assert len(body["zones"]) == 1
    assert body["zones"][0]["zoneName"] == "Ciudad Uno"
