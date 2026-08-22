"""Tests for /api/data/incidence `aggregate` param.

The primary per-country incidence files can be admin-unit × age-group
tables (e.g. the CDC Wonder county files) — useless to a caller that
needs one study-area scalar rate. `aggregate=true` skips the primary
file and serves the national GBD all-ages fallback directly.
"""
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from backend.main import app
from backend.routers import data as data_router


@pytest.fixture
def incidence_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "processed"

    # Primary per-country file: county × age-group rows, no all_ages.
    county = pd.DataFrame({
        "admin_id": ["01001", "01001", "48201"],
        "admin_name": ["Autauga County, AL", "Autauga County, AL", "Harris County, TX"],
        "incidence_rate": [0.0024, 0.0001, 0.0018],
        "age_group": ["65_74", "25_34", "65_74"],
    })
    p = root / "incidence" / "us" / "ihd"
    p.mkdir(parents=True)
    county.to_parquet(p / "2020.parquet")

    # GBD fallback with the national all-ages row.
    gbd = pd.DataFrame({
        "cause": ["ihd", "ihd"],
        "gbd_location_id": [102, 102],
        "location_name": ["United States of America"] * 2,
        "year": [2020, 2020],
        "age_group": ["all_ages", "all_ages"],
        "measure": ["deaths", "incidence"],
        "sex": ["both", "both"],
        "rate": [0.001437, 0.004],
        "rate_lower": [0.00142, 0.0039],
        "rate_upper": [0.00145, 0.0041],
        "ne_country_iso3": ["USA", "USA"],
        "ne_country_uid": ["USA", "USA"],
        "ne_state_uid": [None, None],
    })
    gbd.to_parquet(root / "incidence" / "gbd_rates.parquet")

    monkeypatch.setattr(data_router, "DATA_ROOT", root)
    data_router._read_parquet.cache_clear()
    data_router._gbd_location_names.cache_clear()
    return root


client = TestClient(app)


def test_default_serves_primary_county_file(incidence_data):
    body = client.get("/api/data/incidence/us/ihd/2020").json()
    assert len(body["units"]) == 3
    assert body["units"][0]["admin_id"] == "01001"
    assert "source" not in body


def test_aggregate_skips_primary_and_serves_gbd_national(incidence_data):
    body = client.get("/api/data/incidence/us/ihd/2020?aggregate=true").json()
    assert body["source"] == "gbd_rates"
    assert len(body["units"]) == 1
    u = body["units"][0]
    assert u["age_group"] == "all_ages"
    assert u["measure"] == "deaths"  # deaths preferred by default
    assert u["incidence_rate"] == pytest.approx(0.001437)


def test_aggregate_honors_measure_param(incidence_data):
    body = client.get(
        "/api/data/incidence/us/ihd/2020?aggregate=true&measure=incidence"
    ).json()
    assert body["units"][0]["incidence_rate"] == pytest.approx(0.004)


def test_aggregate_404_when_gbd_lacks_cause(incidence_data):
    resp = client.get("/api/data/incidence/us/copd/2020?aggregate=true")
    assert resp.status_code == 404
