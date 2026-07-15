"""Tests for GET /api/data/urban-centres/{country}."""
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from fastapi.testclient import TestClient
from shapely.geometry import box

from backend.main import app
from backend.routers import data as data_router


@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture()
def urban_data_root(tmp_path, monkeypatch):
    bdir = tmp_path / "boundaries"
    bdir.mkdir(parents=True)
    gpd.GeoDataFrame(
        {"feature_id": ["101", "102", "201"],
         "name": ["Ciudad Uno", "Ciudad Dos", "Lagos"],
         "country_iso3": ["MEX", "MEX", "NGA"]},
        geometry=[box(0, 0, 1, 1), box(2, 0, 3, 1), box(5, 5, 6, 6)],
        crs="EPSG:4326",
    ).to_file(bdir / "ghs_ucdb_r2024a.gpkg", driver="GPKG")

    sdir = tmp_path / "ghs_smod_gee" / "ghs_smod"
    sdir.mkdir(parents=True)
    for y, pops in ((2019, [900_000.0, 400_000.0, 11e6]),
                    (2020, [1_000_000.0, 500_000.0, 12e6])):
        pd.DataFrame({"feature_id": ["101", "102", "201"],
                      "pop_total": pops}).to_parquet(sdir / f"{y}.parquet")

    monkeypatch.setattr(data_router, "DATA_ROOT", tmp_path)
    return tmp_path


def test_lists_centres_sorted_by_population(client, urban_data_root):
    resp = client.get("/api/data/urban-centres/MEX?year=2020")
    assert resp.status_code == 200
    body = resp.json()
    assert body["country"] == "MEX"
    assert body["year"] == 2020
    assert [c["id"] for c in body["centres"]] == ["101", "102"]
    assert body["centres"][0] == {
        "id": "101", "name": "Ciudad Uno", "population": 1_000_000.0,
    }


def test_defaults_to_latest_year(client, urban_data_root):
    resp = client.get("/api/data/urban-centres/MEX")
    assert resp.status_code == 200
    assert resp.json()["year"] == 2020


def test_unknown_country_404(client, urban_data_root):
    assert client.get("/api/data/urban-centres/FRA").status_code == 404


def test_slug_normalization(client, urban_data_root):
    resp = client.get("/api/data/urban-centres/mexico")
    assert resp.status_code == 200
    assert resp.json()["country"] == "MEX"
