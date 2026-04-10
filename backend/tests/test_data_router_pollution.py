"""Tests for the new /api/data/pollution endpoints."""

from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from backend.main import app
from backend.services import pollution_exposure


@pytest.fixture
def patched_parquets(tmp_path: Path, monkeypatch):
    rows = [
        ("pm25", 6, "CHN", "China", 3, "CHN", "CHN", None, 2019,
         41.0, 22.0, 71.0, "ug_m3", "gbd_2023"),
        ("no2", 6, "CHN", "China", 3, "CHN", "CHN", None, 2019,
         16.2, 7.4, 28.1, "ppb", "gbd_2023"),
    ]
    cols = [
        "pollutant", "gbd_location_id", "ihme_loc_id", "location_name",
        "location_level", "ne_country_iso3", "ne_country_uid",
        "ne_state_uid", "year", "mean", "lower", "upper", "unit", "release",
    ]
    df = pd.DataFrame(rows, columns=cols)
    pollution_parquet = tmp_path / "gbd_pollution.parquet"
    df.to_parquet(pollution_parquet, index=False)

    cat = pd.DataFrame([{
        "year": 2019, "relative_path": "pm25_gbd2023/2019.tif",
        "crs": "EPSG:4326", "pixel_size_deg": 0.1, "nodata": -9999.0,
        "xmin": -180.0, "ymin": -90.0, "xmax": 180.0, "ymax": 90.0,
        "unit": "ug_m3", "source": "IHME GBD 2023",
    }])
    cat["year"] = cat["year"].astype("int16")
    cat_dir = tmp_path / "pm25_gbd2023"
    cat_dir.mkdir()
    cat.to_parquet(cat_dir / "catalog.parquet", index=False)
    (cat_dir / "2019.tif").write_bytes(b"")

    monkeypatch.setattr(pollution_exposure, "_POLLUTION_PARQUET", pollution_parquet)
    monkeypatch.setattr(pollution_exposure, "_RASTER_CATALOG", cat_dir / "catalog.parquet")
    monkeypatch.setattr(pollution_exposure, "_POLLUTION_ROOT", tmp_path)
    pollution_exposure._clear_cache()
    return tmp_path


def test_default_endpoint_returns_pm25_for_china(patched_parquets):
    client = TestClient(app)
    resp = client.get(
        "/api/data/pollution/default",
        params={"pollutant": "pm25", "year": 2019, "ne_country_uid": "CHN"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["pollutant"] == "pm25"
    assert abs(data["mean"] - 41.0) < 1e-6
    assert data["year_used"] == 2019


def test_default_endpoint_404_on_unknown_country(patched_parquets):
    client = TestClient(app)
    resp = client.get(
        "/api/data/pollution/default",
        params={"pollutant": "pm25", "year": 2019, "ne_country_uid": "ZZZ"},
    )
    assert resp.status_code == 404


def test_raster_catalog_endpoint_pm25(patched_parquets):
    client = TestClient(app)
    resp = client.get("/api/data/pollution/raster-catalog",
                      params={"pollutant": "pm25"})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["year"] == 2019
    assert data[0]["unit"] == "ug_m3"


def test_raster_catalog_endpoint_no2_empty(patched_parquets):
    client = TestClient(app)
    resp = client.get("/api/data/pollution/raster-catalog",
                      params={"pollutant": "no2"})
    assert resp.status_code == 200
    assert resp.json() == []
