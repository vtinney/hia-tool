"""Tests for the defaultConcentrationLayer resolution in the spatial endpoint."""

from pathlib import Path

import pandas as pd
import pytest

from backend.services import pollution_exposure
from backend.routers.compute import _resolve_default_concentration_layer


@pytest.fixture
def patched_catalog(tmp_path: Path, monkeypatch):
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

    monkeypatch.setattr(pollution_exposure, "_RASTER_CATALOG", cat_dir / "catalog.parquet")
    monkeypatch.setattr(pollution_exposure, "_POLLUTION_ROOT", tmp_path)
    pollution_exposure._clear_cache()
    return tmp_path


def test_resolve_valid_layer(patched_catalog):
    path = _resolve_default_concentration_layer("pm25_gbd2023_2019")
    assert path is not None
    assert path.exists()


def test_resolve_unknown_year(patched_catalog):
    path = _resolve_default_concentration_layer("pm25_gbd2023_2099")
    assert path is None


def test_resolve_unknown_pollutant(patched_catalog):
    path = _resolve_default_concentration_layer("no2_gbd2023_2019")
    assert path is None


def test_resolve_garbage_string(patched_catalog):
    assert _resolve_default_concentration_layer("nonsense") is None
    assert _resolve_default_concentration_layer("") is None
