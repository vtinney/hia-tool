"""Tests for the GHS-UCDB urban-centre resolver path."""
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import box

import backend.services.resolver as resolver
from backend.services.resolver import prepare_urban_centre_inputs


def _fake_urban_data(tmp_path: Path) -> None:
    """Three centres: two Mexican, one Nigerian."""
    gdf = gpd.GeoDataFrame(
        {
            "feature_id": ["101", "102", "201"],
            "name": ["Ciudad Uno", "Ciudad Dos", "Lagos"],
            "country_iso3": ["MEX", "MEX", "NGA"],
        },
        geometry=[box(0, 0, 1, 1), box(2, 0, 3, 1), box(5, 5, 6, 6)],
        crs="EPSG:4326",
    )
    bdir = tmp_path / "processed" / "boundaries"
    bdir.mkdir(parents=True)
    gdf.to_file(bdir / "ghs_ucdb_r2024a.gpkg", driver="GPKG")

    stats = pd.DataFrame({
        "feature_id": ["101", "102", "201"],
        "name": ["Ciudad Uno", "Ciudad Dos", "Lagos"],
        "year": [2020] * 3,
        "pop_source_year": [2020] * 3,
        "pop_total": [1_000_000.0, 500_000.0, 12_000_000.0],
        "pm25_mean": [21.0, 16.0, 38.0],
        "pm25_popweighted": [22.0, 17.0, 37.5],
    })
    sdir = tmp_path / "processed" / "ghs_smod_gee" / "ghs_smod"
    sdir.mkdir(parents=True)
    stats.to_parquet(sdir / "2020.parquet")


@pytest.fixture()
def urban_env(tmp_path, monkeypatch):
    _fake_urban_data(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))
    # The boundary GeoDataFrame is cached at module level; reset per test.
    monkeypatch.setattr(resolver, "_ghs_ucdb_gdf", None)
    return tmp_path


def test_country_filter_returns_only_that_countrys_centres(urban_env):
    res = prepare_urban_centre_inputs(
        pollutant="pm25", country="MEX", year=2020, control_mode="scalar",
        control_value=5.0,
    )
    assert res.zone_ids == ["101", "102"]
    assert res.zone_names == ["Ciudad Uno", "Ciudad Dos"]
    assert res.parent_ids == ["MEX", "MEX"]
    np.testing.assert_allclose(res.c_baseline, [22.0, 17.0])
    np.testing.assert_allclose(res.c_control, [5.0, 5.0])
    np.testing.assert_allclose(res.population, [1_000_000.0, 500_000.0])
    assert res.provenance.concentration["grain"] == "urban_centre"


def test_city_ids_filter_selects_subset(urban_env):
    res = prepare_urban_centre_inputs(
        pollutant="pm25", country="MEX", year=2020, control_mode="scalar",
        control_value=0.0, city_ids=["102"],
    )
    assert res.zone_ids == ["102"]
    np.testing.assert_allclose(res.population, [500_000.0])


def test_unknown_city_ids_raise_404_style(urban_env):
    with pytest.raises(FileNotFoundError):
        prepare_urban_centre_inputs(
            pollutant="pm25", country="MEX", year=2020,
            control_mode="scalar", control_value=0.0, city_ids=["999"],
        )


def test_non_pm25_rejected(urban_env):
    with pytest.raises(FileNotFoundError):
        prepare_urban_centre_inputs(
            pollutant="no2", country="MEX", year=2020,
            control_mode="scalar", control_value=0.0,
        )


def test_unknown_country_raises(urban_env):
    with pytest.raises(FileNotFoundError):
        prepare_urban_centre_inputs(
            pollutant="pm25", country="FRA", year=2020,
            control_mode="scalar", control_value=0.0,
        )


def test_global_not_supported(urban_env):
    with pytest.raises(FileNotFoundError):
        prepare_urban_centre_inputs(
            pollutant="pm25", country="global", year=2020,
            control_mode="scalar", control_value=0.0,
        )
