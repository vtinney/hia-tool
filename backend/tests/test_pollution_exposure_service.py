"""Tests for backend.services.pollution_exposure."""

from pathlib import Path

import pandas as pd
import pytest

from backend.services import pollution_exposure


@pytest.fixture
def patched_parquets(tmp_path: Path, monkeypatch):
    # Pollution parquet with 4 rows: pm25/ozone × China country/Bahia state
    rows = [
        ("pm25", 6, "CHN", "China", 3, "CHN", "CHN", None, 2019,
         41.0, 22.0, 71.0, "ug_m3", "gbd_2023"),
        ("pm25", 4770, None, "Bahia", 4, "BRA", "BRA", "BRA-1", 2019,
         12.0, 8.0, 18.0, "ug_m3", "gbd_2023"),
        ("ozone", 6, None, "China", 3, "CHN", "CHN", None, 2021,
         53.0, 52.6, 53.3, "ppb", "gbd_2021"),
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

    # GHS join parquet: city 1001 → China country, city 1002 → Bahia state
    ghs = pd.DataFrame([
        {"ghs_uid": 1001, "ne_country_uid": "CHN", "ne_state_uid": None},
        {"ghs_uid": 1002, "ne_country_uid": "BRA", "ne_state_uid": "BRA-1"},
    ])
    ghs_parquet = tmp_path / "ghs_smod_to_ne.parquet"
    ghs.to_parquet(ghs_parquet, index=False)

    # Raster catalog
    cat = pd.DataFrame([
        {"year": 2019, "relative_path": "pm25_gbd2023/2019.tif",
         "crs": "EPSG:4326", "pixel_size_deg": 0.1, "nodata": -9999.0,
         "xmin": -180.0, "ymin": -90.0, "xmax": 180.0, "ymax": 90.0,
         "unit": "ug_m3", "source": "IHME GBD 2023"},
    ])
    cat["year"] = cat["year"].astype("int16")
    cat_dir = tmp_path / "pm25_gbd2023"
    cat_dir.mkdir()
    cat.to_parquet(cat_dir / "catalog.parquet", index=False)
    # Create a dummy tif file at the relative path so get_default_raster_path
    # can return an existing path.
    (cat_dir / "2019.tif").write_bytes(b"")

    monkeypatch.setattr(pollution_exposure, "_POLLUTION_PARQUET", pollution_parquet)
    monkeypatch.setattr(pollution_exposure, "_GHS_PARQUET", ghs_parquet)
    monkeypatch.setattr(pollution_exposure, "_RASTER_CATALOG", cat_dir / "catalog.parquet")
    monkeypatch.setattr(pollution_exposure, "_POLLUTION_ROOT", tmp_path)
    pollution_exposure._clear_cache()
    return tmp_path


def test_country_lookup(patched_parquets):
    result = pollution_exposure.get_default_concentration(
        "pm25", 2019, ne_country_uid="CHN",
    )
    assert result is not None
    assert abs(result["mean"] - 41.0) < 1e-6
    assert result["unit"] == "ug_m3"
    assert result["year_used"] == 2019


def test_state_lookup_preferred_over_country(patched_parquets):
    result = pollution_exposure.get_default_concentration(
        "pm25", 2019, ne_country_uid="BRA", ne_state_uid="BRA-1",
    )
    assert result is not None
    assert abs(result["mean"] - 12.0) < 1e-6


def test_ghs_resolves_via_spatial_join(patched_parquets):
    result = pollution_exposure.get_default_concentration(
        "pm25", 2019, ghs_uid=1002,
    )
    assert result is not None
    # City 1002 is in Bahia → should return state-level value, not
    # the country fallback
    assert abs(result["mean"] - 12.0) < 1e-6


def test_ozone_fallback_to_2021_when_asked_for_2022(patched_parquets):
    result = pollution_exposure.get_default_concentration(
        "ozone", 2022, ne_country_uid="CHN",
    )
    assert result is not None
    assert result["year_used"] == 2021
    assert abs(result["mean"] - 53.0) < 1e-6


def test_unresolved_location_returns_none(patched_parquets):
    result = pollution_exposure.get_default_concentration(
        "pm25", 2019, ne_country_uid="ZZZ",
    )
    assert result is None


def test_get_default_raster_path_pm25(patched_parquets):
    path = pollution_exposure.get_default_raster_path("pm25", 2019)
    assert path is not None
    assert path.exists()


def test_get_default_raster_path_no2_returns_none(patched_parquets):
    assert pollution_exposure.get_default_raster_path("no2", 2019) is None
