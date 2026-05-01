"""Tests for dataset-listing behavior in ``backend.routers.data``.

These tests stub DATA_ROOT to a tmp path so they're hermetic and cover
the contract the frontend relies on: ``countries_covered`` must be set
for WHO-AAP-style global datasets so the UI can filter dataset options
by study-area country.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from backend.routers import data as data_module


@pytest.fixture
def tmp_data_root(tmp_path, monkeypatch):
    monkeypatch.setattr(data_module, "DATA_ROOT", tmp_path)
    data_module._read_parquet.cache_clear()
    data_module._read_csv.cache_clear()
    return tmp_path


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow")


def test_who_aap_entry_includes_countries_covered(tmp_data_root: Path):
    # Two years, each with a different overlap of countries.
    _write_parquet(
        tmp_data_root / "who_aap" / "ne_countries" / "2017.parquet",
        pd.DataFrame({
            "admin_id": ["MEX", "USA", "CAN"],
            "mean_pm25": [15.0, 8.0, 7.0],
            "geometry": [None, None, None],
        }),
    )
    _write_parquet(
        tmp_data_root / "who_aap" / "ne_countries" / "2018.parquet",
        pd.DataFrame({
            "admin_id": ["USA", "FRA"],
            "mean_pm25": [8.0, 10.0],
            "geometry": [None, None],
        }),
    )

    datasets = data_module._scan_datasets()
    who = [d for d in datasets if d.get("id") == "who_aap_pm25_global"]
    assert len(who) == 1
    assert sorted(who[0]["countries_covered"]) == ["CAN", "FRA", "MEX", "USA"]


def test_who_aap_entry_includes_per_country_years(tmp_data_root: Path):
    # FRA only appears in 2018; MEX only in 2017. The aggregate `years`
    # is still the union, but `years_by_country` keys per ISO3 so the UI
    # can scope its picker to the country actually selected.
    _write_parquet(
        tmp_data_root / "who_aap" / "ne_countries" / "2017.parquet",
        pd.DataFrame({
            "admin_id": ["MEX", "USA", "CAN"],
            "mean_pm25": [15.0, 8.0, 7.0],
            "geometry": [None, None, None],
        }),
    )
    _write_parquet(
        tmp_data_root / "who_aap" / "ne_countries" / "2018.parquet",
        pd.DataFrame({
            "admin_id": ["USA", "FRA"],
            "mean_pm25": [8.0, 10.0],
            "geometry": [None, None],
        }),
    )

    datasets = data_module._scan_datasets()
    who = next(d for d in datasets if d.get("id") == "who_aap_pm25_global")
    assert who["years"] == [2017, 2018]
    ybc = who["years_by_country"]
    assert ybc["MEX"] == [2017]
    assert ybc["USA"] == [2017, 2018]
    assert ybc["CAN"] == [2017]
    assert ybc["FRA"] == [2018]


def test_epa_aqs_entry_includes_us_states_covered(tmp_data_root: Path):
    _write_parquet(
        tmp_data_root / "epa_aqs" / "pm25" / "ne_states" / "2020.parquet",
        pd.DataFrame({
            "admin_id": ["US-CA", "US-NY"],
            "mean_pm25": [10.0, 9.0],
            "geometry": [None, None],
        }),
    )

    datasets = data_module._scan_datasets()
    epa = [d for d in datasets if d.get("id") == "epa_aqs_pm25"]
    assert len(epa) == 1
    assert sorted(epa[0]["countries_covered"]) == ["US-CA", "US-NY"]


def test_epa_aqs_entry_includes_per_state_years(tmp_data_root: Path):
    # NY drops out in 2021; CA carries through. years_by_country lets
    # the UI honor that per-state coverage even though both years are
    # in the union ``years`` list.
    _write_parquet(
        tmp_data_root / "epa_aqs" / "pm25" / "ne_states" / "2020.parquet",
        pd.DataFrame({
            "admin_id": ["US-CA", "US-NY"],
            "mean_pm25": [10.0, 9.0],
            "geometry": [None, None],
        }),
    )
    _write_parquet(
        tmp_data_root / "epa_aqs" / "pm25" / "ne_states" / "2021.parquet",
        pd.DataFrame({
            "admin_id": ["US-CA"],
            "mean_pm25": [9.5],
            "geometry": [None],
        }),
    )

    datasets = data_module._scan_datasets()
    epa = next(d for d in datasets if d.get("id") == "epa_aqs_pm25")
    assert epa["years"] == [2020, 2021]
    ybc = epa["years_by_country"]
    assert ybc["US-CA"] == [2020, 2021]
    assert ybc["US-NY"] == [2020]


def test_direct_country_dataset_sets_countries_covered_from_path(
    tmp_data_root: Path,
):
    # Direct pollutant/country/year files already carry country in the
    # path; surface it in countries_covered too for UI uniformity.
    _write_parquet(
        tmp_data_root / "pm25" / "mexico" / "2019.parquet",
        pd.DataFrame({"admin_id": ["MX-01"], "mean_pm25": [16.0], "geometry": [None]}),
    )

    datasets = data_module._scan_datasets()
    direct = [
        d for d in datasets
        if d.get("type") == "concentration"
        and d.get("pollutant") == "pm25"
        and d.get("country") == "mexico"
    ]
    assert len(direct) == 1
    assert direct[0]["countries_covered"] == ["mexico"]


def test_direct_country_dataset_emits_years_by_country(tmp_data_root: Path):
    # Direct files already carry country in the path, so the per-country
    # year list is just {country: years} — but UIs read the same shape
    # across all dataset variants, so emit it for uniformity.
    _write_parquet(
        tmp_data_root / "pm25" / "mexico" / "2019.parquet",
        pd.DataFrame({"admin_id": ["MX-01"], "mean_pm25": [16.0], "geometry": [None]}),
    )
    _write_parquet(
        tmp_data_root / "pm25" / "mexico" / "2020.parquet",
        pd.DataFrame({"admin_id": ["MX-01"], "mean_pm25": [15.0], "geometry": [None]}),
    )

    datasets = data_module._scan_datasets()
    direct = next(
        d for d in datasets
        if d.get("type") == "concentration"
        and d.get("pollutant") == "pm25"
        and d.get("country") == "mexico"
    )
    assert direct["years"] == [2019, 2020]
    assert direct["years_by_country"] == {"mexico": [2019, 2020]}
