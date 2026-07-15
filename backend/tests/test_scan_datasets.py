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


def _gadm_frame(iso3s: list[str]) -> pd.DataFrame:
    # Minimal shape of the GEE-exported admin-2 parquet: feature_id is the
    # GADM GID_2, country_iso3 is what coverage scanning keys on.
    return pd.DataFrame({
        "feature_id": [f"{iso3}.1.1_1" for iso3 in iso3s],
        "name": [f"District {iso3}" for iso3 in iso3s],
        "country_iso3": iso3s,
        "pm25_popweighted": [12.0] * len(iso3s),
        "pop_total": [50_000.0] * len(iso3s),
    })


def test_gadm_adm2_entry_lists_iso3_coverage_and_years(tmp_data_root: Path):
    # CAN drops out in 2016; MEX carries through. Coverage and per-country
    # years come from the country_iso3 column (the parquet has no admin_id).
    _write_parquet(
        tmp_data_root / "who_aap" / "gadm_adm2" / "2015.parquet",
        _gadm_frame(["MEX", "CAN"]),
    )
    _write_parquet(
        tmp_data_root / "who_aap" / "gadm_adm2" / "2016.parquet",
        _gadm_frame(["MEX"]),
    )

    datasets = data_module._scan_datasets()
    gadm = [d for d in datasets if d.get("id") == "gadm_adm2_pm25_global"]
    assert len(gadm) == 1
    entry = gadm[0]
    assert entry["type"] == "concentration"
    assert entry["pollutant"] == "pm25"
    assert entry["aggregation"] == "adm2"
    assert sorted(entry["countries_covered"]) == ["CAN", "MEX"]
    assert entry["years"] == [2015, 2016]
    assert entry["years_by_country"] == {"CAN": [2015], "MEX": [2015, 2016]}


def test_gadm_adm2_entry_respects_pollutant_filter(tmp_data_root: Path):
    _write_parquet(
        tmp_data_root / "who_aap" / "gadm_adm2" / "2015.parquet",
        _gadm_frame(["MEX"]),
    )

    pm25 = data_module._scan_datasets(
        type_filter="concentration", pollutant_filter="pm25"
    )
    assert any(d.get("id") == "gadm_adm2_pm25_global" for d in pm25)

    no2 = data_module._scan_datasets(
        type_filter="concentration", pollutant_filter="no2"
    )
    assert not any(d.get("id") == "gadm_adm2_pm25_global" for d in no2)


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


# ── Filter pushdown ────────────────────────────────────────────────
#
# ``_scan_datasets`` accepts ``type_filter`` / ``pollutant_filter`` so the
# concentration-only path (Step 2's built-in loader) skips the expensive
# GBD-incidence enumeration instead of building ~7k entries it throws away.
# These tests pin the contract: a filtered scan must equal the full scan
# post-filtered (no rows gained or lost), and must not touch other blocks.


@pytest.fixture
def mixed_data_root(tmp_data_root: Path) -> Path:
    """A DATA_ROOT spanning every dataset type so filter pushdown has
    something to exclude."""
    _write_parquet(
        tmp_data_root / "epa_aqs" / "pm25" / "ne_states" / "2020.parquet",
        pd.DataFrame({"admin_id": ["US-CA"], "mean_pm25": [10.0], "geometry": [None]}),
    )
    _write_parquet(
        tmp_data_root / "epa_aqs" / "no2" / "ne_states" / "2020.parquet",
        pd.DataFrame({"admin_id": ["US-CA"], "mean_no2": [20.0], "geometry": [None]}),
    )
    _write_parquet(
        tmp_data_root / "demographics" / "us" / "2022.parquet",
        pd.DataFrame({"geoid": ["06001"], "total_pop": [1000], "geometry": [None]}),
    )
    _write_parquet(
        tmp_data_root / "incidence" / "gbd_rates.parquet",
        pd.DataFrame({
            "cause": ["ihd", "ihd", "stroke"],
            "location_name": ["Mexico", "United States of America", "Mexico"],
            "year": [2019, 2019, 2019],
            "rate": [1.0, 2.0, 3.0],
        }),
    )
    return tmp_data_root


def test_concentration_filter_matches_full_scan(mixed_data_root: Path):
    full = data_module._scan_datasets()
    expected = [d for d in full if d.get("type") == "concentration"]
    filtered = data_module._scan_datasets(type_filter="concentration")
    assert sorted(map(str, filtered)) == sorted(map(str, expected))
    # And it must not have produced any incidence entries at all.
    assert all(d.get("type") == "concentration" for d in filtered)


def test_pollutant_filter_matches_full_scan(mixed_data_root: Path):
    full = data_module._scan_datasets()
    expected = [
        d for d in full
        if d.get("type") == "concentration" and d.get("pollutant") == "pm25"
    ]
    filtered = data_module._scan_datasets(
        type_filter="concentration", pollutant_filter="pm25"
    )
    assert sorted(map(str, filtered)) == sorted(map(str, expected))
    assert all(d.get("pollutant") == "pm25" for d in filtered)


def test_incidence_filter_matches_full_scan(mixed_data_root: Path):
    full = data_module._scan_datasets()
    expected = [d for d in full if d.get("type") == "incidence"]
    filtered = data_module._scan_datasets(type_filter="incidence")
    assert sorted(map(str, filtered)) == sorted(map(str, expected))


def test_population_filter_includes_acs_derived(mixed_data_root: Path):
    # The ACS-derived population entry lives in the demographics block;
    # a population filter must still surface it.
    full = data_module._scan_datasets()
    expected = [d for d in full if d.get("type") == "population"]
    filtered = data_module._scan_datasets(type_filter="population")
    assert sorted(map(str, filtered)) == sorted(map(str, expected))
    assert any(d.get("id") == "acs_population_us" for d in filtered)


# ── GHS-UCDB urban centres ─────────────────────────────────────────
#
# The urban-centre stats parquet (Task 1's GEE export) carries no country
# column, so coverage keys on country_iso3 in the (small) boundary
# GeoPackage instead. Every year file carries every centre, so
# years_by_country is uniform across countries.


def _write_ghs_ucdb_gpkg(tmp_data_root: Path) -> None:
    import geopandas as gpd
    from shapely.geometry import box

    bdir = tmp_data_root / "boundaries"
    bdir.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame(
        {
            "feature_id": ["101", "201"],
            "name": ["Ciudad Uno", "Lagos"],
            "country_iso3": ["MEX", "NGA"],
        },
        geometry=[box(0, 0, 1, 1), box(5, 5, 6, 6)],
        crs="EPSG:4326",
    ).to_file(bdir / "ghs_ucdb_r2024a.gpkg", driver="GPKG")


def test_scan_emits_ghs_smod_urban_dataset(tmp_data_root: Path):
    _write_ghs_ucdb_gpkg(tmp_data_root)

    sdir = tmp_data_root / "ghs_smod_gee" / "ghs_smod"
    sdir.mkdir(parents=True)
    for y in (2019, 2020):
        _write_parquet(
            sdir / f"{y}.parquet",
            pd.DataFrame({"feature_id": ["101", "201"], "pop_total": [1.0, 2.0]}),
        )

    datasets = data_module._scan_datasets(
        type_filter="concentration", pollutant_filter="pm25"
    )
    ids = {d.get("id") for d in datasets}
    assert "ghs_smod_pm25_global" in ids
    entry = next(d for d in datasets if d.get("id") == "ghs_smod_pm25_global")
    assert entry["type"] == "concentration"
    assert entry["pollutant"] == "pm25"
    assert entry["aggregation"] == "urban"
    assert entry["countries_covered"] == ["MEX", "NGA"]
    assert entry["years"] == [2019, 2020]
    assert entry["years_by_country"]["MEX"] == [2019, 2020]
    assert entry["years_by_country"]["NGA"] == [2019, 2020]


def test_ghs_smod_entry_respects_pollutant_filter(tmp_data_root: Path):
    _write_ghs_ucdb_gpkg(tmp_data_root)

    sdir = tmp_data_root / "ghs_smod_gee" / "ghs_smod"
    sdir.mkdir(parents=True)
    _write_parquet(
        sdir / "2020.parquet",
        pd.DataFrame({"feature_id": ["101", "201"], "pop_total": [1.0, 2.0]}),
    )

    pm25 = data_module._scan_datasets(
        type_filter="concentration", pollutant_filter="pm25"
    )
    assert any(d.get("id") == "ghs_smod_pm25_global" for d in pm25)

    no2 = data_module._scan_datasets(
        type_filter="concentration", pollutant_filter="no2"
    )
    assert not any(d.get("id") == "ghs_smod_pm25_global" for d in no2)
