"""Tests for the built-in data resolver."""
import numpy as np

from backend.services.resolver import Provenance, ResolvedInputs


def test_provenance_dataclass_shape():
    p = Provenance(
        concentration={"grain": "state", "source": "epa_aqs"},
        population={"grain": "tract", "source": "acs"},
        incidence={"grain": "national", "source": "crf_default"},
    )
    assert p.concentration["grain"] == "state"
    assert p.population["source"] == "acs"
    assert p.incidence["grain"] == "national"


def test_resolved_inputs_dataclass_shape():
    import numpy as np
    r = ResolvedInputs(
        zone_ids=["06001", "06003"],
        zone_names=["Alameda County", "Alpine County"],
        parent_ids=["06", "06"],
        geometries=[{"type": "Polygon", "coordinates": []}] * 2,
        c_baseline=np.array([12.5, 8.0]),
        c_control=np.array([5.0, 5.0]),
        population=np.array([1_600_000, 1_100]),
        provenance=Provenance(
            concentration={"grain": "state", "source": "epa_aqs"},
            population={"grain": "tract", "source": "acs"},
            incidence={"grain": "national", "source": "crf_default"},
        ),
        warnings=[],
    )
    assert len(r.zone_ids) == 2
    assert r.population.sum() == 1_601_100


from pathlib import Path
import pandas as pd
import pytest
from backend.services.resolver import load_reporting_polygons


def _fake_acs_parquet(tmp_path: Path) -> Path:
    df = pd.DataFrame({
        "geoid":        ["06001000100", "06001000200", "06003000100", "36061000100"],
        "state_fips":   ["06", "06", "06", "36"],
        "county_fips":  ["001", "001", "003", "061"],
        "total_pop":    [3500, 4200, 500, 2800],
        "geometry":     ["POLYGON((0 0,1 0,1 1,0 1,0 0))"] * 4,
    })
    processed = tmp_path / "processed" / "demographics" / "us"
    processed.mkdir(parents=True)
    path = processed / "2022.parquet"
    df.to_parquet(path)
    return path


def test_load_reporting_polygons_tract_state_filter(tmp_path, monkeypatch):
    _fake_acs_parquet(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))
    polys = load_reporting_polygons(
        country="us", year=2022, analysis_level="tract", state_filter="06",
    )
    assert polys["zone_ids"] == ["06001000100", "06001000200", "06003000100"]
    assert polys["parent_ids"] == ["001", "001", "003"]  # county_fips
    assert list(polys["population"]) == [3500, 4200, 500]


def test_load_reporting_polygons_county_aggregates_tracts(tmp_path, monkeypatch):
    _fake_acs_parquet(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))
    polys = load_reporting_polygons(
        country="us", year=2022, analysis_level="county", state_filter="06",
    )
    assert polys["zone_ids"] == ["06001", "06003"]
    assert polys["parent_ids"] == ["06", "06"]  # state_fips
    assert list(polys["population"]) == [7700, 500]   # 3500+4200, 500


def test_load_reporting_polygons_state_aggregates_everything(tmp_path, monkeypatch):
    _fake_acs_parquet(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))
    polys = load_reporting_polygons(
        country="us", year=2022, analysis_level="state",
    )
    assert sorted(polys["zone_ids"]) == ["06", "36"]
    state_06_idx = polys["zone_ids"].index("06")
    assert polys["population"][state_06_idx] == 8200  # 3500+4200+500


from backend.services.resolver import resolve_concentration


def _fake_epa_aqs_state(tmp_path: Path) -> Path:
    df = pd.DataFrame({
        "admin_id": ["US-06", "US-36"],
        "mean_pm25": [11.4, 9.2],
    })
    p = tmp_path / "processed" / "epa_aqs" / "pm25" / "ne_states"
    p.mkdir(parents=True)
    path = p / "2022.parquet"
    df.to_parquet(path)
    return path


def test_resolve_concentration_state_broadcasts_to_tracts(tmp_path, monkeypatch):
    _fake_acs_parquet(tmp_path)
    _fake_epa_aqs_state(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))
    polys = load_reporting_polygons("us", 2022, "tract", state_filter="06")
    c, prov, warnings = resolve_concentration(
        pollutant="pm25", country="us", year=2022,
        analysis_level="tract", polygons=polys,
    )
    # All CA tracts get the same CA state value
    assert len(c) == 3
    assert (c == 11.4).all()
    assert prov["grain"] == "state"
    assert prov["source"] == "epa_aqs"
    assert prov["broadcast_to"] == "tract"
    assert any("broadcast" in w.lower() for w in warnings)


def test_resolve_concentration_state_level_direct_use(tmp_path, monkeypatch):
    _fake_acs_parquet(tmp_path)
    _fake_epa_aqs_state(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))
    polys = load_reporting_polygons("us", 2022, "state")
    c, prov, warnings = resolve_concentration(
        pollutant="pm25", country="us", year=2022,
        analysis_level="state", polygons=polys,
    )
    assert len(c) == 2
    # State 06 → 11.4, state 36 → 9.2
    idx_06 = polys["zone_ids"].index("06")
    assert c[idx_06] == 11.4
    assert prov.get("broadcast_to") is None
    assert warnings == []


from backend.services.resolver import resolve_control


def test_resolve_control_scalar():
    c_base = np.array([10.0, 20.0, 30.0])
    c_ctrl = resolve_control(
        c_base=c_base, control_mode="scalar", control_value=5.0,
    )
    assert (c_ctrl == 5.0).all()


def test_resolve_control_rollback_percent():
    c_base = np.array([10.0, 20.0, 30.0])
    c_ctrl = resolve_control(
        c_base=c_base, control_mode="rollback", rollback_percent=25.0,
    )
    np.testing.assert_allclose(c_ctrl, [7.5, 15.0, 22.5])


def test_resolve_control_benchmark_same_as_scalar():
    c_base = np.array([10.0, 20.0, 30.0])
    c_ctrl = resolve_control(
        c_base=c_base, control_mode="benchmark", control_value=5.0,
    )
    assert (c_ctrl == 5.0).all()


def test_resolve_control_builtin_defaults_to_baseline():
    c_base = np.array([10.0, 20.0, 30.0])
    c_ctrl = resolve_control(c_base=c_base, control_mode="builtin")
    # Not implemented yet — falls back to baseline
    np.testing.assert_array_equal(c_ctrl, c_base)


from backend.services.resolver import prepare_builtin_inputs


def test_prepare_builtin_inputs_tract_california(tmp_path, monkeypatch):
    _fake_acs_parquet(tmp_path)
    _fake_epa_aqs_state(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))

    result = prepare_builtin_inputs(
        pollutant="pm25", country="us", year=2022,
        analysis_level="tract", state_filter="06",
        control_mode="benchmark", control_value=5.0,
    )
    assert len(result.zone_ids) == 3
    assert (result.c_baseline == 11.4).all()  # CA state value broadcast
    assert (result.c_control == 5.0).all()
    assert result.provenance.concentration["grain"] == "state"
    assert result.provenance.concentration["broadcast_to"] == "tract"
    assert result.provenance.population["grain"] == "tract"
    assert any("broadcast" in w.lower() for w in result.warnings)


def test_prepare_builtin_inputs_rollback_control(tmp_path, monkeypatch):
    _fake_acs_parquet(tmp_path)
    _fake_epa_aqs_state(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))

    result = prepare_builtin_inputs(
        pollutant="pm25", country="us", year=2022,
        analysis_level="state", control_mode="rollback", rollback_percent=20.0,
    )
    # For each state, control = baseline * 0.8
    np.testing.assert_allclose(result.c_control, result.c_baseline * 0.8)


from backend.services.resolver import _resolve_acs_year, YearGapTooLarge


def test_resolve_acs_year_exact_match(tmp_path, monkeypatch):
    _fake_acs_parquet(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))
    year, gap = _resolve_acs_year("us", 2022)
    assert year == 2022
    assert gap == 0


def test_resolve_acs_year_nearest_within_2(tmp_path, monkeypatch):
    _fake_acs_parquet(tmp_path)  # writes 2022.parquet
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))
    year, gap = _resolve_acs_year("us", 2023)  # no 2023 file; use 2022
    assert year == 2022
    assert gap == 1


def test_resolve_acs_year_gap_too_large(tmp_path, monkeypatch):
    _fake_acs_parquet(tmp_path)  # only 2022
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))
    with pytest.raises(YearGapTooLarge):
        _resolve_acs_year("us", 2026)  # gap = 4 > 2


def test_prepare_builtin_inputs_emits_year_gap_warning(tmp_path, monkeypatch):
    _fake_acs_parquet(tmp_path)
    _fake_epa_aqs_state(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))

    # Concentration parquet for 2023 — but demographics only has 2022
    aqs_dir = tmp_path / "processed" / "epa_aqs" / "pm25" / "ne_states"
    pd.DataFrame({"admin_id": ["US-06"], "mean_pm25": [10.0]}).to_parquet(
        aqs_dir / "2023.parquet"
    )

    result = prepare_builtin_inputs(
        pollutant="pm25", country="us", year=2023,
        analysis_level="state", control_mode="benchmark", control_value=5.0,
    )
    assert any("population year" in w.lower() for w in result.warnings)


from backend.services.resolver import prepare_custom_boundary_inputs


def test_prepare_custom_boundary_inputs_country_broadcast(tmp_path, monkeypatch):
    _fake_acs_parquet(tmp_path)

    # WHO AAP country-level PM2.5
    who_dir = tmp_path / "processed" / "who_aap" / "ne_countries"
    who_dir.mkdir(parents=True)
    pd.DataFrame({
        "admin_id": ["USA"], "mean_pm25": [7.5],
    }).to_parquet(who_dir / "2022.parquet")
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))

    # User-uploaded boundary — two tiny polygons
    import geopandas as gpd
    from shapely.geometry import box
    gdf = gpd.GeoDataFrame({
        "id": ["zone-a", "zone-b"],
        "name": ["Zone A", "Zone B"],
        "geometry": [box(-122.5, 37.7, -122.3, 37.9), box(-74.1, 40.6, -73.9, 40.8)],
    }, crs="EPSG:4326")
    boundary_path = tmp_path / "boundary.geojson"
    gdf.to_file(boundary_path, driver="GeoJSON")

    result = prepare_custom_boundary_inputs(
        pollutant="pm25", country="us", year=2022,
        boundary_path=str(boundary_path),
        control_mode="scalar", control_value=5.0,
    )
    assert len(result.zone_ids) == 2
    assert (result.c_baseline == 7.5).all()  # WHO country scalar broadcast
    assert (result.c_control == 5.0).all()
    assert result.provenance.concentration["source"] == "who_aap"
    assert result.provenance.population["grain"] == "country_scalar"
