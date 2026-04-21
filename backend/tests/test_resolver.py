"""Tests for the built-in data resolver."""
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
