"""Tests for the WorldPop-via-GEE population datasets: scan entries,
country filtering of global datasets, and the ?dataset= source selector
on /api/data/population/{country}/{year}."""
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from httpx import ASGITransport, AsyncClient
from shapely.geometry import box

from backend.main import app
from backend.routers import data as data_router


@pytest.fixture
def worldpop_data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """GADM adm2 + GHS-SMOD stats parquets, the urban boundary gpkg, and a
    placeholder national-totals dir, under one temp DATA_ROOT."""
    # GADM admin-2 stats: two MEX rows, one NGA row
    adm2_dir = tmp_path / "who_aap" / "gadm_adm2"
    adm2_dir.mkdir(parents=True)
    adm2 = pd.DataFrame({
        "feature_id": ["MEX.1.1_1", "MEX.1.2_1", "NGA.1.1_1"],
        "name": ["Muni Uno", "Muni Dos", "Lagos Mainland"],
        "country_iso3": ["MEX", "MEX", "NGA"],
        "year": [2020] * 3,
        "pop_source_year": [2020] * 3,
        "pop_total": [100_000.0, 50_000.0, 900_000.0],
        "pm25_mean": [20.0, 15.0, 35.0],
        "pm25_popweighted": [21.0, 16.0, 36.0],
        "age_0": [10_000.0, 5_000.0, 90_000.0],
        "age_25": [40_000.0, 20_000.0, 360_000.0],
    })
    adm2.to_parquet(adm2_dir / "2020.parquet")

    # GHS-SMOD urban stats (no country column) + boundary gpkg with iso3
    ghs_dir = tmp_path / "ghs_smod_gee" / "ghs_smod"
    ghs_dir.mkdir(parents=True)
    ghs = pd.DataFrame({
        "feature_id": ["101", "102", "201"],
        "name": ["Ciudad Uno", "Ciudad Dos", "Lagos"],
        "year": [2020] * 3,
        "pop_source_year": [2020] * 3,
        "pop_total": [1_000_000.0, 500_000.0, 12_000_000.0],
        "pm25_mean": [21.0, 16.0, 38.0],
        "pm25_popweighted": [22.0, 17.0, 37.5],
        "age_0": [100_000.0, 50_000.0, 1_200_000.0],
        "age_25": [400_000.0, 200_000.0, 4_800_000.0],
    })
    ghs.to_parquet(ghs_dir / "2020.parquet")

    bdir = tmp_path / "boundaries"
    bdir.mkdir(parents=True)
    gpd.GeoDataFrame(
        {"feature_id": ["101", "102", "201"],
         "name": ["Ciudad Uno", "Ciudad Dos", "Lagos"],
         "country_iso3": ["MEX", "MEX", "NGA"]},
        geometry=[box(0, 0, 1, 1), box(2, 0, 3, 1), box(5, 5, 6, 6)],
        crs="EPSG:4326",
    ).to_file(bdir / "ghs_ucdb_r2024a.gpkg", driver="GPKG")

    # Placeholder national totals for Mexico (the legacy Step 3 source)
    pop_dir = tmp_path / "population" / "mexico"
    pop_dir.mkdir(parents=True)
    pd.DataFrame({
        "admin_id": ["MEX"], "admin_name": ["Mexico"], "total": [126_000_000.0],
    }).to_parquet(pop_dir / "2020.parquet")

    monkeypatch.setattr(data_router, "DATA_ROOT", tmp_path)
    data_router._read_parquet.cache_clear()
    yield tmp_path
    data_router._read_parquet.cache_clear()


async def _get(path: str):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(path)


# ── Scan entries ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_scan_emits_worldpop_population_datasets(worldpop_data_root):
    resp = await _get("/api/data/datasets?type=population")
    assert resp.status_code == 200
    by_id = {d.get("id"): d for d in resp.json()["datasets"]}

    adm2 = by_id.get("gadm_adm2_pop_global")
    assert adm2 is not None
    assert adm2["type"] == "population"
    assert adm2["aggregation"] == "adm2"
    assert adm2["countries_covered"] == ["MEX", "NGA"]
    assert adm2["years"] == [2020]

    urban = by_id.get("ghs_smod_pop_global")
    assert urban is not None
    assert urban["type"] == "population"
    assert urban["aggregation"] == "urban"
    assert urban["countries_covered"] == ["MEX", "NGA"]
    assert urban["years_by_country"]["MEX"] == [2020]


@pytest.mark.asyncio
async def test_country_filter_admits_covering_global_datasets(worldpop_data_root):
    resp = await _get("/api/data/datasets?type=population&country=MEX")
    assert resp.status_code == 200
    ids = {d.get("id") for d in resp.json()["datasets"]}
    assert "gadm_adm2_pop_global" in ids
    assert "ghs_smod_pop_global" in ids
    # The legacy per-country totals entry must still be listed.
    assert any(d.get("country") == "mexico" for d in resp.json()["datasets"])


@pytest.mark.asyncio
async def test_country_filter_excludes_non_covering_global(worldpop_data_root):
    resp = await _get("/api/data/datasets?type=population&country=FRA")
    assert resp.status_code == 200
    ids = {d.get("id") for d in resp.json()["datasets"]}
    assert "gadm_adm2_pop_global" not in ids
    assert "ghs_smod_pop_global" not in ids


# ── ?dataset= selector on the population endpoint ───────────────────


@pytest.mark.asyncio
async def test_population_from_gadm_adm2_dataset(worldpop_data_root):
    resp = await _get("/api/data/population/MEX/2020?dataset=gadm_adm2_pop_global")
    assert resp.status_code == 200
    body = resp.json()
    assert body["source"] == "worldpop_gee_gadm_adm2"
    units = body["units"]
    assert len(units) == 2
    assert {u["admin_id"] for u in units} == {"MEX.1.1_1", "MEX.1.2_1"}
    top = next(u for u in units if u["admin_id"] == "MEX.1.1_1")
    assert top["admin_name"] == "Muni Uno"
    assert top["total"] == 100_000.0
    assert top["age_groups"] == {"age_0": 10_000.0, "age_25": 40_000.0}


@pytest.mark.asyncio
async def test_population_from_ghs_smod_dataset(worldpop_data_root):
    resp = await _get("/api/data/population/mexico/2020?dataset=ghs_smod_pop_global")
    assert resp.status_code == 200
    body = resp.json()
    assert body["source"] == "worldpop_gee_ghs_smod"
    units = body["units"]
    assert len(units) == 2
    assert {u["admin_id"] for u in units} == {"101", "102"}
    assert sum(u["total"] for u in units) == 1_500_000.0


@pytest.mark.asyncio
async def test_population_default_path_unchanged(worldpop_data_root):
    resp = await _get("/api/data/population/mexico/2020")
    assert resp.status_code == 200
    units = resp.json()["units"]
    assert len(units) == 1
    assert units[0]["total"] == 126_000_000.0


@pytest.mark.asyncio
async def test_population_unknown_dataset_404(worldpop_data_root):
    resp = await _get("/api/data/population/MEX/2020?dataset=nope")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_population_dataset_uncovered_country_404(worldpop_data_root):
    resp = await _get("/api/data/population/FRA/2020?dataset=gadm_adm2_pop_global")
    assert resp.status_code == 404
