"""Tests for /api/data/demographics endpoint filter + simplify behavior."""
from pathlib import Path

import pandas as pd
import pytest
from httpx import ASGITransport, AsyncClient

from backend.main import app
from backend.routers import data as data_router


# ────────────────────────────────────────────────────────────────────
#  Fixture: a tiny demographics parquet across 2 states / 3 counties
# ────────────────────────────────────────────────────────────────────


def _rect_wkt(x0: float, y0: float, dx: float = 1.0, dy: float = 1.0, steps: int = 20) -> str:
    """Build a WKT POLYGON with many collinear points along each side
    so that Douglas-Peucker simplification has something to drop."""
    pts: list[tuple[float, float]] = []
    # bottom edge
    for i in range(steps + 1):
        pts.append((x0 + dx * i / steps, y0))
    # right edge
    for i in range(1, steps + 1):
        pts.append((x0 + dx, y0 + dy * i / steps))
    # top edge
    for i in range(1, steps + 1):
        pts.append((x0 + dx - dx * i / steps, y0 + dy))
    # left edge
    for i in range(1, steps + 1):
        pts.append((x0, y0 + dy - dy * i / steps))
    coord_str = ", ".join(f"{px} {py}" for px, py in pts)
    return f"POLYGON (({coord_str}))"


@pytest.fixture
def demographics_parquet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Write a small demographics parquet and point DATA_ROOT at it."""
    rows = [
        # state 06 (CA), county 037 (LA) — 2 tracts
        {
            "geoid": "06037000100", "state_fips": "06", "county_fips": "037",
            "tract_code": "000100", "vintage": 2022, "boundary_year": 2020,
            "total_pop": 1000, "pct_minority": 0.4, "pct_below_200_pov": 0.15,
            "median_hh_income": 75000.0,
            "geometry": _rect_wkt(-118.0, 34.0),
        },
        {
            "geoid": "06037000200", "state_fips": "06", "county_fips": "037",
            "tract_code": "000200", "vintage": 2022, "boundary_year": 2020,
            "total_pop": 2000, "pct_minority": 0.6, "pct_below_200_pov": 0.25,
            "median_hh_income": 60000.0,
            "geometry": _rect_wkt(-119.0, 34.0),
        },
        # state 06, county 073 (San Diego) — 1 tract
        {
            "geoid": "06073000100", "state_fips": "06", "county_fips": "073",
            "tract_code": "000100", "vintage": 2022, "boundary_year": 2020,
            "total_pop": 1500, "pct_minority": 0.35, "pct_below_200_pov": 0.12,
            "median_hh_income": 85000.0,
            "geometry": _rect_wkt(-117.0, 32.0),
        },
        # state 44 (RI), county 007 — 1 tract
        {
            "geoid": "44007010100", "state_fips": "44", "county_fips": "007",
            "tract_code": "010100", "vintage": 2022, "boundary_year": 2020,
            "total_pop": 800, "pct_minority": 0.2, "pct_below_200_pov": 0.18,
            "median_hh_income": 55000.0,
            "geometry": _rect_wkt(-71.4, 41.8),
        },
    ]
    df = pd.DataFrame(rows)
    out_dir = tmp_path / "demographics" / "us"
    out_dir.mkdir(parents=True)
    out_path = out_dir / "2022.parquet"
    df.to_parquet(out_path, engine="pyarrow")

    # Point the router at the temp data root and invalidate the read cache
    monkeypatch.setattr(data_router, "DATA_ROOT", tmp_path)
    data_router._read_parquet.cache_clear()
    yield out_path
    data_router._read_parquet.cache_clear()


async def _get(path: str):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(path)


# ────────────────────────────────────────────────────────────────────
#  Tests
# ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_returns_all_rows_with_no_filter(demographics_parquet):
    resp = await _get("/api/data/demographics/us/2022")
    assert resp.status_code == 200
    data = resp.json()
    assert data["type"] == "FeatureCollection"
    assert len(data["features"]) == 4


@pytest.mark.asyncio
async def test_state_filter_narrows_results(demographics_parquet):
    resp = await _get("/api/data/demographics/us/2022?state=06")
    assert resp.status_code == 200
    features = resp.json()["features"]
    assert len(features) == 3
    assert all(f["properties"]["state_fips"] == "06" for f in features)


@pytest.mark.asyncio
async def test_state_plus_county_filter(demographics_parquet):
    resp = await _get("/api/data/demographics/us/2022?state=06&county=037")
    assert resp.status_code == 200
    features = resp.json()["features"]
    assert len(features) == 2
    assert all(f["properties"]["county_fips"] == "037" for f in features)


@pytest.mark.asyncio
async def test_county_without_state_is_rejected(demographics_parquet):
    resp = await _get("/api/data/demographics/us/2022?county=037")
    assert resp.status_code == 400
    assert "state" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_filter_matching_nothing_returns_404(demographics_parquet):
    resp = await _get("/api/data/demographics/us/2022?state=99")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_missing_year_returns_404(demographics_parquet):
    resp = await _get("/api/data/demographics/us/1999")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_default_simplify_reduces_vertex_count(demographics_parquet):
    """With the default tolerance the rectangle should collapse to its
    four corners, dropping all the collinear mid-edge points."""
    resp = await _get("/api/data/demographics/us/2022?state=44")
    assert resp.status_code == 200
    feat = resp.json()["features"][0]
    coords = feat["geometry"]["coordinates"][0]
    # Our rect was built with 20 steps per edge (~81 points); a simplified
    # rectangle keeps only the 4 unique corners (+ closing point).
    assert len(coords) <= 6, f"expected simplified rect, got {len(coords)} pts"


@pytest.mark.asyncio
async def test_simplify_zero_preserves_all_vertices(demographics_parquet):
    """simplify=0 should disable simplification entirely."""
    resp = await _get("/api/data/demographics/us/2022?state=44&simplify=0")
    assert resp.status_code == 200
    feat = resp.json()["features"][0]
    coords = feat["geometry"]["coordinates"][0]
    # All 80 edge points preserved (plus closing point = 81).
    assert len(coords) >= 80


@pytest.mark.asyncio
async def test_simplify_above_max_is_rejected(demographics_parquet):
    resp = await _get("/api/data/demographics/us/2022?simplify=1.0")
    assert resp.status_code == 422  # FastAPI/Pydantic validation error


# ────────────────────────────────────────────────────────────────────
#  Vintages endpoint — lets the frontend discover which ACS years are
#  actually on disk instead of hardcoding a fallback list.
# ────────────────────────────────────────────────────────────────────


@pytest.fixture
def demographics_multi_year(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Seed `demographics/us/{2015,2020,2022}.parquet` for vintage discovery."""
    out_dir = tmp_path / "demographics" / "us"
    out_dir.mkdir(parents=True)
    rows = [{
        "geoid": "06037000100", "state_fips": "06", "county_fips": "037",
        "tract_code": "000100", "vintage": 2020, "boundary_year": 2020,
        "total_pop": 1000, "pct_minority": 0.4, "pct_below_200_pov": 0.15,
        "median_hh_income": 75000.0,
        "geometry": _rect_wkt(-118.0, 34.0),
    }]
    df = pd.DataFrame(rows)
    for year in (2015, 2020, 2022):
        df.to_parquet(out_dir / f"{year}.parquet", engine="pyarrow")
    monkeypatch.setattr(data_router, "DATA_ROOT", tmp_path)
    yield out_dir


@pytest.mark.asyncio
async def test_vintages_returns_sorted_list(demographics_multi_year):
    resp = await _get("/api/data/demographics/vintages/us")
    assert resp.status_code == 200
    assert resp.json() == {"country": "us", "vintages": [2015, 2020, 2022]}


@pytest.mark.asyncio
async def test_vintages_accepts_country_aliases(demographics_multi_year):
    resp = await _get("/api/data/demographics/vintages/united-states")
    assert resp.status_code == 200
    # Canonicalized to the on-disk slug "us".
    assert resp.json() == {"country": "us", "vintages": [2015, 2020, 2022]}


@pytest.mark.asyncio
async def test_vintages_unknown_country_returns_404(tmp_path, monkeypatch):
    # Empty DATA_ROOT — no demographics directory.
    monkeypatch.setattr(data_router, "DATA_ROOT", tmp_path)
    resp = await _get("/api/data/demographics/vintages/mexico")
    assert resp.status_code == 404
