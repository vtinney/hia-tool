# HIA Polygon-Based Results Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire Step 1's analysis-level radio through the HIA compute so results are produced per reporting polygon, and add a choropleth map, per-polygon table, and CSV/GeoJSON downloads to the Results page.

**Architecture:** A new `backend/services/resolver.py` orchestrates "finest-of-each-input" data assembly for built-in datasets. The existing `/api/compute/spatial` endpoint becomes a Pydantic discriminated union with three modes (`builtin`, `uploaded`, `builtin_custom_boundary`) that all converge on the existing `_run_spatial_compute` engine. A new `causeRollups` field and `allCauseDeaths` split fix the current cause-specific / all-cause double-counting. The frontend routes spatial runs through this endpoint and renders per-polygon results with a Mapbox choropleth.

**Tech Stack:** Python 3.11, FastAPI, Pydantic v2, NumPy, pandas, GeoPandas, rasterstats, pytest, React 18, Vite, Zustand, Mapbox GL, Recharts, Vitest, React Testing Library.

**Related spec:** `docs/superpowers/specs/2026-04-21-hia-polygon-results-design.md`

---

## Task 0: Prerequisites and baseline

**Files:** none — working-tree check only.

- [ ] **Step 1: Create a feature branch**

```bash
cd C:/Users/vsoutherland/Claude/hia-tool
git checkout main
git pull
git checkout -b feature/polygon-results
```

- [ ] **Step 2: Verify backend baseline tests pass**

Run: `cd backend && python -m pytest -q`
Expected: all existing tests pass. If any fail on `main`, stop and investigate before starting.

- [ ] **Step 3: Verify frontend baseline tests pass**

Run: `cd frontend && npm test -- --run`
Expected: all existing tests pass.

- [ ] **Step 4: Start dev servers and confirm the app loads**

Two terminals:
```bash
# backend
cd backend && uvicorn main:app --reload
# frontend
cd frontend && npm run dev
```
Expected: visit `http://localhost:5173/analysis/1`, pick USA + CA + state level + PM2.5, proceed through Step 2 with manual entry, confirm current behavior runs end-to-end (gives one scalar answer). This is the baseline we're replacing.

---

## Phase 1 — CRF library enrichment

## Task 1: Add `cause` and `endpointType` to every CRF

**Files:**
- Modify: `frontend/src/data/crf-library.json` (all 39 entries)
- Create: `frontend/src/data/__tests__/crf-library.test.js`

- [ ] **Step 1: Write the failing test**

Create `frontend/src/data/__tests__/crf-library.test.js`:

```js
import { describe, it, expect } from 'vitest'
import library from '../crf-library.json'

const VALID_CAUSES = new Set([
  'all_cause', 'ihd', 'stroke', 'lung_cancer', 'copd', 'lri', 'diabetes',
  'dementia', 'asthma', 'asthma_ed', 'respiratory_mortality',
  'respiratory_hosp', 'cardiovascular', 'cardiovascular_hosp',
  'cardiac_hosp', 'birth_weight', 'gestational_age',
])

const VALID_ENDPOINT_TYPES = new Set([
  'mortality', 'hospitalization', 'ed_visit', 'incidence', 'prevalence',
])

describe('crf-library', () => {
  it('every CRF has a valid cause', () => {
    for (const crf of library) {
      expect(VALID_CAUSES.has(crf.cause)).toBe(true)
    }
  })

  it('every CRF has a valid endpointType', () => {
    for (const crf of library) {
      expect(VALID_ENDPOINT_TYPES.has(crf.endpointType)).toBe(true)
    }
  })

  it('ids are unique', () => {
    const ids = library.map((c) => c.id)
    expect(new Set(ids).size).toBe(ids.length)
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd frontend && npm test -- --run src/data/__tests__/crf-library.test.js`
Expected: FAIL — `cause` and `endpointType` are undefined on every CRF.

- [ ] **Step 3: Add `cause` and `endpointType` to every CRF entry**

Apply the mapping below. Every entry must get two new fields inserted alphabetically after `betaHigh` (or any consistent location). Use the Edit tool per entry to minimize diff noise.

Mapping (id → cause, endpointType):
```
epa_pm25_acm_adult           → all_cause,             mortality
epa_pm25_ihd_adult           → ihd,                   mortality
epa_pm25_lc_adult            → lung_cancer,           mortality
epa_pm25_copd_adult          → copd,                  mortality
epa_pm25_stroke_adult        → stroke,                mortality
epa_ozone_resp_mort          → respiratory_mortality, mortality
epa_ozone_acm                → all_cause,             mortality
epa_no2_acm_adult            → all_cause,             mortality
epa_no2_resp_mort            → respiratory_mortality, mortality
epa_so2_acm_adult            → all_cause,             mortality
gbd_pm25_acm_adult           → all_cause,             mortality
gbd_pm25_ihd                 → ihd,                   mortality
gbd_pm25_stroke              → stroke,                mortality
gbd_pm25_lc                  → lung_cancer,           mortality
gbd_pm25_copd                → copd,                  mortality
gbd_pm25_lri                 → lri,                   mortality
gbd_pm25_dm2                 → diabetes,              mortality
gbd_ozone_copd_mort          → copd,                  mortality
gbd_no2_asthma_child         → asthma,                incidence
gemm_pm25_acm                → all_cause,             mortality
gemm_pm25_cvd                → cardiovascular,        mortality
gemm_pm25_resp               → respiratory_mortality, mortality
fusion_pm25_acm              → all_cause,             mortality
fusion_pm25_cvd              → cardiovascular,        mortality
fusion_pm25_lc               → lung_cancer,           mortality
hrapie_pm25_acm              → all_cause,             mortality
hrapie_pm25_resp_hosp        → respiratory_hosp,      hospitalization
hrapie_pm25_cardiac_hosp     → cardiac_hosp,          hospitalization
hrapie_ozone_acm             → all_cause,             mortality
hrapie_so2_resp_hosp         → respiratory_hosp,      hospitalization
hrapie_so2_asthma_ed         → asthma_ed,             ed_visit
st_pm25_acm_liu2019          → all_cause,             mortality
st_pm25_cvd_liu2019          → cardiovascular,        mortality
st_pm25_resp_liu2019         → respiratory_mortality, mortality
st_ozone_acm_bell2004        → all_cause,             mortality
st_ozone_resp_bell2004       → respiratory_mortality, mortality
st_pm25_acm_hrapie           → all_cause,             mortality
st_pm25_hosp_cvd_hrapie      → cardiovascular_hosp,   hospitalization
st_pm25_hosp_resp_hrapie     → respiratory_hosp,      hospitalization
```

Example edit pattern — insert after `"betaHigh"`:

```json
    "beta": 0.00583,
    "betaLow": 0.00396,
    "betaHigh": 0.00769,
    "cause": "all_cause",
    "endpointType": "mortality",
    "source": "Turner et al. 2016 (ACS CPS-II)",
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd frontend && npm test -- --run src/data/__tests__/crf-library.test.js`
Expected: PASS on all three test cases.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/data/crf-library.json frontend/src/data/__tests__/crf-library.test.js
git commit -m "feat(crf): add cause and endpointType to every CRF in the library"
```

---

## Phase 2 — Backend resolver service

## Task 2: Create resolver skeleton with provenance type

**Files:**
- Create: `backend/services/resolver.py`
- Create: `backend/tests/test_resolver.py`

- [ ] **Step 1: Write the failing test**

Create `backend/tests/test_resolver.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && python -m pytest tests/test_resolver.py -v`
Expected: FAIL — `backend.services.resolver` module does not exist.

- [ ] **Step 3: Create resolver skeleton**

Create `backend/services/resolver.py`:

```python
"""Built-in HIA data resolver.

Given a pollutant/country/year/analysisLevel, assembles per-polygon
arrays of concentration, population, and incidence aligned to the
requested reporting polygon. Handles finest-of-each-input logic:
broadcast coarser inputs, aggregate finer inputs.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np


@dataclass
class Provenance:
    """Records the native grain and source of each resolved input."""
    concentration: dict[str, Any]
    population: dict[str, Any]
    incidence: dict[str, Any]


@dataclass
class ResolvedInputs:
    """Per-polygon arrays + metadata returned by the resolver."""
    zone_ids: list[str]
    zone_names: list[str | None]
    parent_ids: list[str | None]
    geometries: list[dict]
    c_baseline: np.ndarray
    c_control: np.ndarray
    population: np.ndarray
    provenance: Provenance
    warnings: list[str] = field(default_factory=list)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && python -m pytest tests/test_resolver.py -v`
Expected: PASS on both tests.

- [ ] **Step 5: Commit**

```bash
git add backend/services/resolver.py backend/tests/test_resolver.py
git commit -m "feat(resolver): add Provenance and ResolvedInputs dataclasses"
```

---

## Task 3: Implement boundary resolution (tract / county / state / country)

**Files:**
- Modify: `backend/services/resolver.py`
- Modify: `backend/tests/test_resolver.py`

- [ ] **Step 1: Write the failing test**

Append to `backend/tests/test_resolver.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && python -m pytest tests/test_resolver.py -v`
Expected: FAIL — `load_reporting_polygons` does not exist.

- [ ] **Step 3: Implement `load_reporting_polygons`**

Append to `backend/services/resolver.py`:

```python
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely import wkt
from shapely.geometry import mapping
from shapely.ops import unary_union


def _data_root() -> Path:
    return Path(os.getenv("DATA_ROOT", "./data/processed"))


def _acs_path(country: str, year: int) -> Path:
    path = _data_root() / "demographics" / country / f"{year}.parquet"
    if not path.exists():
        raise FileNotFoundError(str(path))
    return path


def _gdf_from_acs(country: str, year: int) -> gpd.GeoDataFrame:
    """Load the ACS tract parquet and convert WKT geometry to shapely."""
    df = pd.read_parquet(_acs_path(country, year))
    df["geometry"] = df["geometry"].apply(
        lambda g: wkt.loads(g) if isinstance(g, str) else g
    )
    return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")


def load_reporting_polygons(
    country: str,
    year: int,
    analysis_level: str,
    state_filter: str | None = None,
    county_filter: str | None = None,
) -> dict[str, Any]:
    """Return reporting polygons at the requested grain.

    For ``analysis_level`` values:
        - ``tract``:   ACS tracts directly (requires ``state_filter``)
        - ``county``:  tracts dissolved by ``state_fips + county_fips``
        - ``state``:   tracts dissolved by ``state_fips``
        - ``country``: single polygon (union of all tracts)

    Returns a dict with ``zone_ids``, ``zone_names``, ``parent_ids``,
    ``geometries``, ``population``, and ``population_by_parent``
    (only set for sub-reporting-unit broadcasts later on).
    """
    if analysis_level not in {"tract", "county", "state", "country"}:
        raise ValueError(f"Unknown analysis_level: {analysis_level}")

    gdf = _gdf_from_acs(country, year)
    if state_filter is not None:
        gdf = gdf[gdf["state_fips"] == state_filter]
    if county_filter is not None:
        if state_filter is None:
            raise ValueError("county_filter requires state_filter")
        gdf = gdf[gdf["county_fips"] == county_filter]

    if len(gdf) == 0:
        raise ValueError(
            f"No tracts matched filters state={state_filter} county={county_filter}"
        )

    if analysis_level == "tract":
        return {
            "zone_ids": gdf["geoid"].astype(str).tolist(),
            "zone_names": gdf["geoid"].astype(str).tolist(),
            "parent_ids": gdf["county_fips"].astype(str).tolist(),
            "state_ids": gdf["state_fips"].astype(str).tolist(),
            "geometries": [mapping(g) for g in gdf["geometry"]],
            "population": gdf["total_pop"].to_numpy(dtype=float),
        }

    if analysis_level == "county":
        # Dissolve tracts up to state_fips + county_fips
        gdf = gdf.copy()
        gdf["county_geoid"] = gdf["state_fips"] + gdf["county_fips"]
        dissolved = gdf.dissolve(
            by="county_geoid",
            aggfunc={"total_pop": "sum", "state_fips": "first", "county_fips": "first"},
        ).reset_index()
        return {
            "zone_ids": dissolved["county_geoid"].astype(str).tolist(),
            "zone_names": dissolved["county_geoid"].astype(str).tolist(),
            "parent_ids": dissolved["state_fips"].astype(str).tolist(),
            "state_ids": dissolved["state_fips"].astype(str).tolist(),
            "geometries": [mapping(g) for g in dissolved["geometry"]],
            "population": dissolved["total_pop"].to_numpy(dtype=float),
        }

    if analysis_level == "state":
        dissolved = gdf.dissolve(
            by="state_fips",
            aggfunc={"total_pop": "sum"},
        ).reset_index()
        return {
            "zone_ids": dissolved["state_fips"].astype(str).tolist(),
            "zone_names": dissolved["state_fips"].astype(str).tolist(),
            "parent_ids": [country] * len(dissolved),
            "state_ids": dissolved["state_fips"].astype(str).tolist(),
            "geometries": [mapping(g) for g in dissolved["geometry"]],
            "population": dissolved["total_pop"].to_numpy(dtype=float),
        }

    # analysis_level == "country"
    country_geom = unary_union(gdf["geometry"].to_list())
    return {
        "zone_ids": [country],
        "zone_names": [country.upper()],
        "parent_ids": [None],
        "state_ids": [None],
        "geometries": [mapping(country_geom)],
        "population": np.array([gdf["total_pop"].sum()], dtype=float),
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && python -m pytest tests/test_resolver.py -v`
Expected: all resolver tests pass.

- [ ] **Step 5: Commit**

```bash
git add backend/services/resolver.py backend/tests/test_resolver.py
git commit -m "feat(resolver): load reporting polygons at tract/county/state/country grain"
```

---

## Task 4: Implement concentration resolution with state-broadcast fallback

**Files:**
- Modify: `backend/services/resolver.py`
- Modify: `backend/tests/test_resolver.py`

- [ ] **Step 1: Write the failing test**

Append to `backend/tests/test_resolver.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && python -m pytest tests/test_resolver.py::test_resolve_concentration_state_broadcasts_to_tracts -v`
Expected: FAIL — `resolve_concentration` does not exist.

- [ ] **Step 3: Implement `resolve_concentration`**

Append to `backend/services/resolver.py`:

```python
def _epa_aqs_state_path(pollutant: str, year: int) -> Path:
    return (
        _data_root() / "epa_aqs" / pollutant / "ne_states" / f"{year}.parquet"
    )


def _epa_aqs_country_path(pollutant: str, year: int) -> Path:
    return (
        _data_root() / "epa_aqs" / pollutant / "ne_countries" / f"{year}.parquet"
    )


def _who_aap_path(year: int) -> Path:
    return _data_root() / "who_aap" / "ne_countries" / f"{year}.parquet"


def _concentration_column(df: pd.DataFrame, pollutant: str) -> str:
    """Find the concentration column for a pollutant."""
    candidates = [f"mean_{pollutant}", "mean", "concentration", "value"]
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"No concentration column found in {list(df.columns)}")


def resolve_concentration(
    pollutant: str,
    country: str,
    year: int,
    analysis_level: str,
    polygons: dict[str, Any],
) -> tuple[np.ndarray, dict[str, Any], list[str]]:
    """Return per-polygon concentration values, provenance, and warnings.

    Resolution order:
    1. EPA AQS state-level parquet (US only): direct use if analysis_level == 'state',
       broadcast to tracts/counties via state_ids otherwise.
    2. EPA AQS country-level parquet (US only): broadcast scalar to all polygons.
    3. WHO AAP country-level parquet: broadcast scalar to all polygons.

    Raises FileNotFoundError if no source matches.
    """
    n_zones = len(polygons["zone_ids"])
    warnings: list[str] = []

    # EPA AQS state-level (US)
    state_path = _epa_aqs_state_path(pollutant, year)
    if country == "us" and state_path.exists():
        df = pd.read_parquet(state_path)
        df = df[df["admin_id"].str.startswith("US-", na=False)]
        df["state_fips"] = df["admin_id"].str.replace("US-", "", regex=False)
        col = _concentration_column(df, pollutant)
        lookup = dict(zip(df["state_fips"], df[col]))

        if analysis_level == "state":
            c = np.array(
                [lookup.get(sid, np.nan) for sid in polygons["zone_ids"]],
                dtype=float,
            )
            prov = {"grain": "state", "source": "epa_aqs"}
            return c, prov, warnings

        # Broadcast state value to each tract/county via state_ids
        state_ids = polygons.get("state_ids", [None] * n_zones)
        c = np.array(
            [lookup.get(sid, np.nan) for sid in state_ids], dtype=float,
        )
        prov = {
            "grain": "state", "source": "epa_aqs", "broadcast_to": analysis_level,
        }
        warnings.append(
            f"Concentration (state) broadcast to {analysis_level} reporting unit — "
            f"per-{analysis_level} C is uniform within a state"
        )
        return c, prov, warnings

    # EPA AQS country-level (US)
    country_path = _epa_aqs_country_path(pollutant, year)
    if country == "us" and country_path.exists():
        df = pd.read_parquet(country_path)
        df = df[df["admin_id"] == "USA"]
        if len(df) == 0:
            raise FileNotFoundError(f"No US row in {country_path}")
        col = _concentration_column(df, pollutant)
        scalar = float(df[col].iloc[0])
        c = np.full(n_zones, scalar, dtype=float)
        prov = {
            "grain": "country", "source": "epa_aqs", "broadcast_to": analysis_level,
        }
        warnings.append(
            f"Concentration (country) broadcast to {analysis_level} — "
            f"all polygons share the same C"
        )
        return c, prov, warnings

    # WHO AAP country-level (PM2.5 only)
    who_path = _who_aap_path(year)
    if pollutant == "pm25" and who_path.exists():
        df = pd.read_parquet(who_path)
        iso3_by_slug = {"us": "USA", "mexico": "MEX", "mex": "MEX"}
        iso3 = iso3_by_slug.get(country, country.upper() if len(country) == 3 else None)
        if iso3 is None:
            raise FileNotFoundError(f"No ISO3 mapping for country={country}")
        df = df[df["admin_id"] == iso3]
        if len(df) == 0:
            raise FileNotFoundError(f"No {iso3} row in {who_path}")
        col = _concentration_column(df, pollutant)
        scalar = float(df[col].iloc[0])
        c = np.full(n_zones, scalar, dtype=float)
        prov = {
            "grain": "country", "source": "who_aap", "broadcast_to": analysis_level,
        }
        warnings.append(
            f"Concentration (WHO AAP country-level) broadcast to {analysis_level}"
        )
        return c, prov, warnings

    raise FileNotFoundError(
        f"No concentration data for {pollutant}/{country}/{year} at any grain"
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && python -m pytest tests/test_resolver.py -v`
Expected: all resolver tests pass including the two new ones.

- [ ] **Step 5: Commit**

```bash
git add backend/services/resolver.py backend/tests/test_resolver.py
git commit -m "feat(resolver): resolve concentration with state-broadcast fallback"
```

---

## Task 5: Implement control concentration (scalar / rollback / benchmark)

**Files:**
- Modify: `backend/services/resolver.py`
- Modify: `backend/tests/test_resolver.py`

- [ ] **Step 1: Write the failing test**

Append to `backend/tests/test_resolver.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && python -m pytest tests/test_resolver.py::test_resolve_control_scalar -v`
Expected: FAIL — `resolve_control` does not exist.

- [ ] **Step 3: Implement `resolve_control`**

Append to `backend/services/resolver.py`:

```python
def resolve_control(
    c_base: np.ndarray,
    control_mode: str,
    control_value: float | None = None,
    rollback_percent: float | None = None,
) -> np.ndarray:
    """Compute per-polygon control concentration given the baseline array.

    Modes:
    - ``scalar`` / ``benchmark``: broadcast ``control_value`` to every polygon.
    - ``rollback``: multiply baseline by (1 − rollback_percent/100).
    - ``builtin``: not yet implemented; falls back to baseline (no change scenario).
    """
    if control_mode in ("scalar", "benchmark"):
        if control_value is None:
            raise ValueError(f"control_mode={control_mode} requires control_value")
        return np.full_like(c_base, control_value, dtype=float)

    if control_mode == "rollback":
        if rollback_percent is None:
            raise ValueError("control_mode=rollback requires rollback_percent")
        return c_base * (1.0 - rollback_percent / 100.0)

    if control_mode == "builtin":
        # Future: fetch alternate-year/alternate-scenario from parquet.
        # For v1: no-change fallback.
        return c_base.copy()

    raise ValueError(f"Unknown control_mode: {control_mode}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && python -m pytest tests/test_resolver.py -v`
Expected: all resolver tests pass.

- [ ] **Step 5: Commit**

```bash
git add backend/services/resolver.py backend/tests/test_resolver.py
git commit -m "feat(resolver): resolve control concentration (scalar/rollback/benchmark)"
```

---

## Task 6: Implement `prepare_builtin_inputs` orchestrator

**Files:**
- Modify: `backend/services/resolver.py`
- Modify: `backend/tests/test_resolver.py`

- [ ] **Step 1: Write the failing test**

Append to `backend/tests/test_resolver.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && python -m pytest tests/test_resolver.py::test_prepare_builtin_inputs_tract_california -v`
Expected: FAIL — `prepare_builtin_inputs` does not exist.

- [ ] **Step 3: Implement orchestrator**

Append to `backend/services/resolver.py`:

```python
def prepare_builtin_inputs(
    pollutant: str,
    country: str,
    year: int,
    analysis_level: str,
    control_mode: str,
    state_filter: str | None = None,
    county_filter: str | None = None,
    control_value: float | None = None,
    rollback_percent: float | None = None,
) -> ResolvedInputs:
    """Top-level orchestrator for built-in data requests.

    Loads reporting polygons, resolves concentration and control,
    returns ``ResolvedInputs`` with provenance and warnings.
    """
    polygons = load_reporting_polygons(
        country=country, year=year, analysis_level=analysis_level,
        state_filter=state_filter, county_filter=county_filter,
    )

    c_base, c_prov, c_warnings = resolve_concentration(
        pollutant=pollutant, country=country, year=year,
        analysis_level=analysis_level, polygons=polygons,
    )

    c_ctrl = resolve_control(
        c_base=c_base, control_mode=control_mode,
        control_value=control_value, rollback_percent=rollback_percent,
    )

    # Population grain is determined by analysis_level — we always pull
    # ACS tract and dissolve, so population grain IS analysis_level.
    pop_prov = {"grain": analysis_level, "source": "acs"}

    # Incidence provenance is filled in at compute time when CRF rates
    # are known. Placeholder is overwritten by the router.
    inc_prov = {"grain": "crf_default", "source": "crf_library"}

    return ResolvedInputs(
        zone_ids=polygons["zone_ids"],
        zone_names=polygons["zone_names"],
        parent_ids=polygons["parent_ids"],
        geometries=polygons["geometries"],
        c_baseline=c_base,
        c_control=c_ctrl,
        population=polygons["population"],
        provenance=Provenance(
            concentration=c_prov,
            population=pop_prov,
            incidence=inc_prov,
        ),
        warnings=c_warnings,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && python -m pytest tests/test_resolver.py -v`
Expected: all resolver tests pass.

- [ ] **Step 5: Commit**

```bash
git add backend/services/resolver.py backend/tests/test_resolver.py
git commit -m "feat(resolver): add prepare_builtin_inputs orchestrator"
```

---

## Task 6.5: Implement nearest-year ACS fallback with ±2 year warning

**Files:**
- Modify: `backend/services/resolver.py`
- Modify: `backend/tests/test_resolver.py`

- [ ] **Step 1: Write the failing test**

Append to `backend/tests/test_resolver.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && python -m pytest tests/test_resolver.py::test_resolve_acs_year_exact_match -v`
Expected: FAIL — `_resolve_acs_year` / `YearGapTooLarge` do not exist.

- [ ] **Step 3: Implement the fallback**

Append to `backend/services/resolver.py`:

```python
class YearGapTooLarge(Exception):
    """Raised when requested year is > 2 years from the nearest ACS file."""


MAX_ACS_YEAR_GAP = 2


def _available_acs_years(country: str) -> list[int]:
    """Return sorted years for which we have a demographics/{country}/{y}.parquet."""
    dirpath = _data_root() / "demographics" / country
    if not dirpath.exists():
        return []
    return sorted(
        int(p.stem) for p in dirpath.iterdir()
        if p.suffix == ".parquet" and p.stem.isdigit()
    )


def _resolve_acs_year(country: str, requested_year: int) -> tuple[int, int]:
    """Find the closest available ACS year. Returns (year, |gap|).

    Raises ``YearGapTooLarge`` when no year within MAX_ACS_YEAR_GAP exists.
    """
    years = _available_acs_years(country)
    if not years:
        raise FileNotFoundError(f"No ACS demographics for country={country}")
    nearest = min(years, key=lambda y: abs(y - requested_year))
    gap = abs(nearest - requested_year)
    if gap > MAX_ACS_YEAR_GAP:
        raise YearGapTooLarge(
            f"Nearest ACS year {nearest} is {gap} years from requested {requested_year}"
        )
    return nearest, gap
```

Then modify `prepare_builtin_inputs` to use this. Replace its first lines:

```python
def prepare_builtin_inputs(
    pollutant: str,
    country: str,
    year: int,
    analysis_level: str,
    control_mode: str,
    state_filter: str | None = None,
    county_filter: str | None = None,
    control_value: float | None = None,
    rollback_percent: float | None = None,
) -> ResolvedInputs:
    # Resolve ACS year first — may differ from requested concentration year
    acs_year, year_gap = _resolve_acs_year(country, year)

    polygons = load_reporting_polygons(
        country=country, year=acs_year, analysis_level=analysis_level,
        state_filter=state_filter, county_filter=county_filter,
    )

    c_base, c_prov, c_warnings = resolve_concentration(
        pollutant=pollutant, country=country, year=year,
        analysis_level=analysis_level, polygons=polygons,
    )

    c_ctrl = resolve_control(
        c_base=c_base, control_mode=control_mode,
        control_value=control_value, rollback_percent=rollback_percent,
    )

    pop_prov = {"grain": analysis_level, "source": "acs", "year": acs_year}
    inc_prov = {"grain": "crf_default", "source": "crf_library"}

    warnings = list(c_warnings)
    if year_gap > 0:
        warnings.append(
            f"Population year {acs_year} used for concentration year {year} "
            f"(gap of {year_gap} year{'s' if year_gap != 1 else ''})"
        )

    return ResolvedInputs(
        zone_ids=polygons["zone_ids"],
        zone_names=polygons["zone_names"],
        parent_ids=polygons["parent_ids"],
        geometries=polygons["geometries"],
        c_baseline=c_base,
        c_control=c_ctrl,
        population=polygons["population"],
        provenance=Provenance(
            concentration=c_prov,
            population=pop_prov,
            incidence=inc_prov,
        ),
        warnings=warnings,
    )
```

Also map `YearGapTooLarge` to an HTTP 422 in the router. In Task 10's `run_spatial_compute` wrapper, add at the top:

```python
from backend.services.resolver import YearGapTooLarge

try:
    if req.mode == "builtin":
        resolved = prepare_builtin_inputs(...)
    ...
except YearGapTooLarge as e:
    raise HTTPException(status_code=422, detail=str(e))
except FileNotFoundError as e:
    raise HTTPException(status_code=404, detail=str(e))
```

(Task 10 below already has `prepare_builtin_inputs` calls — wrap them in this try/except when implementing Task 10.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && python -m pytest tests/test_resolver.py -v`
Expected: all resolver tests pass.

- [ ] **Step 5: Commit**

```bash
git add backend/services/resolver.py backend/tests/test_resolver.py
git commit -m "feat(resolver): nearest-year ACS fallback with ±2 year warning"
```

---

## Task 7: Implement custom-boundary resolver (built-in C/pop + uploaded boundary)

**Files:**
- Modify: `backend/services/resolver.py`
- Modify: `backend/tests/test_resolver.py`

- [ ] **Step 1: Write the failing test**

Append to `backend/tests/test_resolver.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && python -m pytest tests/test_resolver.py::test_prepare_custom_boundary_inputs_country_broadcast -v`
Expected: FAIL — `prepare_custom_boundary_inputs` does not exist.

- [ ] **Step 3: Implement**

Append to `backend/services/resolver.py`:

```python
from backend.services.geo_processor import (
    read_boundaries, _detect_id_column, _detect_name_column,
)


def prepare_custom_boundary_inputs(
    pollutant: str,
    country: str,
    year: int,
    boundary_path: str,
    control_mode: str,
    control_value: float | None = None,
    rollback_percent: float | None = None,
) -> ResolvedInputs:
    """Resolver for user-uploaded boundary + built-in C/pop.

    Today, population for non-US custom boundaries falls back to
    WHO AAP country-level scalar broadcast. US custom boundaries use
    the same fallback until we add zonal-stats of ACS tracts — the
    follow-up for that is noted in the spec.
    """
    gdf = read_boundaries(boundary_path)
    n_zones = len(gdf)
    id_col = _detect_id_column(gdf)
    name_col = _detect_name_column(gdf)

    zone_ids = (
        gdf[id_col].astype(str).tolist()
        if id_col != "index" else [str(i) for i in range(n_zones)]
    )
    zone_names = (
        gdf[name_col].astype(str).tolist() if name_col else [None] * n_zones
    )
    geometries = [mapping(g) if g else None for g in gdf.geometry]

    # Build a synthetic polygons dict so resolve_concentration works
    # unchanged. state_ids is unknown for custom boundaries.
    polygons = {
        "zone_ids": zone_ids,
        "zone_names": zone_names,
        "parent_ids": [None] * n_zones,
        "state_ids": [None] * n_zones,
        "geometries": geometries,
        "population": np.zeros(n_zones, dtype=float),
    }

    # resolve_concentration only broadcasts country-level scalars for
    # custom boundaries (state-level broadcast needs state_ids).
    # We drop back to country-level resolution by clearing state_ids.
    c_base, c_prov, c_warnings = resolve_concentration(
        pollutant=pollutant, country=country, year=year,
        analysis_level="custom", polygons=polygons,
    )
    c_ctrl = resolve_control(
        c_base=c_base, control_mode=control_mode,
        control_value=control_value, rollback_percent=rollback_percent,
    )

    # Population fallback for custom boundaries: country-level scalar,
    # split evenly across polygons. Flagged explicitly.
    pop_total_path = _data_root() / "population" / country / f"{year}.parquet"
    pop_per_zone = np.zeros(n_zones, dtype=float)
    pop_prov = {"grain": "country_scalar", "source": "fallback_even_split"}
    warnings = list(c_warnings)
    warnings.append(
        "Population fallback: country-level total split evenly across "
        "custom polygons. Upload a population raster for per-polygon accuracy."
    )

    if pop_total_path.exists():
        df = pd.read_parquet(pop_total_path)
        total = float(df["total"].sum()) if "total" in df.columns else 0.0
        pop_per_zone = np.full(n_zones, total / max(n_zones, 1))

    return ResolvedInputs(
        zone_ids=zone_ids,
        zone_names=zone_names,
        parent_ids=[None] * n_zones,
        geometries=geometries,
        c_baseline=c_base,
        c_control=c_ctrl,
        population=pop_per_zone,
        provenance=Provenance(
            concentration=c_prov,
            population=pop_prov,
            incidence={"grain": "crf_default", "source": "crf_library"},
        ),
        warnings=warnings,
    )
```

Also update `resolve_concentration` — change the guard that restricts state-broadcast so a `"custom"` analysis_level skips the state path and falls through to country:

```python
# In resolve_concentration, replace:
#   if country == "us" and state_path.exists():
# with:
    if country == "us" and state_path.exists() and analysis_level != "custom":
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && python -m pytest tests/test_resolver.py -v`
Expected: all resolver tests pass.

- [ ] **Step 5: Commit**

```bash
git add backend/services/resolver.py backend/tests/test_resolver.py
git commit -m "feat(resolver): add prepare_custom_boundary_inputs"
```

---

## Phase 3 — Compute router (discriminated union + cause rollups)

## Task 8: Add Pydantic request/response models for the new endpoint shape

**Files:**
- Modify: `backend/routers/compute.py`
- Create: `backend/tests/test_compute_models.py`

- [ ] **Step 1: Write the failing test**

Create `backend/tests/test_compute_models.py`:

```python
"""Tests for the discriminated-union request and extended response models."""
import pytest
from pydantic import ValidationError

from backend.routers.compute import (
    SpatialComputeRequest, SpatialComputeResponse, ProvenanceModel,
    CauseRollup, EstimateCI, CRFInput,
)


def _sample_crf() -> dict:
    return {
        "id": "epa_pm25_ihd_adult", "source": "Pope 2004", "endpoint": "IHD",
        "beta": 0.015, "betaLow": 0.01, "betaHigh": 0.02,
        "functionalForm": "log-linear", "defaultRate": 0.0025,
        "cause": "ihd", "endpointType": "mortality",
    }


def test_builtin_request_validates():
    payload = {
        "mode": "builtin",
        "pollutant": "pm25", "country": "us", "year": 2022,
        "analysisLevel": "tract", "stateFilter": "06",
        "controlMode": "benchmark", "controlConcentration": 5.0,
        "selectedCRFs": [_sample_crf()],
        "monteCarloIterations": 1000,
    }
    req = SpatialComputeRequest.model_validate(payload)
    assert req.mode == "builtin"
    assert req.analysisLevel == "tract"


def test_uploaded_request_validates():
    payload = {
        "mode": "uploaded",
        "concentrationFileId": 1, "populationFileId": 2, "boundaryFileId": 3,
        "selectedCRFs": [_sample_crf()],
    }
    req = SpatialComputeRequest.model_validate(payload)
    assert req.mode == "uploaded"


def test_custom_boundary_request_validates():
    payload = {
        "mode": "builtin_custom_boundary",
        "pollutant": "pm25", "country": "us", "year": 2022,
        "boundaryFileId": 3,
        "controlMode": "scalar", "controlConcentration": 5.0,
        "selectedCRFs": [_sample_crf()],
    }
    req = SpatialComputeRequest.model_validate(payload)
    assert req.mode == "builtin_custom_boundary"


def test_discriminator_rejects_unknown_mode():
    with pytest.raises(ValidationError):
        SpatialComputeRequest.model_validate({"mode": "banana"})


def test_cause_rollup_has_required_fields():
    r = CauseRollup(
        cause="ihd", endpointLabel="Ischemic heart disease",
        attributableCases=EstimateCI(mean=100, lower95=50, upper95=150),
        attributableRate=EstimateCI(mean=5.0, lower95=2.5, upper95=7.5),
        crfIds=["epa_pm25_ihd_adult"],
    )
    assert r.cause == "ihd"
    assert len(r.crfIds) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && python -m pytest tests/test_compute_models.py -v`
Expected: FAIL — new classes don't exist.

- [ ] **Step 3: Update `backend/routers/compute.py`**

Add to the top of `backend/routers/compute.py` (after existing imports):

```python
from typing import Annotated, Literal, Union
from pydantic import Field as PydField
```

Add these models above the existing `ComputeRequest`:

```python
# ── New discriminated-union request models ────────────────────────

class CRFInputV2(BaseModel):
    """Extension of CRFInput that carries cause / endpointType."""
    id: str
    source: str = ""
    endpoint: str = ""
    beta: float
    betaLow: float
    betaHigh: float
    functionalForm: str = "log-linear"
    defaultRate: float | None = None
    cause: str = "all_cause"          # enum validated client-side
    endpointType: str = "mortality"   # mortality | hospitalization | ed_visit | incidence | prevalence


class BuiltinMode(BaseModel):
    mode: Literal["builtin"]
    pollutant: str
    country: str
    year: int
    analysisLevel: Literal["country", "state", "county", "tract"]
    stateFilter: str | None = None
    countyFilter: str | None = None
    controlMode: Literal["scalar", "builtin", "rollback", "benchmark"]
    controlConcentration: float | None = None
    controlRollbackPercent: float | None = None
    selectedCRFs: list[CRFInputV2]
    monteCarloIterations: int = Field(default=1000, ge=100, le=50_000)


class UploadedMode(BaseModel):
    mode: Literal["uploaded"]
    concentrationFileId: int
    controlFileId: int | None = None
    controlConcentration: float | None = None
    populationFileId: int
    boundaryFileId: int
    selectedCRFs: list[CRFInputV2]
    monteCarloIterations: int = Field(default=1000, ge=100, le=50_000)


class CustomBoundaryBuiltinMode(BaseModel):
    mode: Literal["builtin_custom_boundary"]
    pollutant: str
    country: str
    year: int
    boundaryFileId: int
    controlMode: Literal["scalar", "builtin", "rollback", "benchmark"]
    controlConcentration: float | None = None
    controlRollbackPercent: float | None = None
    selectedCRFs: list[CRFInputV2]
    monteCarloIterations: int = Field(default=1000, ge=100, le=50_000)


SpatialComputeRequest = Annotated[
    Union[BuiltinMode, UploadedMode, CustomBoundaryBuiltinMode],
    PydField(discriminator="mode"),
]


# ── New response models ──────────────────────────────────────────

class ProvenanceModel(BaseModel):
    concentration: dict
    population: dict
    incidence: dict


class CauseRollup(BaseModel):
    cause: str
    endpointLabel: str
    attributableCases: EstimateCI
    attributableRate: EstimateCI
    crfIds: list[str]


class SpatialComputeResponseV2(BaseModel):
    """Extended spatial response with provenance, rollups, and separate totals."""
    resultId: str
    zones: list[ZoneResult]
    aggregate: ComputeResponse
    causeRollups: list[CauseRollup]
    totalDeaths: EstimateCI
    allCauseDeaths: EstimateCI | None = None
    provenance: ProvenanceModel
    warnings: list[str] = Field(default_factory=list)
    processingTimeSeconds: float = 0.0
```

Keep the original `SpatialComputeRequest` and `SpatialComputeResponse` for now — they'll be replaced in Task 9.

Also add `parent_id` to the existing `ZoneResult` by editing it:

```python
class ZoneResult(BaseModel):
    zoneId: str
    zoneName: str | None = None
    parentId: str | None = None  # NEW
    geometry: dict | None = None
    baselineConcentration: float
    controlConcentration: float
    population: float
    results: list[CRFResult]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && python -m pytest tests/test_compute_models.py -v`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add backend/routers/compute.py backend/tests/test_compute_models.py
git commit -m "feat(api): add discriminated-union request + rollup response models"
```

---

## Task 9: Implement cause rollup aggregation + totalDeaths/allCauseDeaths split

**Files:**
- Create: `backend/services/rollups.py`
- Create: `backend/tests/test_rollups.py`

- [ ] **Step 1: Write the failing test**

Create `backend/tests/test_rollups.py`:

```python
"""Tests for cause-rollup aggregation and mortality split."""
from backend.services.rollups import build_cause_rollups, split_mortality_totals


def _crf_result(crf_id, cause, endpoint_type, cases_mean, rate_mean):
    return {
        "crfId": crf_id,
        "study": "test",
        "endpoint": crf_id,
        "attributableCases": {"mean": cases_mean, "lower95": cases_mean * 0.8, "upper95": cases_mean * 1.2},
        "attributableFraction": {"mean": 0.01, "lower95": 0.005, "upper95": 0.015},
        "attributableRate": {"mean": rate_mean, "lower95": rate_mean * 0.8, "upper95": rate_mean * 1.2},
        "_cause": cause,
        "_endpointType": endpoint_type,
    }


def test_build_cause_rollups_sums_within_cause():
    # Two stroke CRFs → one "stroke" rollup summing both
    results = [
        _crf_result("epa_pm25_stroke_adult", "stroke", "mortality", 100, 5.0),
        _crf_result("gbd_pm25_stroke",       "stroke", "mortality", 80, 4.0),
        _crf_result("epa_pm25_ihd_adult",    "ihd",    "mortality", 50, 2.5),
    ]
    rollups = build_cause_rollups(results)
    by_cause = {r["cause"]: r for r in rollups}
    assert by_cause["stroke"]["attributableCases"]["mean"] == 180
    assert by_cause["ihd"]["attributableCases"]["mean"] == 50
    assert sorted(by_cause["stroke"]["crfIds"]) == sorted(
        ["epa_pm25_stroke_adult", "gbd_pm25_stroke"]
    )


def test_split_mortality_totals_excludes_all_cause():
    # IHD + stroke are cause-specific; ACM is separate
    results = [
        _crf_result("epa_pm25_ihd_adult",  "ihd",       "mortality", 50, 2.5),
        _crf_result("epa_pm25_stroke_adult","stroke",   "mortality", 30, 1.5),
        _crf_result("epa_pm25_acm_adult",  "all_cause", "mortality", 200, 10.0),
    ]
    total_deaths, all_cause_deaths = split_mortality_totals(results)
    assert total_deaths["mean"] == 80  # IHD + stroke, NOT including all-cause
    assert all_cause_deaths is not None
    assert all_cause_deaths["mean"] == 200


def test_split_mortality_totals_no_all_cause():
    results = [
        _crf_result("epa_pm25_ihd_adult", "ihd",    "mortality", 50, 2.5),
        _crf_result("epa_pm25_stroke_adult","stroke","mortality", 30, 1.5),
    ]
    total_deaths, all_cause_deaths = split_mortality_totals(results)
    assert total_deaths["mean"] == 80
    assert all_cause_deaths is None


def test_build_cause_rollups_skips_non_mortality_when_filtered():
    """Rollups include ALL causes for UI, but endpoint_type filtering
    happens at the mortality-total step."""
    results = [
        _crf_result("hrapie_pm25_resp_hosp", "respiratory_hosp", "hospitalization", 25, 1.2),
        _crf_result("epa_pm25_ihd_adult",    "ihd",              "mortality",        50, 2.5),
    ]
    rollups = build_cause_rollups(results)
    causes = {r["cause"] for r in rollups}
    assert causes == {"respiratory_hosp", "ihd"}
    total_deaths, _ = split_mortality_totals(results)
    assert total_deaths["mean"] == 50  # hosp excluded
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && python -m pytest tests/test_rollups.py -v`
Expected: FAIL — `backend.services.rollups` does not exist.

- [ ] **Step 3: Implement**

Create `backend/services/rollups.py`:

```python
"""Cause-based rollups and mortality total splits.

The HIA engine emits one result per CRF. For the Results page we need:
- Per-cause totals (sum across CRFs tagged with the same cause)
- A cause-specific mortality total (excluding all-cause to avoid double-counting)
- A separate all-cause mortality total (only when an all-cause CRF was selected)
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any

CAUSE_LABELS = {
    "all_cause": "All-cause mortality",
    "ihd": "Ischemic heart disease",
    "stroke": "Stroke",
    "lung_cancer": "Lung cancer",
    "copd": "COPD",
    "lri": "Lower respiratory infection",
    "diabetes": "Type 2 diabetes",
    "dementia": "Dementia",
    "asthma": "Asthma incidence",
    "asthma_ed": "Asthma ED visits",
    "respiratory_mortality": "Respiratory mortality",
    "respiratory_hosp": "Respiratory hospitalization",
    "cardiovascular": "Cardiovascular mortality",
    "cardiovascular_hosp": "Cardiovascular hospitalization",
    "cardiac_hosp": "Cardiac hospitalization",
    "birth_weight": "Low birth weight",
    "gestational_age": "Preterm birth",
}


def _zero_ci() -> dict[str, float]:
    return {"mean": 0.0, "lower95": 0.0, "upper95": 0.0}


def _sum_ci(a: dict[str, float], b: dict[str, float]) -> dict[str, float]:
    return {
        "mean": a["mean"] + b["mean"],
        "lower95": a["lower95"] + b["lower95"],
        "upper95": a["upper95"] + b["upper95"],
    }


def build_cause_rollups(
    crf_results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Group CRF results by cause; sum cases, population-weight rates.

    Each ``crf_results`` item must carry private ``_cause`` and
    ``_endpointType`` keys added by the compute router.
    """
    by_cause: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in crf_results:
        by_cause[r["_cause"]].append(r)

    rollups: list[dict[str, Any]] = []
    for cause, results in by_cause.items():
        cases = _zero_ci()
        rate = _zero_ci()
        for r in results:
            cases = _sum_ci(cases, r["attributableCases"])
            # Simple mean across CRFs in the same cause — not
            # population-weighted because rates already are per-100k.
            rate["mean"] += r["attributableRate"]["mean"]
            rate["lower95"] += r["attributableRate"]["lower95"]
            rate["upper95"] += r["attributableRate"]["upper95"]
        rollups.append({
            "cause": cause,
            "endpointLabel": CAUSE_LABELS.get(cause, cause),
            "attributableCases": cases,
            "attributableRate": rate,
            "crfIds": [r["crfId"] for r in results],
        })
    return rollups


def split_mortality_totals(
    crf_results: list[dict[str, Any]],
) -> tuple[dict[str, float], dict[str, float] | None]:
    """Return (totalDeaths, allCauseDeaths).

    ``totalDeaths`` = sum across CRFs with ``_endpointType == "mortality"``
    AND ``_cause != "all_cause"`` (cause-specific mortality only).

    ``allCauseDeaths`` = sum across CRFs with ``_cause == "all_cause"``
    AND ``_endpointType == "mortality"``, or ``None`` when no all-cause
    mortality CRF was selected.

    The two totals are never summed together — that would double-count.
    """
    total_deaths = _zero_ci()
    all_cause_deaths = _zero_ci()
    any_all_cause = False

    for r in crf_results:
        if r.get("_endpointType") != "mortality":
            continue
        if r.get("_cause") == "all_cause":
            all_cause_deaths = _sum_ci(all_cause_deaths, r["attributableCases"])
            any_all_cause = True
        else:
            total_deaths = _sum_ci(total_deaths, r["attributableCases"])

    return total_deaths, (all_cause_deaths if any_all_cause else None)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && python -m pytest tests/test_rollups.py -v`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add backend/services/rollups.py backend/tests/test_rollups.py
git commit -m "feat(rollups): add cause-based rollups and mortality total split"
```

---

## Task 10: Wire resolver + rollups into `/api/compute/spatial`

**Files:**
- Modify: `backend/routers/compute.py`
- Modify: `backend/tests/` (new `test_compute_spatial.py`)

- [ ] **Step 1: Write the failing test**

Create `backend/tests/test_compute_spatial.py`:

```python
"""End-to-end tests for /api/compute/spatial with the new modes."""
import numpy as np
import pandas as pd
import pytest
from pathlib import Path
from fastapi.testclient import TestClient

from backend.main import app


def _setup_data(tmp_path: Path) -> None:
    # ACS tracts for CA
    df = pd.DataFrame({
        "geoid":       ["06001000100", "06001000200", "06003000100"],
        "state_fips":  ["06", "06", "06"],
        "county_fips": ["001", "001", "003"],
        "total_pop":   [3500, 4200, 500],
        "geometry":    ["POLYGON((0 0,1 0,1 1,0 1,0 0))"] * 3,
    })
    p = tmp_path / "processed" / "demographics" / "us"
    p.mkdir(parents=True)
    df.to_parquet(p / "2022.parquet")

    # EPA AQS state-level PM2.5
    aqs = pd.DataFrame({"admin_id": ["US-06"], "mean_pm25": [11.4]})
    a = tmp_path / "processed" / "epa_aqs" / "pm25" / "ne_states"
    a.mkdir(parents=True)
    aqs.to_parquet(a / "2022.parquet")


def _ihd_crf() -> dict:
    return {
        "id": "epa_pm25_ihd_adult", "source": "Pope 2004",
        "endpoint": "Ischemic heart disease",
        "beta": 0.015, "betaLow": 0.01, "betaHigh": 0.02,
        "functionalForm": "log-linear", "defaultRate": 0.0025,
        "cause": "ihd", "endpointType": "mortality",
    }


def _acm_crf() -> dict:
    return {
        "id": "epa_pm25_acm_adult", "source": "Turner 2016",
        "endpoint": "All-cause mortality",
        "beta": 0.00583, "betaLow": 0.00396, "betaHigh": 0.00769,
        "functionalForm": "log-linear", "defaultRate": 0.008,
        "cause": "all_cause", "endpointType": "mortality",
    }


def test_builtin_mode_returns_per_tract_results(tmp_path, monkeypatch):
    _setup_data(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))
    client = TestClient(app)
    r = client.post("/api/compute/spatial", json={
        "mode": "builtin",
        "pollutant": "pm25", "country": "us", "year": 2022,
        "analysisLevel": "tract", "stateFilter": "06",
        "controlMode": "benchmark", "controlConcentration": 5.0,
        "selectedCRFs": [_ihd_crf()],
        "monteCarloIterations": 200,
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["zones"]) == 3
    assert body["provenance"]["concentration"]["grain"] == "state"
    assert "broadcast_to" in body["provenance"]["concentration"]
    assert len(body["causeRollups"]) == 1
    assert body["causeRollups"][0]["cause"] == "ihd"
    assert body["allCauseDeaths"] is None
    assert any("broadcast" in w.lower() for w in body["warnings"])


def test_builtin_mode_splits_all_cause_and_cause_specific(tmp_path, monkeypatch):
    _setup_data(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))
    client = TestClient(app)
    r = client.post("/api/compute/spatial", json={
        "mode": "builtin",
        "pollutant": "pm25", "country": "us", "year": 2022,
        "analysisLevel": "state", "stateFilter": "06",
        "controlMode": "benchmark", "controlConcentration": 5.0,
        "selectedCRFs": [_ihd_crf(), _acm_crf()],
        "monteCarloIterations": 200,
    })
    assert r.status_code == 200
    body = r.json()
    assert body["allCauseDeaths"] is not None
    assert body["totalDeaths"]["mean"] > 0  # cause-specific (IHD only)
    assert body["allCauseDeaths"]["mean"] > 0
    # Total and all-cause are NEVER summed together — verify they differ
    assert body["totalDeaths"]["mean"] != body["allCauseDeaths"]["mean"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && python -m pytest tests/test_compute_spatial.py -v`
Expected: FAIL — current endpoint doesn't accept the builtin mode.

- [ ] **Step 3: Rewrite the `/api/compute/spatial` handler**

In `backend/routers/compute.py`:
1. **Delete** the existing `run_spatial_compute` async handler (decorator `@router.post("/compute/spatial"...)`).
2. **Delete** the existing `_run_spatial_compute` worker function it called.
3. **Delete** the old `SpatialComputeRequest` and `SpatialComputeResponse` classes (their fields were rolled into the new discriminated union and `SpatialComputeResponseV2`).
4. Add the new code below. The old `ZoneResult`, `CRFResult`, `EstimateCI`, `ComputeResponse` classes are still used — keep them.

Replace the route handler with:

```python
import time
import uuid

from backend.services.resolver import (
    prepare_builtin_inputs, prepare_custom_boundary_inputs, ResolvedInputs,
)
from backend.services.rollups import build_cause_rollups, split_mortality_totals
from backend.services.results_cache import save_result  # added in Task 12


@router.post("/compute/spatial", response_model=SpatialComputeResponseV2)
async def run_spatial_compute(
    req: SpatialComputeRequest,
    db: AsyncSession = Depends(get_db),
) -> SpatialComputeResponseV2:
    """Unified spatial compute: built-in, uploaded, or custom boundary."""
    start = time.perf_counter()

    if req.mode == "builtin":
        resolved = prepare_builtin_inputs(
            pollutant=req.pollutant, country=req.country, year=req.year,
            analysis_level=req.analysisLevel,
            state_filter=req.stateFilter, county_filter=req.countyFilter,
            control_mode=req.controlMode,
            control_value=req.controlConcentration,
            rollback_percent=req.controlRollbackPercent,
        )
    elif req.mode == "builtin_custom_boundary":
        boundary_record = await _get_upload(db, req.boundaryFileId)
        resolved = prepare_custom_boundary_inputs(
            pollutant=req.pollutant, country=req.country, year=req.year,
            boundary_path=str(_resolve_file_path(boundary_record)),
            control_mode=req.controlMode,
            control_value=req.controlConcentration,
            rollback_percent=req.controlRollbackPercent,
        )
    else:  # uploaded
        resolved = await _resolve_uploaded(db, req)

    crfs_as_dicts = [crf.model_dump() for crf in req.selectedCRFs]

    loop = asyncio.get_event_loop()
    raw = await loop.run_in_executor(
        _executor,
        _run_spatial_compute_v2,
        resolved,
        crfs_as_dicts,
        req.monteCarloIterations,
    )

    # Build rollups and totals
    crf_results_with_tags = raw["aggregate_crf_results"]  # carries _cause/_endpointType
    rollups = build_cause_rollups(crf_results_with_tags)
    total_deaths, all_cause_deaths = split_mortality_totals(crf_results_with_tags)

    # Strip private keys before serialization
    for r in crf_results_with_tags:
        r.pop("_cause", None)
        r.pop("_endpointType", None)

    result_id = str(uuid.uuid4())
    elapsed = time.perf_counter() - start

    response = SpatialComputeResponseV2(
        resultId=result_id,
        zones=raw["zones"],
        aggregate=ComputeResponse(
            results=crf_results_with_tags,
            totalDeaths=total_deaths,  # cause-specific only
        ),
        causeRollups=[CauseRollup.model_validate(r) for r in rollups],
        totalDeaths=EstimateCI.model_validate(total_deaths),
        allCauseDeaths=EstimateCI.model_validate(all_cause_deaths) if all_cause_deaths else None,
        provenance=ProvenanceModel.model_validate({
            "concentration": resolved.provenance.concentration,
            "population": resolved.provenance.population,
            "incidence": resolved.provenance.incidence,
        }),
        warnings=resolved.warnings,
        processingTimeSeconds=elapsed,
    )
    save_result(result_id, response)  # for download endpoints
    return response


async def _get_upload(db: AsyncSession, file_id: int) -> FileUpload:
    result = await db.execute(select(FileUpload).where(FileUpload.id == file_id))
    record = result.scalar_one_or_none()
    if record is None:
        raise HTTPException(status_code=404, detail=f"File upload {file_id} not found")
    if record.status == "error":
        raise HTTPException(
            status_code=400,
            detail=f"File {record.original_filename} failed validation",
        )
    return record


async def _resolve_uploaded(
    db: AsyncSession, req: UploadedMode,
) -> ResolvedInputs:
    """Adapter: wrap the existing prepare_spatial_inputs output as ResolvedInputs."""
    from backend.services.geo_processor import prepare_spatial_inputs

    file_ids = [req.concentrationFileId, req.populationFileId, req.boundaryFileId]
    if req.controlFileId:
        file_ids.append(req.controlFileId)

    result = await db.execute(select(FileUpload).where(FileUpload.id.in_(file_ids)))
    records = {r.id: r for r in result.scalars().all()}
    for fid in file_ids:
        if fid not in records:
            raise HTTPException(status_code=404, detail=f"File upload {fid} not found")

    spatial = prepare_spatial_inputs(
        concentration_raster_path=str(_resolve_file_path(records[req.concentrationFileId])),
        population_raster_path=str(_resolve_file_path(records[req.populationFileId])),
        boundary_path=str(_resolve_file_path(records[req.boundaryFileId])),
        control_raster_path=(
            str(_resolve_file_path(records[req.controlFileId]))
            if req.controlFileId else None
        ),
        control_value=req.controlConcentration,
    )

    from backend.services.resolver import Provenance, ResolvedInputs
    return ResolvedInputs(
        zone_ids=spatial["zone_ids"],
        zone_names=spatial["zone_names"],
        parent_ids=[None] * len(spatial["zone_ids"]),
        geometries=spatial["geometries"],
        c_baseline=spatial["c_baseline"],
        c_control=spatial["c_control"],
        population=spatial["population"],
        provenance=Provenance(
            concentration={"grain": "raster", "source": "uploaded"},
            population={"grain": "raster", "source": "uploaded"},
            incidence={"grain": "crf_default", "source": "crf_library"},
        ),
        warnings=[],
    )


def _run_spatial_compute_v2(
    resolved: ResolvedInputs,
    selected_crfs: list[dict],
    mc_iterations: int,
) -> dict:
    """Worker: same math as the original _run_spatial_compute but consumes ResolvedInputs.

    Returns a dict with ``zones`` (list of ZoneResult dicts) and
    ``aggregate_crf_results`` (list carrying private _cause/_endpointType keys).
    """
    import numpy as np
    from backend.services.hia_engine import (
        _beta_se, _compute_single_crf, _summarise, _summarise_spatial,
    )

    n_zones = len(resolved.zone_ids)
    rng = np.random.default_rng()
    per_100k = 100_000

    zones: list[dict] = [
        {
            "zoneId": resolved.zone_ids[i],
            "zoneName": resolved.zone_names[i],
            "parentId": resolved.parent_ids[i],
            "geometry": resolved.geometries[i],
            "baselineConcentration": float(resolved.c_baseline[i]),
            "controlConcentration": float(resolved.c_control[i]),
            "population": float(resolved.population[i]),
            "results": [],
        }
        for i in range(n_zones)
    ]

    crf_results_agg: list[dict] = []

    for crf in selected_crfs:
        se = _beta_se(crf["betaLow"], crf["betaHigh"])
        form = crf.get("functionalForm", "log-linear")
        y0 = crf.get("defaultRate") or 0.008
        betas = rng.normal(loc=crf["beta"], scale=se, size=mc_iterations)

        zone_cases = np.zeros((mc_iterations, n_zones))
        zone_paf = np.zeros((mc_iterations, n_zones))
        for zi in range(n_zones):
            cases_zi, paf_zi = _compute_single_crf(
                form, betas,
                float(resolved.c_baseline[zi]),
                float(resolved.c_control[zi]),
                y0, float(resolved.population[zi]),
                crf=crf,
            )
            zone_cases[:, zi] = cases_zi
            zone_paf[:, zi] = paf_zi

        cases_by_zone = _summarise_spatial(zone_cases)
        paf_by_zone = _summarise_spatial(zone_paf)
        pop_arr = resolved.population.copy()
        pop_arr[pop_arr == 0] = 1
        zone_rate = (zone_cases / pop_arr[np.newaxis, :]) * per_100k
        rate_by_zone = _summarise_spatial(zone_rate)

        for zi in range(n_zones):
            zones[zi]["results"].append({
                "crfId": crf["id"],
                "study": crf.get("source", ""),
                "endpoint": crf.get("endpoint", ""),
                "attributableCases": cases_by_zone[zi],
                "attributableFraction": paf_by_zone[zi],
                "attributableRate": rate_by_zone[zi],
            })

        total_cases_per_iter = zone_cases.sum(axis=1)
        total_pop = resolved.population.sum()

        crf_results_agg.append({
            "crfId": crf["id"],
            "study": crf.get("source", ""),
            "endpoint": crf.get("endpoint", ""),
            "attributableCases": _summarise(total_cases_per_iter),
            "attributableFraction": _summarise(
                total_cases_per_iter / (y0 * total_pop) if total_pop > 0
                else np.zeros(mc_iterations)
            ),
            "attributableRate": _summarise(
                (total_cases_per_iter / total_pop * per_100k) if total_pop > 0
                else np.zeros(mc_iterations)
            ),
            "_cause": crf.get("cause", "all_cause"),
            "_endpointType": crf.get("endpointType", "mortality"),
        })

    return {"zones": zones, "aggregate_crf_results": crf_results_agg}
```

- [ ] **Step 4: Run test to verify it passes**

Task 12 will provide `backend/services/results_cache.py`. Temporarily add a stub at the top of `compute.py`:

```python
# Temporary stub — replaced in Task 12
def save_result(result_id, response):
    pass
```

Then run: `cd backend && python -m pytest tests/test_compute_spatial.py -v`
Expected: both tests pass.

- [ ] **Step 5: Commit**

```bash
git add backend/routers/compute.py backend/tests/test_compute_spatial.py
git commit -m "feat(compute): wire resolver + rollups into /api/compute/spatial"
```

---

## Task 11: Remove the stale `CRFInput`/`CRFResult` `defaultRate` assumption when missing

**Files:**
- Modify: `backend/routers/compute.py`

- [ ] **Step 1: Confirm behavior in the new worker already handles this**

The new `_run_spatial_compute_v2` uses `y0 = crf.get("defaultRate") or 0.008` — safe fallback. The scalar `/api/compute` endpoint still uses `CRFInput` (v1). Leave that alone; no test change required.

- [ ] **Step 2: Commit**

No commit — verification-only task. Proceed to Task 12.

---

## Phase 4 — Downloads

## Task 12: Implement in-memory result cache

**Files:**
- Create: `backend/services/results_cache.py`
- Create: `backend/tests/test_results_cache.py`
- Modify: `backend/routers/compute.py` (replace stub import)
- Modify: `backend/requirements.txt` (add `cachetools`)

- [ ] **Step 1: Add `cachetools` to requirements**

```bash
echo "cachetools>=5.3" >> backend/requirements.txt
pip install cachetools
```

- [ ] **Step 2: Write the failing test**

Create `backend/tests/test_results_cache.py`:

```python
from backend.services.results_cache import save_result, load_result, ResultNotFound
import pytest


class FakeResponse:
    def __init__(self, x): self.x = x
    def model_dump(self): return {"x": self.x}


def test_save_and_load_roundtrip():
    save_result("abc123", FakeResponse(7))
    loaded = load_result("abc123")
    assert loaded == {"x": 7}


def test_load_missing_raises():
    with pytest.raises(ResultNotFound):
        load_result("nonexistent-id")
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd backend && python -m pytest tests/test_results_cache.py -v`
Expected: FAIL — module does not exist.

- [ ] **Step 4: Implement the cache**

Create `backend/services/results_cache.py`:

```python
"""In-memory TTL cache for compute results.

Results live for 1 hour; the Results page re-fetches on demand for
CSV / GeoJSON downloads. On cache miss, the caller should 410 Gone.
"""
from __future__ import annotations

from typing import Any

from cachetools import TTLCache

_cache: TTLCache = TTLCache(maxsize=32, ttl=3600)


class ResultNotFound(Exception):
    """Raised when a result UUID is not in the cache."""


def save_result(result_id: str, response: Any) -> None:
    """Store a response. Accepts any object with ``model_dump()``."""
    _cache[result_id] = response.model_dump()


def load_result(result_id: str) -> dict:
    """Retrieve a previously-saved result. Raises ResultNotFound if missing/expired."""
    if result_id not in _cache:
        raise ResultNotFound(result_id)
    return _cache[result_id]
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd backend && python -m pytest tests/test_results_cache.py -v`
Expected: both tests pass.

- [ ] **Step 6: Replace the stub in `compute.py`**

In `backend/routers/compute.py`, remove:
```python
# Temporary stub — replaced in Task 12
def save_result(result_id, response):
    pass
```
and keep only:
```python
from backend.services.results_cache import save_result
```

- [ ] **Step 7: Verify compute tests still pass**

Run: `cd backend && python -m pytest tests/test_compute_spatial.py tests/test_results_cache.py -v`
Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add backend/services/results_cache.py backend/tests/test_results_cache.py backend/routers/compute.py backend/requirements.txt
git commit -m "feat(results): add 1-hour in-memory result cache"
```

---

## Task 13: Implement CSV (long) and GeoJSON (wide) download endpoints

**Files:**
- Create: `backend/services/download_serializers.py`
- Create: `backend/tests/test_download_serializers.py`
- Modify: `backend/routers/compute.py` (add two GET routes)

- [ ] **Step 1: Write the failing test**

Create `backend/tests/test_download_serializers.py`:

```python
"""Tests for CSV long-format and GeoJSON wide-format serializers."""
from backend.services.download_serializers import (
    result_to_csv_long, result_to_geojson_wide,
)


def _sample_result() -> dict:
    return {
        "resultId": "abc123",
        "zones": [
            {
                "zoneId": "06001",
                "zoneName": "Alameda",
                "parentId": "06",
                "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]},
                "baselineConcentration": 11.4,
                "controlConcentration": 5.0,
                "population": 1_600_000,
                "results": [
                    {
                        "crfId": "epa_pm25_ihd_adult", "study": "Pope 2004",
                        "endpoint": "Ischemic heart disease",
                        "attributableCases": {"mean": 120, "lower95": 80, "upper95": 160},
                        "attributableFraction": {"mean": 0.03, "lower95": 0.02, "upper95": 0.04},
                        "attributableRate": {"mean": 7.5, "lower95": 5.0, "upper95": 10.0},
                    },
                ],
            },
        ],
        "causeRollups": [
            {
                "cause": "ihd", "endpointLabel": "Ischemic heart disease",
                "attributableCases": {"mean": 120, "lower95": 80, "upper95": 160},
                "attributableRate": {"mean": 7.5, "lower95": 5.0, "upper95": 10.0},
                "crfIds": ["epa_pm25_ihd_adult"],
            },
        ],
    }


def test_csv_long_has_one_row_per_polygon_crf():
    csv_str = result_to_csv_long(_sample_result(), crf_metadata={
        "epa_pm25_ihd_adult": {"cause": "ihd", "endpointType": "mortality"},
    })
    lines = csv_str.strip().splitlines()
    assert len(lines) == 2  # header + 1 row
    header = lines[0].split(",")
    assert "polygon_id" in header
    assert "cause" in header
    assert "attributable_cases_mean" in header
    assert "06001" in lines[1]


def test_geojson_wide_pivots_causes():
    gj = result_to_geojson_wide(_sample_result())
    assert gj["type"] == "FeatureCollection"
    assert len(gj["features"]) == 1
    props = gj["features"][0]["properties"]
    assert props["polygon_id"] == "06001"
    assert props["cases_ihd_mean"] == 120
    assert props["cases_ihd_lower95"] == 80
    assert props["rate_per_100k_ihd_mean"] == 7.5
    assert "geometry" not in props  # geometry goes in the Feature, not properties
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && python -m pytest tests/test_download_serializers.py -v`
Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement the serializers**

Create `backend/services/download_serializers.py`:

```python
"""Serializers for the Results download endpoints.

CSV  (long): one row per polygon × CRF.
GeoJSON (wide): one Feature per polygon, per-cause rollups pivoted to properties.
"""
from __future__ import annotations

import csv
import io
from typing import Any


CSV_HEADER = [
    "polygon_id", "polygon_name", "parent_id",
    "baseline_c", "control_c", "delta_c", "population",
    "crf_id", "crf_source", "cause", "endpoint_type", "endpoint",
    "attributable_cases_mean", "attributable_cases_lower95", "attributable_cases_upper95",
    "attributable_fraction_mean",
    "rate_per_100k_mean", "rate_per_100k_lower95", "rate_per_100k_upper95",
]


def result_to_csv_long(
    result: dict[str, Any],
    crf_metadata: dict[str, dict[str, str]],
) -> str:
    """Serialize a SpatialComputeResponseV2 to long-format CSV.

    ``crf_metadata`` maps CRF id → {cause, endpointType} so we can
    enrich each row without inferring from the endpoint string.
    """
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(CSV_HEADER)

    for zone in result["zones"]:
        base_c = zone["baselineConcentration"]
        ctrl_c = zone["controlConcentration"]
        delta = base_c - ctrl_c
        for crf_result in zone["results"]:
            meta = crf_metadata.get(crf_result["crfId"], {})
            w.writerow([
                zone["zoneId"],
                zone.get("zoneName") or "",
                zone.get("parentId") or "",
                base_c, ctrl_c, delta, zone["population"],
                crf_result["crfId"],
                crf_result.get("study", ""),
                meta.get("cause", ""),
                meta.get("endpointType", ""),
                crf_result.get("endpoint", ""),
                crf_result["attributableCases"]["mean"],
                crf_result["attributableCases"]["lower95"],
                crf_result["attributableCases"]["upper95"],
                crf_result["attributableFraction"]["mean"],
                crf_result["attributableRate"]["mean"],
                crf_result["attributableRate"]["lower95"],
                crf_result["attributableRate"]["upper95"],
            ])

    return buf.getvalue()


def result_to_geojson_wide(result: dict[str, Any]) -> dict[str, Any]:
    """Serialize to GeoJSON. Per-cause rollups become pivoted properties."""
    # Pre-index zone-level cause totals by summing CRF results grouped by cause.
    # The response's ``causeRollups`` is global; for per-polygon we have to
    # re-aggregate from zone["results"] using each CRF's cause.
    #
    # The downloader can't know each CRF's cause from the response alone —
    # but the CLI call includes a crfId-to-cause mapping baked into the
    # cause rollup's ``crfIds``. Build reverse index from that.
    crf_to_cause = {}
    for roll in result.get("causeRollups", []):
        for cid in roll.get("crfIds", []):
            crf_to_cause[cid] = roll["cause"]

    features = []
    for zone in result["zones"]:
        props = {
            "polygon_id": zone["zoneId"],
            "polygon_name": zone.get("zoneName"),
            "parent_id": zone.get("parentId"),
            "baseline_c": zone["baselineConcentration"],
            "control_c": zone["controlConcentration"],
            "delta_c": zone["baselineConcentration"] - zone["controlConcentration"],
            "population": zone["population"],
        }

        # Aggregate cases per cause within this zone
        by_cause: dict[str, dict[str, float]] = {}
        for cr in zone["results"]:
            cause = crf_to_cause.get(cr["crfId"], "unknown")
            bucket = by_cause.setdefault(cause, {
                "cases_mean": 0.0, "cases_lower95": 0.0, "cases_upper95": 0.0,
                "rate_mean": 0.0, "rate_lower95": 0.0, "rate_upper95": 0.0,
            })
            bucket["cases_mean"] += cr["attributableCases"]["mean"]
            bucket["cases_lower95"] += cr["attributableCases"]["lower95"]
            bucket["cases_upper95"] += cr["attributableCases"]["upper95"]
            bucket["rate_mean"] += cr["attributableRate"]["mean"]
            bucket["rate_lower95"] += cr["attributableRate"]["lower95"]
            bucket["rate_upper95"] += cr["attributableRate"]["upper95"]

        for cause, b in by_cause.items():
            props[f"cases_{cause}_mean"] = b["cases_mean"]
            props[f"cases_{cause}_lower95"] = b["cases_lower95"]
            props[f"cases_{cause}_upper95"] = b["cases_upper95"]
            props[f"rate_per_100k_{cause}_mean"] = b["rate_mean"]
            props[f"rate_per_100k_{cause}_lower95"] = b["rate_lower95"]
            props[f"rate_per_100k_{cause}_upper95"] = b["rate_upper95"]

        features.append({
            "type": "Feature",
            "geometry": zone["geometry"],
            "properties": props,
        })

    return {"type": "FeatureCollection", "features": features}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && python -m pytest tests/test_download_serializers.py -v`
Expected: both tests pass.

- [ ] **Step 5: Add the download routes**

Append to `backend/routers/compute.py`:

```python
from fastapi.responses import PlainTextResponse, JSONResponse

from backend.services.results_cache import load_result, ResultNotFound
from backend.services.download_serializers import (
    result_to_csv_long, result_to_geojson_wide,
)


@router.get("/compute/results/{result_id}/download")
async def download_result(
    result_id: str,
    format: str = "csv",
) -> Any:
    """CSV (long) or GeoJSON (wide) download of a cached result."""
    try:
        result = load_result(result_id)
    except ResultNotFound:
        raise HTTPException(
            status_code=410,
            detail="Result expired, please re-run the analysis.",
        )

    if format == "csv":
        # Build a CRF metadata lookup from causeRollups
        crf_metadata: dict[str, dict[str, str]] = {}
        for roll in result.get("causeRollups", []):
            for cid in roll["crfIds"]:
                crf_metadata[cid] = {
                    "cause": roll["cause"],
                    "endpointType": "",  # unknown after serialization loss
                }
        body = result_to_csv_long(result, crf_metadata)
        return PlainTextResponse(
            content=body, media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="hia-{result_id[:8]}.csv"'},
        )

    if format == "geojson":
        body = result_to_geojson_wide(result)
        return JSONResponse(
            content=body, media_type="application/geo+json",
            headers={"Content-Disposition": f'attachment; filename="hia-{result_id[:8]}.geojson"'},
        )

    raise HTTPException(status_code=400, detail=f"Unknown format: {format}")
```

- [ ] **Step 6: Run all backend tests**

Run: `cd backend && python -m pytest -q`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add backend/services/download_serializers.py backend/tests/test_download_serializers.py backend/routers/compute.py
git commit -m "feat(downloads): add CSV long + GeoJSON wide download endpoints"
```

---

## Phase 5 — Frontend store + wizard wiring

## Task 14: Extend the Zustand store with new results fields

**Files:**
- Modify: `frontend/src/stores/useAnalysisStore.js`

- [ ] **Step 1: Bump persistence version and widen results shape**

In `frontend/src/stores/useAnalysisStore.js`:

Change `version: 6` to `version: 7` and update the `migrate` function:

```js
migrate: (persisted, version) => {
  if (version < 7) return initialState()
  return persisted
},
```

Update `setResults` — no code change needed; the action already does a raw assign. But update the `DEFAULT_STEP6` for `spatialAggregation` usage by leaving it as-is; we'll rely on `step1.studyArea.analysisLevel`.

No new tests (persistence migration is covered by the store already).

- [ ] **Step 2: Commit**

```bash
git add frontend/src/stores/useAnalysisStore.js
git commit -m "chore(store): bump persistence version to 7 for new results shape"
```

---

## Task 15: Make Step 1's `analysisLevel` load-bearing

**Files:**
- Modify: `frontend/src/pages/steps/Step1StudyArea.jsx`
- Create: `frontend/src/pages/steps/__tests__/Step1StudyArea.test.jsx`

- [ ] **Step 1: Write the failing test**

Create `frontend/src/pages/steps/__tests__/Step1StudyArea.test.jsx`:

```jsx
import { describe, it, expect, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import Step1StudyArea from '../Step1StudyArea'
import useAnalysisStore from '../../../stores/useAnalysisStore'


describe('Step1StudyArea', () => {
  beforeEach(() => useAnalysisStore.getState().reset())

  it('defaults analysisLevel to "state" when USA selected', () => {
    render(<Step1StudyArea />)
    fireEvent.change(screen.getByRole('combobox', { name: /country/i }), {
      target: { value: 'USA' },
    })
    const state = useAnalysisStore.getState().step1
    expect(state.studyArea.id).toBe('USA')
    expect(state.studyArea.analysisLevel).toBe('state')
  })

  it('analysisLevel radio updates state and persists to store', () => {
    render(<Step1StudyArea />)
    fireEvent.change(screen.getByRole('combobox', { name: /country/i }), {
      target: { value: 'USA' },
    })
    fireEvent.change(screen.getAllByRole('combobox')[1], {
      target: { value: '06' },
    })
    fireEvent.click(screen.getByLabelText(/census tract level/i))
    expect(useAnalysisStore.getState().step1.studyArea.analysisLevel).toBe('tract')
  })
})
```

Add a label to the country `<select>` so it's accessible:

```jsx
<label htmlFor="country-select" className="sr-only">Country</label>
<select
  id="country-select"
  aria-label="Country"
  value={studyArea.id}
  ...
>
```

- [ ] **Step 2: Run test to verify it fails or is incomplete**

Run: `cd frontend && npm test -- --run src/pages/steps/__tests__/Step1StudyArea.test.jsx`
Expected: either failure or partial — before editing the component, the radio may not be fully wired.

- [ ] **Step 3: Update the component to default non-US countries to "country"**

In `Step1StudyArea.jsx`, update `handleCountryChange` to set an `analysisLevel` default for every country:

```jsx
const handleCountryChange = useCallback((iso) => {
  const country = countries.find((c) => c.iso === iso)
  if (!country) {
    setStep1({ studyArea: { type: 'country', id: '', name: '', geometry: null, analysisLevel: 'country' } })
    return
  }
  setStep1({
    studyArea: {
      type: 'country',
      id: country.iso,
      name: country.name,
      geometry: null,
      analysisLevel: country.iso === 'USA' ? 'state' : 'country',
      ...(country.iso === 'USA' ? { stateId: '', stateName: '' } : {}),
    },
  })
}, [setStep1])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd frontend && npm test -- --run src/pages/steps/__tests__/Step1StudyArea.test.jsx`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/pages/steps/Step1StudyArea.jsx frontend/src/pages/steps/__tests__/Step1StudyArea.test.jsx
git commit -m "feat(step1): make analysisLevel load-bearing with country default"
```

---

## Task 16: Remove the scalar-collapse in Step 2's built-in concentration loader

**Files:**
- Modify: `frontend/src/pages/steps/Step2AirQuality.jsx`

- [ ] **Step 1: Inline-edit the built-in loader**

In `Step2AirQuality.jsx`, inside `BuiltinConcentrationLoader`'s data-fetch `useEffect` (lines ~264–290):

Replace the block that computes and stores a single mean with a preview-only implementation:

```jsx
useEffect(() => {
  if (!selectedDatasetId || !pollutant || !country || !selectedYear) return

  setLoading(true)
  setError(null)
  setGeojsonPreview(null)

  fetchConcentration(pollutant, country, selectedYear)
    .then((geojson) => {
      if (!geojson) {
        setError(`Built-in data not available for ${studyArea?.name || country} in ${selectedYear}.`)
        return
      }
      setGeojsonPreview(geojson)
      // Preview ONLY — no scalar collapse. Backend resolver re-fetches
      // per-polygon at compute time.
      const features = geojson.features || []
      const concentrations = features
        .map((f) => f.properties?.mean_pm25 ?? f.properties?.mean ?? f.properties?.concentration)
        .filter((v) => v != null)
      const previewMean = concentrations.length > 0
        ? Math.round((concentrations.reduce((a, b) => a + b, 0) / concentrations.length) * 100) / 100
        : null
      onDataLoaded(previewMean, geojson)  // previewMean shown in UI only
    })
    .catch((err) => setError(err.message))
    .finally(() => setLoading(false))
}, [selectedDatasetId, pollutant, country, selectedYear]) // eslint-disable-line react-hooks/exhaustive-deps
```

No test added — Step 2's existing behavior (render the preview with a scalar displayed) is unchanged. The backend now ignores `baseline.value` for spatial runs.

- [ ] **Step 2: Commit**

```bash
git add frontend/src/pages/steps/Step2AirQuality.jsx
git commit -m "refactor(step2): keep scalar preview but rely on backend for per-polygon compute"
```

---

## Task 17: Rewrite Step 6's routing rule and payload builder

**Files:**
- Modify: `frontend/src/pages/steps/Step6Run.jsx`
- Modify: `frontend/src/lib/api.js`

- [ ] **Step 1: Update the API client**

In `frontend/src/lib/api.js`, ensure `runSpatialCompute` accepts the union-shaped body unchanged. If it currently serializes specific fields, switch to passing the body through:

```js
export async function runSpatialCompute(payload) {
  const res = await fetch('/api/compute/spatial', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  if (!res.ok) {
    const detail = await res.text()
    throw new Error(`Spatial compute failed: ${res.status} ${detail}`)
  }
  return res.json()
}
```

- [ ] **Step 2: Update Step 6's `handleRunAnalysis`**

Replace the `handleRunAnalysis` body in `Step6Run.jsx` with:

```jsx
const handleRunAnalysis = useCallback(async () => {
  setRunning(true)
  setError(null)

  const analysisLevel = step1.studyArea?.analysisLevel || 'country'
  const hasUploadedRasters = Boolean(
    step2.baseline?.uploadId && step3.uploadId && step1.studyArea?.boundaryUploadId
  )
  const hasBoundaryUpload = Boolean(step1.studyArea?.boundaryUploadId)
  const isBuiltinDataset = step2.baseline?.type === 'dataset'

  const needsSpatial =
    analysisLevel !== 'country' || hasBoundaryUpload || isBuiltinDataset || hasUploadedRasters

  try {
    if (needsSpatial) {
      let payload
      if (hasUploadedRasters) {
        payload = {
          mode: 'uploaded',
          concentrationFileId: step2.baseline.uploadId,
          controlFileId: step2.control?.uploadId || null,
          controlConcentration: step2.control?.value ?? null,
          populationFileId: step3.uploadId,
          boundaryFileId: step1.studyArea.boundaryUploadId,
          selectedCRFs: selectedCRFDetails.map(_crfPayload),
          monteCarloIterations: step6.monteCarloIterations || 1000,
        }
      } else if (hasBoundaryUpload) {
        payload = {
          mode: 'builtin_custom_boundary',
          pollutant: step1.pollutant,
          country: _countrySlug(step1.studyArea.id),
          year: step2.baseline.year,
          boundaryFileId: step1.studyArea.boundaryUploadId,
          ..._controlFields(step2.control),
          selectedCRFs: selectedCRFDetails.map(_crfPayload),
          monteCarloIterations: step6.monteCarloIterations || 1000,
        }
      } else {
        payload = {
          mode: 'builtin',
          pollutant: step1.pollutant,
          country: _countrySlug(step1.studyArea.id),
          year: step2.baseline.year,
          analysisLevel,
          stateFilter: step1.studyArea.stateId || null,
          countyFilter: null,
          ..._controlFields(step2.control),
          selectedCRFs: selectedCRFDetails.map(_crfPayload),
          monteCarloIterations: step6.monteCarloIterations || 1000,
        }
      }
      const results = await runSpatialCompute(payload)
      setResults(results)
    } else {
      // Scalar pathway: client-side engine (unchanged)
      const config = {
        pollutant: step1.pollutant,
        baselineConcentration: step2.baseline?.value,
        controlConcentration: step2.control?.value ?? step2.baseline?.value,
        population: step3.totalPopulation,
        ageGroups: step3.ageGroups,
        selectedCRFs: selectedCRFDetails,
        incidenceRates: step4.rates,
        poolingMethod: step6.poolingMethod,
        monteCarloIterations: step6.monteCarloIterations,
      }
      const raw = await Promise.resolve(computeHIA(config))
      const detail = raw.results.map((r) => ({
        crfStudy: r.study,
        framework: crfLookup[r.crfId]?.framework || '',
        endpoint: r.endpoint,
        attributableCases: r.attributableCases.mean,
        lower95: r.attributableCases.lower95,
        upper95: r.attributableCases.upper95,
        attributableFraction: r.attributableFraction.mean,
        ratePer100k: r.attributableRate.mean,
      }))
      const totalPop = step3.totalPopulation || 1
      const avgFraction = detail.length > 0
        ? detail.reduce((s, d) => s + (d.attributableFraction || 0), 0) / detail.length : 0
      const avgRate = detail.length > 0
        ? detail.reduce((s, d) => s + (d.ratePer100k || 0), 0) / detail.length : 0
      setResults({
        meta: { analysisName: step1.analysisName || '' },
        summary: {
          totalDeaths: raw.totalDeaths,
          attributableFraction: avgFraction,
          attributableRate: avgRate,
        },
        detail,
      })
    }
    navigate('/analysis/results')
  } catch (err) {
    setError(err.message || 'Analysis failed. Please check your inputs.')
  } finally {
    setRunning(false)
  }
}, [step1, step2, step3, step4, step5, step6, selectedCRFDetails, setResults, navigate, crfLookup])


// Helpers (add above handleRunAnalysis or at module scope)
function _crfPayload(crf) {
  return {
    id: crf.id, source: crf.source, endpoint: crf.endpoint,
    beta: crf.beta, betaLow: crf.betaLow, betaHigh: crf.betaHigh,
    functionalForm: crf.functionalForm, defaultRate: crf.defaultRate,
    cause: crf.cause, endpointType: crf.endpointType,
  }
}

function _countrySlug(iso) {
  if (!iso) return ''
  return iso === 'USA' ? 'us' : iso.toLowerCase()
}

function _controlFields(control) {
  if (!control || control.type === 'none') {
    return { controlMode: 'scalar', controlConcentration: 0.0 }
  }
  if (control.type === 'rollback') {
    return { controlMode: 'rollback', controlRollbackPercent: control.rollbackPercent }
  }
  if (control.type === 'benchmark' || control.type === 'manual') {
    return { controlMode: control.type === 'benchmark' ? 'benchmark' : 'scalar', controlConcentration: control.value }
  }
  return { controlMode: 'scalar', controlConcentration: control.value }
}
```

- [ ] **Step 3: Remove `hasSpatialInputs` — now dead code**

Delete the `hasSpatialInputs` const. Update the blue "Spatial mode" banner to check `needsSpatial` from the component scope — compute it in a `useMemo`:

```jsx
const needsSpatial = useMemo(() => {
  const level = step1.studyArea?.analysisLevel || 'country'
  return (
    level !== 'country'
    || Boolean(step1.studyArea?.boundaryUploadId)
    || step2.baseline?.type === 'dataset'
    || Boolean(step2.baseline?.uploadId && step3.uploadId && step1.studyArea?.boundaryUploadId)
  )
}, [step1.studyArea?.analysisLevel, step1.studyArea?.boundaryUploadId, step2.baseline, step3.uploadId])

// Replace hasSpatialInputs references with needsSpatial
```

- [ ] **Step 4: Manually verify**

Run the app: `cd frontend && npm run dev`. Walk through a USA → CA → tract → PM2.5 dataset → IHD CRF path. Submit. Confirm DevTools Network tab shows a POST to `/api/compute/spatial` with `mode: "builtin"` and a non-empty `zones[]` in the response.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/pages/steps/Step6Run.jsx frontend/src/lib/api.js
git commit -m "feat(step6): route to /api/compute/spatial for all spatial runs"
```

---

## Phase 6 — Results page (choropleth, per-polygon table, downloads)

## Task 18: Build the `ChoroplethMap` component

**Files:**
- Create: `frontend/src/components/ChoroplethMap.jsx`
- Create: `frontend/src/components/__tests__/ChoroplethMap.test.jsx`

- [ ] **Step 1: Write the failing test**

Create `frontend/src/components/__tests__/ChoroplethMap.test.jsx`:

```jsx
import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import ChoroplethMap from '../ChoroplethMap'

vi.mock('mapbox-gl', () => ({
  default: {
    accessToken: '',
    Map: vi.fn().mockImplementation(() => ({
      on: vi.fn(),
      addControl: vi.fn(),
      addSource: vi.fn(),
      addLayer: vi.fn(),
      setFilter: vi.fn(),
      setPaintProperty: vi.fn(),
      remove: vi.fn(),
      fitBounds: vi.fn(),
    })),
    NavigationControl: vi.fn(),
  },
}))


const sampleZones = [
  {
    zoneId: '06001', zoneName: 'Alameda', parentId: '06',
    geometry: { type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]] },
    baselineConcentration: 11.4, controlConcentration: 5.0, population: 1_600_000,
    results: [{
      crfId: 'epa_pm25_ihd_adult', study: 'Pope 2004', endpoint: 'IHD',
      attributableCases: { mean: 120, lower95: 80, upper95: 160 },
      attributableFraction: { mean: 0.03, lower95: 0.02, upper95: 0.04 },
      attributableRate: { mean: 7.5, lower95: 5.0, upper95: 10.0 },
    }],
  },
]

const sampleRollups = [{
  cause: 'ihd', endpointLabel: 'Ischemic heart disease',
  attributableCases: { mean: 120, lower95: 80, upper95: 160 },
  attributableRate: { mean: 7.5, lower95: 5.0, upper95: 10.0 },
  crfIds: ['epa_pm25_ihd_adult'],
}]


describe('ChoroplethMap', () => {
  it('renders cause and metric dropdowns', () => {
    render(<ChoroplethMap zones={sampleZones} causeRollups={sampleRollups} />)
    expect(screen.getByLabelText(/cause/i)).toBeInTheDocument()
    expect(screen.getByLabelText(/metric/i)).toBeInTheDocument()
  })

  it('includes "Sum of cause-specific" and each rollup cause as options', () => {
    render(<ChoroplethMap zones={sampleZones} causeRollups={sampleRollups} />)
    const causeSelect = screen.getByLabelText(/cause/i)
    expect(causeSelect).toHaveTextContent(/sum of cause-specific/i)
    expect(causeSelect).toHaveTextContent(/ischemic heart disease/i)
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd frontend && npm test -- --run src/components/__tests__/ChoroplethMap.test.jsx`
Expected: FAIL — `ChoroplethMap` does not exist.

- [ ] **Step 3: Implement `ChoroplethMap`**

Create `frontend/src/components/ChoroplethMap.jsx`:

```jsx
import { useEffect, useMemo, useRef, useState } from 'react'

const MAPBOX_TOKEN = import.meta.env.VITE_MAPBOX_TOKEN
const VIRIDIS = ['#440154', '#482878', '#3e4989', '#31688e', '#26828e', '#1f9e89', '#35b779', '#6ece58', '#b5de2b', '#fde725']

function quantileBreaks(values, n = 7) {
  const sorted = [...values].filter((v) => v != null && !Number.isNaN(v)).sort((a, b) => a - b)
  if (sorted.length === 0) return [0]
  const breaks = []
  for (let i = 1; i < n; i++) {
    const q = (i / n) * (sorted.length - 1)
    breaks.push(sorted[Math.floor(q)])
  }
  return breaks
}

function buildMapboxPaintExpression(breaks) {
  const expr = ['interpolate', ['linear'], ['get', 'value']]
  breaks.forEach((b, i) => {
    expr.push(b, VIRIDIS[i])
  })
  return expr
}

/**
 * Choropleth map of per-polygon HIA results. Colors the value the user
 * picks (cases or rate/100k) for the cause group they pick from the
 * cause dropdown.
 */
export default function ChoroplethMap({ zones, causeRollups }) {
  const containerRef = useRef(null)
  const mapRef = useRef(null)
  const [cause, setCause] = useState('__sum__')
  const [metric, setMetric] = useState('cases')

  // Build crfId → cause lookup from rollups
  const crfToCause = useMemo(() => {
    const m = {}
    for (const r of causeRollups || []) {
      for (const cid of r.crfIds || []) m[cid] = r.cause
    }
    return m
  }, [causeRollups])

  // Compute per-zone value for current cause + metric
  const values = useMemo(() => {
    return (zones || []).map((z) => {
      let cases = 0
      let rate = 0
      for (const cr of z.results || []) {
        const crfCause = crfToCause[cr.crfId] || 'unknown'
        const include = cause === '__sum__'
          ? crfCause !== 'all_cause'
          : crfCause === cause
        if (!include) continue
        cases += cr.attributableCases?.mean || 0
        rate += cr.attributableRate?.mean || 0
      }
      return { zoneId: z.zoneId, value: metric === 'cases' ? cases : rate }
    })
  }, [zones, cause, metric, crfToCause])

  // Build GeoJSON source once per zones change
  const geojsonSource = useMemo(() => {
    const valueById = Object.fromEntries(values.map((v) => [v.zoneId, v.value]))
    return {
      type: 'FeatureCollection',
      features: (zones || []).map((z) => ({
        type: 'Feature',
        geometry: z.geometry,
        properties: {
          zoneId: z.zoneId,
          zoneName: z.zoneName,
          value: valueById[z.zoneId] ?? 0,
        },
      })),
    }
  }, [zones, values])

  // Initialize map
  useEffect(() => {
    if (!MAPBOX_TOKEN || mapRef.current) return
    let cancelled = false
    import('mapbox-gl').then((mapboxgl) => {
      if (cancelled || !containerRef.current) return
      mapboxgl.default.accessToken = MAPBOX_TOKEN
      const map = new mapboxgl.default.Map({
        container: containerRef.current,
        style: 'mapbox://styles/mapbox/light-v11',
        center: [-98, 39], zoom: 3, projection: 'mercator',
      })
      map.addControl(new mapboxgl.default.NavigationControl(), 'top-right')
      map.on('load', () => {
        map.addSource('zones', { type: 'geojson', data: geojsonSource })
        const breaks = quantileBreaks(values.map((v) => v.value))
        map.addLayer({
          id: 'zones-fill', type: 'fill', source: 'zones',
          paint: {
            'fill-color': buildMapboxPaintExpression(breaks),
            'fill-opacity': 0.75,
            'fill-outline-color': '#ffffff',
          },
        })
      })
      mapRef.current = map
    })
    return () => { cancelled = true; mapRef.current?.remove(); mapRef.current = null }
  }, []) // eslint-disable-line

  // Update source + paint when cause/metric changes
  useEffect(() => {
    const map = mapRef.current
    if (!map || !map.getSource) return
    const src = map.getSource?.('zones')
    if (src && src.setData) src.setData(geojsonSource)
    const breaks = quantileBreaks(values.map((v) => v.value))
    map.setPaintProperty?.('zones-fill', 'fill-color', buildMapboxPaintExpression(breaks))
  }, [geojsonSource, values])

  const causeOptions = useMemo(() => {
    const opts = [{ value: '__sum__', label: 'Sum of cause-specific' }]
    for (const r of causeRollups || []) {
      opts.push({ value: r.cause, label: r.endpointLabel })
    }
    return opts
  }, [causeRollups])

  if (!MAPBOX_TOKEN) {
    return (
      <div className="rounded-xl bg-gray-100 p-6 text-sm text-gray-500 text-center">
        Set VITE_MAPBOX_TOKEN in your .env to enable the choropleth map.
      </div>
    )
  }

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
      <div className="flex flex-wrap items-center gap-4 p-4 border-b border-gray-100">
        <label className="flex items-center gap-2 text-sm">
          <span className="text-gray-600">Cause:</span>
          <select
            aria-label="Cause"
            value={cause}
            onChange={(e) => setCause(e.target.value)}
            className="rounded-md border border-gray-300 px-2 py-1 text-sm"
          >
            {causeOptions.map((o) => (
              <option key={o.value} value={o.value}>{o.label}</option>
            ))}
          </select>
        </label>
        <label className="flex items-center gap-2 text-sm">
          <span className="text-gray-600">Metric:</span>
          <select
            aria-label="Metric"
            value={metric}
            onChange={(e) => setMetric(e.target.value)}
            className="rounded-md border border-gray-300 px-2 py-1 text-sm"
          >
            <option value="cases">Attributable cases</option>
            <option value="rate">Rate per 100k</option>
          </select>
        </label>
      </div>
      <div ref={containerRef} style={{ height: 480 }} />
    </div>
  )
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd frontend && npm test -- --run src/components/__tests__/ChoroplethMap.test.jsx`
Expected: both tests pass.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/ChoroplethMap.jsx frontend/src/components/__tests__/ChoroplethMap.test.jsx
git commit -m "feat(results): add ChoroplethMap with toggleable cause and metric"
```

---

## Task 19: Build the `PerPolygonTable` component

**Files:**
- Create: `frontend/src/components/PerPolygonTable.jsx`
- Create: `frontend/src/components/__tests__/PerPolygonTable.test.jsx`

- [ ] **Step 1: Write the failing test**

Create `frontend/src/components/__tests__/PerPolygonTable.test.jsx`:

```jsx
import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import PerPolygonTable from '../PerPolygonTable'

const zones = [
  {
    zoneId: 'A', zoneName: 'Alpha', parentId: 'X',
    baselineConcentration: 10, controlConcentration: 5, population: 1000,
    results: [{ crfId: 'x', attributableCases: { mean: 50, lower95: 40, upper95: 60 }, attributableRate: { mean: 5, lower95: 4, upper95: 6 } }],
  },
  {
    zoneId: 'B', zoneName: 'Beta', parentId: 'X',
    baselineConcentration: 20, controlConcentration: 5, population: 500,
    results: [{ crfId: 'x', attributableCases: { mean: 30, lower95: 20, upper95: 40 }, attributableRate: { mean: 6, lower95: 4, upper95: 8 } }],
  },
]
const rollups = [{ cause: 'ihd', endpointLabel: 'IHD', crfIds: ['x'], attributableCases: { mean: 80, lower95: 60, upper95: 100 }, attributableRate: { mean: 5.5, lower95: 4, upper95: 7 } }]


describe('PerPolygonTable', () => {
  it('renders a row per zone', () => {
    render(<PerPolygonTable zones={zones} causeRollups={rollups} />)
    expect(screen.getByText('Alpha')).toBeInTheDocument()
    expect(screen.getByText('Beta')).toBeInTheDocument()
  })

  it('sorts by attributable cases when the column header is clicked', () => {
    render(<PerPolygonTable zones={zones} causeRollups={rollups} />)
    fireEvent.click(screen.getByRole('button', { name: /attributable cases/i }))
    const rows = screen.getAllByRole('row')
    // Row 0 is header. Row 1 should be the higher-cases zone.
    // Click toggles ascending vs descending; pick the expected polarity.
    expect(rows[1]).toHaveTextContent(/Alpha/)  // 50 > 30 desc
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd frontend && npm test -- --run src/components/__tests__/PerPolygonTable.test.jsx`
Expected: FAIL — component does not exist.

- [ ] **Step 3: Implement `PerPolygonTable`**

Create `frontend/src/components/PerPolygonTable.jsx`:

```jsx
import { useMemo, useState } from 'react'

function fmt(n, d = 1) {
  if (n == null) return '—'
  return Number(n).toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d })
}

export default function PerPolygonTable({ zones, causeRollups }) {
  const [sortKey, setSortKey] = useState('cases')
  const [sortDesc, setSortDesc] = useState(true)
  const [page, setPage] = useState(0)
  const PAGE_SIZE = 50

  const crfToCause = useMemo(() => {
    const m = {}
    for (const r of causeRollups || []) for (const cid of r.crfIds || []) m[cid] = r.cause
    return m
  }, [causeRollups])

  const rows = useMemo(() => {
    return (zones || []).map((z) => {
      let cases = 0, rate = 0
      for (const cr of z.results || []) {
        if (crfToCause[cr.crfId] === 'all_cause') continue
        cases += cr.attributableCases?.mean || 0
        rate += cr.attributableRate?.mean || 0
      }
      return {
        zoneId: z.zoneId,
        zoneName: z.zoneName || z.zoneId,
        baseline: z.baselineConcentration,
        control: z.controlConcentration,
        delta: z.baselineConcentration - z.controlConcentration,
        population: z.population,
        cases, rate,
      }
    })
  }, [zones, crfToCause])

  const sorted = useMemo(() => {
    const out = [...rows]
    out.sort((a, b) => {
      const diff = (a[sortKey] ?? 0) - (b[sortKey] ?? 0)
      return sortDesc ? -diff : diff
    })
    return out
  }, [rows, sortKey, sortDesc])

  const paged = sorted.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)
  const totalPages = Math.max(1, Math.ceil(sorted.length / PAGE_SIZE))

  function toggleSort(key) {
    if (sortKey === key) setSortDesc(!sortDesc)
    else { setSortKey(key); setSortDesc(true) }
  }

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 text-left text-xs uppercase tracking-wide text-gray-500">
            <tr>
              <th className="px-4 py-2">Polygon</th>
              <th className="px-4 py-2"><button onClick={() => toggleSort('baseline')}>Baseline C</button></th>
              <th className="px-4 py-2"><button onClick={() => toggleSort('control')}>Control C</button></th>
              <th className="px-4 py-2"><button onClick={() => toggleSort('delta')}>ΔC</button></th>
              <th className="px-4 py-2"><button onClick={() => toggleSort('population')}>Population</button></th>
              <th className="px-4 py-2"><button onClick={() => toggleSort('cases')}>Attributable cases</button></th>
              <th className="px-4 py-2"><button onClick={() => toggleSort('rate')}>Rate per 100k</button></th>
            </tr>
          </thead>
          <tbody>
            {paged.map((r) => (
              <tr key={r.zoneId} className="border-t border-gray-100">
                <td className="px-4 py-2 font-medium">{r.zoneName}</td>
                <td className="px-4 py-2">{fmt(r.baseline, 1)}</td>
                <td className="px-4 py-2">{fmt(r.control, 1)}</td>
                <td className="px-4 py-2">{fmt(r.delta, 1)}</td>
                <td className="px-4 py-2">{fmt(r.population, 0)}</td>
                <td className="px-4 py-2">{fmt(r.cases, 1)}</td>
                <td className="px-4 py-2">{fmt(r.rate, 2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {totalPages > 1 && (
        <div className="flex items-center justify-between p-3 border-t border-gray-100 text-sm">
          <span>Page {page + 1} of {totalPages}</span>
          <div className="flex gap-2">
            <button disabled={page === 0} onClick={() => setPage(page - 1)} className="px-3 py-1 rounded-md border disabled:opacity-50">Prev</button>
            <button disabled={page + 1 >= totalPages} onClick={() => setPage(page + 1)} className="px-3 py-1 rounded-md border disabled:opacity-50">Next</button>
          </div>
        </div>
      )}
    </div>
  )
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd frontend && npm test -- --run src/components/__tests__/PerPolygonTable.test.jsx`
Expected: both tests pass.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/PerPolygonTable.jsx frontend/src/components/__tests__/PerPolygonTable.test.jsx
git commit -m "feat(results): add PerPolygonTable with sort and pagination"
```

---

## Task 20: Wire the new components into the Results page

**Files:**
- Modify: `frontend/src/pages/Results.jsx`

- [ ] **Step 1: Render the provenance bar, cause cards, map, table, and downloads**

At the top of `Results.jsx` add imports:

```jsx
import ChoroplethMap from '../components/ChoroplethMap'
import PerPolygonTable from '../components/PerPolygonTable'
```

Extend the page body (below the existing hero aggregate block) with:

```jsx
// Inside Results component, after existing hero block
const spatial = Boolean(results?.zones?.length)

{spatial && results.provenance && (
  <div className="flex flex-wrap gap-x-4 gap-y-1 px-4 py-2 text-xs bg-gray-50 border border-gray-200 rounded-md mb-4">
    <span><span className="text-gray-500">Concentration:</span> {results.provenance.concentration.grain}{results.provenance.concentration.broadcast_to ? ` (broadcast to ${results.provenance.concentration.broadcast_to})` : ''} ({results.provenance.concentration.source})</span>
    <span><span className="text-gray-500">Population:</span> {results.provenance.population.grain} ({results.provenance.population.source})</span>
    <span><span className="text-gray-500">Incidence:</span> {results.provenance.incidence.grain} ({results.provenance.incidence.source})</span>
  </div>
)}

{spatial && (results.warnings || []).length > 0 && (
  <div className="bg-amber-50 border border-amber-200 text-amber-800 rounded-md p-3 mb-4 text-sm">
    <p className="font-medium mb-1">Warnings</p>
    <ul className="list-disc list-inside space-y-1">
      {results.warnings.map((w, i) => <li key={i}>{w}</li>)}
    </ul>
  </div>
)}

{spatial && results.allCauseDeaths && (
  <div className="bg-blue-50 border border-blue-200 text-blue-800 rounded-md p-3 mb-4 text-sm">
    All-cause mortality (separate): <strong>{Number(results.allCauseDeaths.mean).toFixed(0)}</strong> deaths — shown separately because it would double-count if summed with cause-specific totals.
  </div>
)}

{spatial && (results.causeRollups || []).length > 0 && (
  <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-3 mb-6">
    {results.causeRollups.map((r) => (
      <div key={r.cause} className="bg-white rounded-xl border border-gray-200 p-4">
        <p className="text-xs uppercase tracking-wide text-gray-500">{r.endpointLabel}</p>
        <p className="text-2xl font-semibold mt-1">{Number(r.attributableCases.mean).toFixed(0)}</p>
        <p className="text-xs text-gray-500">95% CI {Number(r.attributableCases.lower95).toFixed(0)}–{Number(r.attributableCases.upper95).toFixed(0)}</p>
        <p className="text-xs text-gray-400 mt-2">{r.crfIds.length} CRF{r.crfIds.length === 1 ? '' : 's'}</p>
      </div>
    ))}
  </div>
)}

{spatial && (
  <>
    <h2 className="text-lg font-semibold mb-2">Per-polygon choropleth</h2>
    <div className="mb-6">
      <ChoroplethMap zones={results.zones} causeRollups={results.causeRollups} />
    </div>

    <h2 className="text-lg font-semibold mb-2">Per-polygon results</h2>
    <div className="mb-6">
      <PerPolygonTable zones={results.zones} causeRollups={results.causeRollups} />
    </div>

    <div className="flex gap-3 mb-6">
      <a
        href={`/api/compute/results/${results.resultId}/download?format=csv`}
        className="px-4 py-2 rounded-md border text-sm font-medium hover:bg-gray-50"
      >Download CSV (long)</a>
      <a
        href={`/api/compute/results/${results.resultId}/download?format=geojson`}
        className="px-4 py-2 rounded-md border text-sm font-medium hover:bg-gray-50"
      >Download GeoJSON (wide)</a>
    </div>
  </>
)}
```

- [ ] **Step 2: Manually verify end-to-end**

Start backend + frontend. Walk USA → CA → tract → PM2.5 → 2022 → IHD CRF → submit. Confirm:
- Choropleth renders
- Per-polygon table has >= 9,000 rows and paginates
- CSV download opens cleanly in Excel
- GeoJSON download loads in geojson.io or QGIS

- [ ] **Step 3: Commit**

```bash
git add frontend/src/pages/Results.jsx
git commit -m "feat(results): add provenance bar, cause cards, choropleth, polygon table, downloads"
```

---

## Phase 7 — Finalization

## Task 21: Add an end-to-end integration test

**Files:**
- Create: `backend/tests/test_compute_spatial_e2e.py`

- [ ] **Step 1: Write the test**

Create `backend/tests/test_compute_spatial_e2e.py`:

```python
"""End-to-end: built-in compute → download CSV/GeoJSON."""
import pandas as pd
import pytest
from pathlib import Path
from fastapi.testclient import TestClient

from backend.main import app


def _setup(tmp_path: Path):
    df = pd.DataFrame({
        "geoid": ["06001000100", "06001000200"],
        "state_fips": ["06", "06"], "county_fips": ["001", "001"],
        "total_pop": [3500, 4200],
        "geometry": ["POLYGON((0 0,1 0,1 1,0 1,0 0))"] * 2,
    })
    p = tmp_path / "processed" / "demographics" / "us"
    p.mkdir(parents=True)
    df.to_parquet(p / "2022.parquet")

    aqs = pd.DataFrame({"admin_id": ["US-06"], "mean_pm25": [11.4]})
    a = tmp_path / "processed" / "epa_aqs" / "pm25" / "ne_states"
    a.mkdir(parents=True)
    aqs.to_parquet(a / "2022.parquet")


def test_compute_then_download_csv_and_geojson(tmp_path, monkeypatch):
    _setup(tmp_path)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "processed"))
    client = TestClient(app)

    compute = client.post("/api/compute/spatial", json={
        "mode": "builtin",
        "pollutant": "pm25", "country": "us", "year": 2022,
        "analysisLevel": "tract", "stateFilter": "06",
        "controlMode": "benchmark", "controlConcentration": 5.0,
        "selectedCRFs": [{
            "id": "epa_pm25_ihd_adult", "source": "Pope 2004",
            "endpoint": "IHD",
            "beta": 0.015, "betaLow": 0.01, "betaHigh": 0.02,
            "functionalForm": "log-linear", "defaultRate": 0.0025,
            "cause": "ihd", "endpointType": "mortality",
        }],
        "monteCarloIterations": 100,
    })
    assert compute.status_code == 200, compute.text
    result_id = compute.json()["resultId"]

    csv_r = client.get(f"/api/compute/results/{result_id}/download?format=csv")
    assert csv_r.status_code == 200
    assert csv_r.headers["content-type"].startswith("text/csv")
    assert "polygon_id,polygon_name" in csv_r.text

    gj_r = client.get(f"/api/compute/results/{result_id}/download?format=geojson")
    assert gj_r.status_code == 200
    body = gj_r.json()
    assert body["type"] == "FeatureCollection"
    assert "cases_ihd_mean" in body["features"][0]["properties"]


def test_download_410_on_unknown_id():
    client = TestClient(app)
    r = client.get("/api/compute/results/does-not-exist/download?format=csv")
    assert r.status_code == 410
```

- [ ] **Step 2: Run test**

Run: `cd backend && python -m pytest tests/test_compute_spatial_e2e.py -v`
Expected: all pass.

- [ ] **Step 3: Commit**

```bash
git add backend/tests/test_compute_spatial_e2e.py
git commit -m "test(e2e): compute → download CSV and GeoJSON round-trip"
```

---

## Task 22: Update System Map note in Obsidian

**Files:**
- Modify: `C:/Users/vsoutherland/Documents/obsidian_vault/System Map/HIA Walkthrough.md`

- [ ] **Step 1: Edit the System Map note**

Add a new dated entry under `## Current State`:

```markdown
**2026-04-21 — Polygon-based results.** `/api/compute/spatial` now accepts three modes (`builtin`, `uploaded`, `builtin_custom_boundary`) via a discriminated union. New `backend/services/resolver.py` implements finest-of-each-input logic with broadcast (coarser → finer) and aggregate (finer → coarser) rules. Responses include `causeRollups`, `provenance`, `warnings`, `totalDeaths` (cause-specific only), and `allCauseDeaths` (separate, to prevent double-counting). Results page renders a choropleth (Mapbox GL) with cause/metric toggles, a paginated per-polygon table, and CSV (long) / GeoJSON (wide) downloads backed by a 1-hour in-memory cache. Every CRF in `crf-library.json` now carries `cause` + `endpointType`.
```

Update the `last-updated` frontmatter field to `"2026-04-21"`.

- [ ] **Step 2: Commit**

```bash
git add C:/Users/vsoutherland/Documents/obsidian_vault/"System Map/HIA Walkthrough.md"
git commit -m "docs(system-map): record polygon-based results landing"
```

Note: the Obsidian vault is likely a separate repo. If it's not under git control, just save the edit; no commit needed.

---

## Task 23: Final verification sweep

**Files:** none — verification only.

- [ ] **Step 1: Run the full test suite**

```bash
cd backend && python -m pytest -q
cd ../frontend && npm test -- --run
```
Expected: all tests pass.

- [ ] **Step 2: Walk the happy path one more time**

Start both servers. From `/analysis/1`:
1. USA → California → Census Tract level → PM2.5
2. Step 2 → Built-in Data → EPA AQS PM2.5 US → 2022
3. Step 3 → Population populated from tract count
4. Step 4 → default incidence
5. Step 5 → select `gbd_pm25_ihd` + `gbd_pm25_stroke`
6. Step 6 → Run analysis
7. Results page should show:
   - Hero aggregate with total deaths
   - Provenance bar showing "Concentration: state (broadcast to tract) (epa_aqs)"
   - Warning about state-broadcast
   - Two cause cards: IHD, Stroke
   - Choropleth map colored by Sum of cause-specific, attributable cases
   - Paginated per-polygon table
   - Download CSV and Download GeoJSON buttons functional

- [ ] **Step 3: Open a PR**

```bash
git push -u origin feature/polygon-results
# open PR via GitHub UI or gh CLI
```

---

## Appendix — Expected files created/modified

**Backend:**
- New: `backend/services/resolver.py`, `backend/services/rollups.py`, `backend/services/results_cache.py`, `backend/services/download_serializers.py`
- Modified: `backend/routers/compute.py`, `backend/requirements.txt`
- New tests: `backend/tests/test_resolver.py`, `backend/tests/test_rollups.py`, `backend/tests/test_results_cache.py`, `backend/tests/test_download_serializers.py`, `backend/tests/test_compute_models.py`, `backend/tests/test_compute_spatial.py`, `backend/tests/test_compute_spatial_e2e.py`

**Frontend:**
- Modified: `frontend/src/data/crf-library.json`, `frontend/src/stores/useAnalysisStore.js`, `frontend/src/pages/steps/Step1StudyArea.jsx`, `frontend/src/pages/steps/Step2AirQuality.jsx`, `frontend/src/pages/steps/Step6Run.jsx`, `frontend/src/pages/Results.jsx`, `frontend/src/lib/api.js`
- New: `frontend/src/components/ChoroplethMap.jsx`, `frontend/src/components/PerPolygonTable.jsx`
- New tests: `frontend/src/data/__tests__/crf-library.test.js`, `frontend/src/pages/steps/__tests__/Step1StudyArea.test.jsx`, `frontend/src/components/__tests__/ChoroplethMap.test.jsx`, `frontend/src/components/__tests__/PerPolygonTable.test.jsx`

**Docs:**
- Updated: `docs/superpowers/specs/2026-04-21-hia-polygon-results-design.md` (already exists)
- Updated: `C:/Users/vsoutherland/Documents/obsidian_vault/System Map/HIA Walkthrough.md`
