# HIA Polygon-Based Results — Design

**Date:** 2026-04-21
**Status:** Draft, awaiting user review
**Scope:** Wire Step 1's analysis-level radio through to the HIA compute so results are produced per reporting polygon. Add a choropleth map, per-polygon table, and CSV/GeoJSON downloads to the Results page. Fix the all-cause / cause-specific double-counting in the existing total-deaths aggregation.

## Motivation

Today the Step 1 analysis-level radio (`state` | `county` | `tract`) is saved to the Zustand store and never read by any downstream code. The built-in data path in `Step2AirQuality.jsx:278-287` collapses whatever admin polygons come back from `/api/data/concentration` into a single arithmetic mean and passes that scalar to `computeHIA`, discarding all spatial structure. The spatial backend path (`/api/compute/spatial`) only fires when the user has uploaded a concentration raster + population raster + boundary shapefile — which is rare and not what built-in data users experience.

The result: every analysis today produces one number per CRF for the whole country, regardless of what the user picked in Step 1. This design wires the polygon choice through end-to-end and lets the Results page show spatial variation.

## Core rule (carries through every component)

For each reporting polygon the user picks in Step 1, use the **finest available grain of every input**, aligned to that polygon:
- If an input is **coarser** than the polygon, broadcast its value down the spatial hierarchy (state → each tract within that state inherits the state value).
- If an input is **finer** than the polygon, aggregate up: population-weighted mean for concentration, sum for population, groupby-sum for polygon-resolution aggregation.
- If an input is **a raster finer** than the polygon, run `rasterstats.zonal_stats` population-weighted against a pop raster where available, arithmetic mean otherwise.

Every response carries a provenance object noting the native grain of each input, and a `warnings[]` list noting any broadcast or mismatch.

## Architecture

**Components, one new backend service + four changed files + two new frontend components:**

| # | Module | Kind | Role |
|---|---|---|---|
| 1 | `backend/services/resolver.py` | **new** | Given `(pollutant, country, year, analysisLevel, boundary_upload_id?)`, returns per-polygon arrays: `zone_ids`, `zone_names`, `geometries`, `c_baseline`, `c_control`, `population`, plus a `provenance` dict noting the native grain of each input. Handles broadcast (coarser → finer) and aggregate (finer → coarser, population-weighted for C, sum for pop). |
| 2 | `backend/routers/compute.py` | changed | `/api/compute/spatial` accepts a discriminated-union request: `mode: "uploaded"` (today's three-file path), `mode: "builtin"` (new, drives the resolver on built-in data), or `mode: "builtin_custom_boundary"` (new, user-uploaded boundary + built-in C/pop). All three converge on `_run_spatial_compute` with arrays from either `prepare_spatial_inputs` or the new resolver. Response gains `provenance`, `warnings`, `causeRollups`, and `allCauseDeaths`. |
| 3 | `frontend/src/pages/steps/Step6Run.jsx` | changed | Routes to `/api/compute/spatial` whenever Step 1's `analysisLevel` ≠ `"country"` OR a boundary upload exists OR a built-in dataset is selected. The client-side `computeHIA` path is kept only for the all-manual-entry case (country-level, manual baseline/control, manual population). |
| 4 | `frontend/src/pages/Results.jsx` + new `components/ChoroplethMap.jsx` + new `components/PerPolygonTable.jsx` | changed + new | Renders Mapbox choropleth (toggle: cause + metric), sortable per-polygon table, and a Download menu with CSV / GeoJSON. Aggregate hero block is kept. |
| 5 | `frontend/src/data/crf-library.json` | changed | Every CRF gains a `cause` enum: `all_cause`, `ihd`, `stroke`, `lung_cancer`, `copd`, `lri`, `diabetes`, `dementia`, `asthma`, `respiratory_mortality`, `birth_weight`, `gestational_age`. Results page groups rollups by this field. |

**Key invariant:** `backend/services/hia_engine.py` is unchanged. This is "new data source adapters + new presentation layer," not new math.

## Data flow (built-in US tract, happy path)

1. User picks USA → California → `analysisLevel: tract` in Step 1. Picks `pm25`, `epa_aqs`, year 2022 in Step 2.
2. Step 6 posts to `/api/compute/spatial` with:
   ```json
   {
     "mode": "builtin",
     "pollutant": "pm25",
     "country": "us",
     "year": 2022,
     "analysisLevel": "tract",
     "stateFilter": "06",
     "controlMode": "benchmark",
     "controlConcentration": 5.0,
     "selectedCRFs": [...],
     "monteCarloIterations": 1000
   }
   ```
3. Backend resolver:
   - **Boundary:** fetches `demographics/us/2022.parquet` rows where `state_fips == "06"` — those ~9,000 tracts are the reporting polygons and their geometries come from the same file.
   - **Concentration:** fetches `epa_aqs/pm25/ne_states/2022.parquet` → state-level scalar. Broadcast: every tract inherits its state's C value. Flags `provenance.concentration.grain = "state"`, `provenance.concentration.broadcast_to = "tract"`, appends warning.
   - **Population:** ACS tracts already carry `total_pop`. Direct use. `provenance.population.grain = "tract"`.
   - **Incidence:** per-CRF `defaultRate` from the library, or GBD national scalar. `provenance.incidence.grain = "national"`.
4. Backend hands `(c_base, c_ctrl, pop)` arrays of shape `(n_tracts,)` to `_run_spatial_compute` — the existing engine.
5. Response: `{zones: [...], aggregate: {...}, causeRollups: [...], totalDeaths, allCauseDeaths, provenance, warnings, resultId}`.
6. Frontend stores the full response in `results`. Results page renders hero + provenance + cause rollups + choropleth + per-polygon table.

## Resolver rules

Runs three independent resolutions per request — concentration, population, incidence — then joins them on the reporting polygon's ID.

| Input's native grain | vs. reporting polygon | Action |
|---|---|---|
| Same | e.g., ACS tract pop + tract reporting | Direct use. |
| Coarser | e.g., state-level EPA AQS + tract reporting | Broadcast. Join key is the spatial-hierarchy code — tract→county via `county_fips`, tract→state via `state_fips`, state→country trivially. |
| Finer (raster) | e.g., 1 km raster + tract reporting | Population-weighted zonal mean via `rasterstats.zonal_stats`. Fall back to arithmetic mean if no pop raster. For population input, `sum` instead of `mean`. |
| Finer (polygon) | e.g., tract pop + county reporting | Aggregate: `groupby(parent_fips).sum()` for population; for concentration, `groupby(parent_fips).apply(weighted_mean)` using the finer polygon's `total_pop` as weights. |
| Missing | no data at any grain for the requested country/year | Fail the request with a specific 404, naming what was missing. |

**Provenance object shape:**

```json
{
  "concentration": {"grain": "state",   "source": "epa_aqs",     "broadcast_to": "tract"},
  "population":    {"grain": "tract",   "source": "acs"},
  "incidence":     {"grain": "national","source": "crf_default", "broadcast_to": "tract"}
}
```

**Warning triggers:**
- Coarser-than-reporting broadcast (e.g., state C → tract polygons).
- Population year mismatched with concentration year beyond ±2 years.
- Custom boundary polygons outside the concentration dataset's bounding box — count reported.
- Non-US custom boundary with no population uploaded, falling back to country-level scalar.
- Both an all-cause CRF and cause-specific CRFs selected (double-counting notice).

**What the resolver does NOT do:**
- No caching beyond `lru_cache` on `_read_parquet`. Results caching lives in the download handler (see below).
- No schema detection beyond existing `_detect_id_column` / `_detect_name_column` in `geo_processor.py`.
- No CRS reprojection — all inputs assumed EPSG:4326 (already the ETL's invariant).

## API contract

### Request

```
POST /api/compute/spatial
```

Pydantic discriminated union on `mode`:

```python
class BuiltinMode(BaseModel):
    mode: Literal["builtin"]
    pollutant: str                              # "pm25" | "ozone" | "no2" | "so2"
    country: str                                # ISO3 or slug: "us", "USA", "mex"
    year: int
    analysisLevel: Literal["country", "state", "county", "tract"]
    stateFilter: str | None = None              # 2-digit FIPS; required for county/tract
    countyFilter: str | None = None             # 3-digit FIPS; optional with state
    controlMode: Literal["scalar", "builtin", "rollback", "benchmark"]
    controlConcentration: float | None = None   # scalar or benchmark
    controlRollbackPercent: float | None = None # rollback
    selectedCRFs: list[CRFInput]
    monteCarloIterations: int = 1000

class UploadedMode(BaseModel):
    mode: Literal["uploaded"]
    concentrationFileId: int
    controlFileId: int | None = None
    controlConcentration: float | None = None
    populationFileId: int                       # required today
    boundaryFileId: int
    selectedCRFs: list[CRFInput]
    monteCarloIterations: int = 1000

class CustomBoundaryBuiltinMode(BaseModel):
    mode: Literal["builtin_custom_boundary"]
    pollutant: str
    country: str
    year: int
    boundaryFileId: int                         # user-uploaded polygons
    controlMode: Literal["scalar", "builtin", "rollback", "benchmark"]
    controlConcentration: float | None = None
    controlRollbackPercent: float | None = None
    selectedCRFs: list[CRFInput]
    monteCarloIterations: int = 1000

SpatialComputeRequest = Annotated[
    BuiltinMode | UploadedMode | CustomBoundaryBuiltinMode,
    Field(discriminator="mode"),
]
```

### Response

```python
class ZoneResult(BaseModel):
    zoneId: str
    zoneName: str | None
    parentId: str | None             # state_fips for county/tract, county_fips for tract
    geometry: dict                   # GeoJSON
    baselineConcentration: float
    controlConcentration: float
    population: float
    results: list[CRFResult]         # per-CRF: cases (mean + 95% CI), PAF, rate/100k

class CauseRollup(BaseModel):
    cause: str                       # "ihd" | "stroke" | "all_cause" | ...
    endpointLabel: str               # human-readable, e.g. "Ischemic heart disease"
    attributableCases: EstimateCI    # summed across CRFs in this cause group
    attributableRate: EstimateCI     # population-weighted mean rate per 100k across polygons
    crfIds: list[str]                # which CRFs contributed

class Provenance(BaseModel):
    concentration: dict              # {grain, source, broadcast_to?}
    population: dict
    incidence: dict

class SpatialComputeResponse(BaseModel):
    resultId: str                    # UUID, for download URLs
    zones: list[ZoneResult]
    aggregate: ComputeResponse       # unchanged: per-CRF totals
    causeRollups: list[CauseRollup]  # NEW: grouped by cause, all-cause always separate
    totalDeaths: EstimateCI          # sum across cause-specific mortality only
    allCauseDeaths: EstimateCI | None  # NEW: populated when user picked an all-cause CRF
    provenance: Provenance
    warnings: list[str]
    processingTimeSeconds: float
```

**Why `causeRollups` is its own field:** the frontend needs per-cause totals for the choropleth dropdown and the per-cause download. Computing it client-side would require duplicating the CRF → cause mapping. Putting it on the response keeps that logic server-side where the library lives.

**`totalDeaths` vs `allCauseDeaths`:** fixes the current double-counting in `hia_engine.py:657-670`, which sums any endpoint whose label contains "mortality", "death", or "deaths" — that over-counts if the user picks both `epa_pm25_acm_adult` and `epa_pm25_ihd_adult`. The two totals are never summed; the UI shows a badge when both kinds are selected.

### Download endpoints (new)

```
GET /api/compute/results/{result_id}/download?format=csv
GET /api/compute/results/{result_id}/download?format=geojson
```

- **CSV (long format):** one row per (polygon × CRF). Columns: `polygon_id`, `polygon_name`, `parent_id`, `baseline_c`, `control_c`, `delta_c`, `population`, `crf_id`, `crf_source`, `cause`, `endpoint`, `attributable_cases_mean`, `attributable_cases_lower95`, `attributable_cases_upper95`, `rate_per_100k_mean`, `attributable_fraction_mean`.
- **GeoJSON (wide format):** one Feature per polygon. Properties include core fields (polygon_id, name, parent_id, baseline_c, control_c, population) plus per-cause rollup columns (`cases_ihd_mean`, `cases_ihd_lower95`, `cases_ihd_upper95`, `rate_per_100k_ihd_mean`, … one set per cause present), plus an `all_cause_*` set when an all-cause CRF was selected. Per-CRF columns are not pivoted into the GeoJSON — download the CSV for that. Cause rollups, not CRF rollups, because the GIS consumer wants to map "IHD deaths" not "Pope 2004 ACS CPS-II IHD cases".

**Result cache:** in-memory `cachetools.TTLCache(maxsize=32, ttl=3600)` keyed by the `resultId` UUID. 1-hour TTL. Cache miss on download returns 410 Gone with a "re-run the analysis" message. Cheap to regenerate, matches a typical user session.

## Frontend changes

### Step 1 (`Step1StudyArea.jsx`)
- `step1.studyArea.analysisLevel` becomes load-bearing (today it's dead wiring). Default `"state"` for USA, `"country"` elsewhere.
- The analysis-level radio is also shown whenever a custom boundary is uploaded — with a synthetic option `"custom"` labeled "Uploaded polygons."
- No map-picker changes.

### Step 2 (`Step2AirQuality.jsx`)
- Remove the scalar-collapse at lines 278-287. The `BuiltinConcentrationLoader` still fetches the GeoJSON for preview, but no longer averages to a single number — it sets `baseline.datasetId`, `baseline.year`, and a preview flag. Per-polygon values are re-fetched by the backend resolver at compute time.
- `baseline.value` becomes optional (only set when `type === 'manual'`). Validation updates accordingly.

### Step 6 (`Step6Run.jsx`)
Routing rule:
```js
const needsSpatial =
  step1.studyArea.analysisLevel !== 'country' ||
  step1.studyArea.boundaryUploadId != null ||
  step2.baseline.type === 'dataset' ||
  step2.baseline.uploadId != null

if (needsSpatial) {
  const mode = step2.baseline.uploadId ? 'uploaded'
             : step1.studyArea.boundaryUploadId ? 'builtin_custom_boundary'
             : 'builtin'
  await runSpatialCompute({ mode, ...buildPayload(mode) })
} else {
  const raw = computeHIA(scalarConfig)  // client-side engine, unchanged
}
```

### Results page (`Results.jsx` + two new components)

Top-to-bottom layout:
1. **Hero aggregate block** (existing, kept): total deaths, headline rate per 100k, attributable fraction. Add a small `allCauseDeaths` pill next to `totalDeaths` when both are populated, with a tooltip explaining the cause-specific vs. all-cause distinction.
2. **Provenance bar** (new, thin): "Concentration: state (EPA AQS) · Population: tract (ACS) · Incidence: national (GBD)." Warnings surface here as a dismissible yellow bar.
3. **Cause rollup cards** (new): one card per `causeRollups` entry, plus an "All-cause mortality" card when present. Each card: cause label, attributable cases with CI, contributing CRF count, mini sparkline of top-10 polygons by cases.
4. **`<ChoroplethMap />`** (new): Mapbox GL from `response.zones`. Two dropdowns — cause (default "Sum of cause-specific") and metric (default "Attributable cases"; alt "Rate per 100k"). Viridis color scale, 7 quantile breaks. Click polygon → detail card with per-CRF breakdown.
5. **`<PerPolygonTable />`** (new): sortable table. Columns: name, baseline C, control C, ΔC, population, rate/100k (per selected cause), cases (per selected cause with CI). Pagination at 50 rows.
6. **Existing `ResultsTable`** (per-CRF summary): kept below for backward compatibility.
7. **Download menu**: "Download CSV (long)," "Download GeoJSON (wide)," "Download PDF report" (existing). CSV/GeoJSON hit `/api/compute/results/{resultId}/download`.

### State store (`useAnalysisStore.js`)
`results` gains `zones`, `causeRollups`, `provenance`, `warnings`, `allCauseDeaths`, `resultId`. No breaking change to existing fields.

## Error handling & edge cases

| Case | Behavior |
|---|---|
| User picks tract but no sub-national concentration on disk | Run at tract with C broadcast from state. Yellow warning in provenance bar. |
| User picks tract, no ACS year matches concentration year | Use nearest ACS year within ±2 years. Warning: "Population year N used for concentration year M (gap of \|N−M\| years)". Gap > 2 years fails the request with 422. |
| Custom boundary has polygons outside concentration bounding box | Those polygons get `null` C, null results, rendered as grey on the map. Warning lists the count. |
| Custom boundary uploaded but no population uploaded for non-US country | Fall back to WHO AAP country-level population scalar, broadcast. Warning. If that is also missing, 422 with explicit error. |
| User picks both an all-cause CRF and cause-specific CRFs | Both run. `totalDeaths` (cause-specific) and `allCauseDeaths` reported separately. Hero block shows a "!" icon explaining why they're not summed. |
| Monte Carlo iterations × polygons blows up (e.g., 9k tracts × 5000 iter × 7 CRFs ≈ 315M samples) | No hard soft-cap in v1. Log server-side when `polygons × iterations × crfs > 500M`; include `processingTimeSeconds` in the response so we can observe real load before tuning. Throughput-tune in a follow-up once we have production traces. |
| Result cache miss on download (TTL expired) | 410 Gone with "Result expired, please re-run the analysis." |
| Backend timeout on large tract runs | `ProcessPoolExecutor` already handles this; `processingTimeSeconds` on the response for observability. |

## Testing

**Resolver (`tests/test_resolver.py`, new):**
- State concentration + tract reporting → each tract inherits its state's C. Assert CA tracts all equal, NY tracts all equal, CA ≠ NY.
- Tract pop + county reporting → `groupby(county_fips).sum()` matches hand-rolled expected value.
- Country-only concentration + state reporting → every state inherits country scalar.
- Raster concentration + tract reporting (mock raster) → `zonal_stats` called with tract geoms; results population-weighted when pop raster provided.
- Missing data paths: 404 when no concentration at any grain; 422 when custom boundary and no population.

**Compute router (`tests/test_compute_spatial.py`, extended):**
- All three `mode` variants produce a valid `SpatialComputeResponse`.
- `causeRollups` groups correctly: `epa_pm25_ihd_adult` + `epa_pm25_stroke_adult` → two rollups, `totalDeaths` = sum of both, `allCauseDeaths` = null.
- `epa_pm25_acm_adult` + `epa_pm25_ihd_adult` → `totalDeaths` = just IHD, `allCauseDeaths` = ACM, warning about double-counting.
- Warnings fire in expected cases (state-broadcast-to-tract, year mismatch).

**Frontend:**
- `ChoroplethMap` unit test with React Testing Library — mocked zones render as GeoJSON source; cause/metric dropdowns re-filter.
- `PerPolygonTable` sort + pagination tests.
- Step 6 routing-rule test: the `needsSpatial` decision tree with each input combo.
- Playwright end-to-end: USA → CA → tract → PM2.5 → 2022 → IHD CRF → submit → choropleth renders.

**No changes to `hia_engine.py`** → no new engine tests. Existing `test_hia_engine.py` continues to cover scalar math.

## Out of scope

- Van Donkelaar / Google Earth Engine extract and the ETL to consume it. The resolver's raster-input path is architected but exercised only against mock rasters in tests until real data lands.
- Plan 3 (post-results multi-year) — paused behind this work per project state as of 2026-04-21.
- Valuation changes (Step 7 untouched).
- Non-US built-in admin-1 / admin-2 boundaries beyond USA. Custom uploads still work for them.
- CDC Wonder county-level incidence integration (tracked on `feature/cdc-wonder-baseline-rates`).
- Result caching beyond the 1-hour download TTL.

## Open questions

None blocking. A couple of tactical details that are not decisions so much as tuning:
- Choropleth quantile breaks default to 7. Revisit after seeing real output; small-n polygon sets (<20 counties, <5 states) may read better with equal-interval breaks instead.
- Download filename: `hia-{analysis-slug}-{YYYYMMDD}.csv` (uses `analysisName` from Step 1, slugified; falls back to the resultId UUID's first 8 chars when `analysisName` is blank).
