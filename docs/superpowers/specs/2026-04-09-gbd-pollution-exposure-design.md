# GBD Air Pollution Exposure Integration — Design

**Date:** 2026-04-09
**Status:** Draft for review
**Scope:** Ingest GBD 2023 air pollution exposure estimates (NO2, ozone,
PM2.5) into the HIA tool as the first default concentration layer,
supporting both quick country-level lookups and gridded PM2.5 spatial
analyses.

## Goal

Give HIA tool users pre-filled baseline concentrations when they pick a
country, subnational region, or GHS SMOD urban center, and provide a
ready-to-use gridded PM2.5 raster as the tool's default spatial
concentration layer. No more empty baseline fields in the wizard when a
user hasn't uploaded their own data.

## Scope

**In scope:**
- NO2, ozone, and PM2.5 tabular exposure summaries (mean + 95% UI) at
  the location × year grain, 2015–2023.
- PM2.5 annual global gridded concentration rasters for 2015–2023
  (9 GeoTIFFs).
- Natural Earth crosswalk (countries + states) for resolving GBD
  `location_id` values to the HIA tool's boundary system.
- GHS SMOD urban center → Natural Earth spatial join, so city-level
  analyses transitively inherit GBD values.
- Two new `/api/data/pollution` router endpoints and a
  `defaultConcentrationLayer` field on the spatial compute endpoint.

**Out of scope / deferred:**
- GADM crosswalk (deferred — Natural Earth is the v1 target).
- Population-weighted country means derived from the raster × WorldPop
  (deferred — we serve the GBD summary CSV's unweighted value for v1).
- NO2 and ozone gridded layers (IHME has not published rasters for
  these in the files we have).
- Ozone years 2022 and 2023 (blocked on IHME's GBD 2023 ozone release —
  the ozone file on disk is GBD 2021, ending at 2021).
- Updates to `backend/etl/process_pm25.py` — the existing CLI continues
  to work for bespoke runs. Router-level access goes through the new
  `pollution_exposure` service instead.

## Source files

All files live under `data/raw/gbd/`:

**Top-level:**
- `IHME_GBD_2021_AIR_POLLUTION_1990_2023_CODEBOOK_Y2024M06D06.csv` —
  codebook documenting measure IDs, location IDs, and column codings.

**`data/raw/gbd/pollution/`:**

| Pollutant | File | Release | Coverage | Shape |
|---|---|---|---|---|
| NO2 | `IHME_GBD_2023_AIR_POLLUTION_1990_2023_NO2_Y20251010.csv` | GBD 2023 | 1990–2023 | Tabular: location_id × year_id, unit ppb |
| Ozone | `IHME_GBD_2021_AIR_POLLUTION_1990_2021_OZONE_Y2022M01D31.csv` | GBD 2021 | 1990–2021 | Tabular: location_id × year_id, unit ppb |
| PM2.5 summary | `IHME_GBD_2023_AIR_POLLUTION_1990_2023_PM_Y20250930.CSV` | GBD 2023 | 1990–2023 | Tabular: location_id × year_id, unit µg/m³ |
| PM2.5 rasters | `IHME_GBD_2023_AIR_POLLUTION_1990_2023_PM_{year}_Y2025M02D13.TIF` | GBD 2023 | 1990, 1995, 2000–2023 | Global gridded GeoTIFF, one per year |

## Temporal scope

**2015–2023** only. All rows with `year_id < 2015` are dropped at
ingest. This matches the mortality spec's window for consistency.

Ozone stops at 2021 regardless. Consumers must handle the 2022/2023 gap
— the `pollution_exposure` service implements a "latest available"
fallback that returns the 2021 value with a `year_used` flag when a
caller asks for ozone at a year after 2021.

PM2.5 rasters pre-2015 (1990, 1995, 2000–2014) stay in `data/raw/` but
are not copied or cataloged. Adding them later is a one-line scope
change to the ETL constants.

## Tabular storage schema

**Primary artifact:** `data/processed/pollution/gbd_pollution.parquet`

One row per pollutant × location × year:

| Column | Type | Notes |
|---|---|---|
| `pollutant` | category | `"pm25"`, `"no2"`, or `"ozone"` |
| `gbd_location_id` | int32 | GBD's integer location ID |
| `ihme_loc_id` | string | GBD's ISO3-ish code (e.g., `"BRA"`, `"IND_4841"`); back-filled via crosswalk when missing from the source CSV |
| `location_name` | string | GBD's English name |
| `location_level` | int8 | 0 = Global, 1 = Super-region, 2 = Region, 3 = Country, 4 = Subnational L1, 5 = Subnational L2 |
| `ne_country_iso3` | string, nullable | ISO3 resolved from crosswalk |
| `ne_country_uid` | string, nullable | Natural Earth `ADM0_A3` |
| `ne_state_uid` | string, nullable | Natural Earth state polygon identifier; NULL for country-level GBD rows |
| `year` | int16 | 2015–2023 (ozone only 2015–2021) |
| `mean` | float32 | GBD mean exposure |
| `lower` | float32 | 95% UI lower bound |
| `upper` | float32 | 95% UI upper bound |
| `unit` | category | `"ug_m3"` (PM2.5) or `"ppb"` (NO2, ozone) |
| `release` | category | `"gbd_2023"` or `"gbd_2021"` |

Unresolved locations (no crosswalk match, or intentionally unmappable
super-regions) stay in the parquet with NULL `ne_*_uid` columns.

**Row count estimate:** ~4,000 locations × 9 years × 3 pollutants ≈
108,000 rows, well under 10 MB compressed.

## Raster catalog

**`data/processed/pollution/pm25_gbd2023/catalog.parquet`**

One row per year, describing an on-disk TIF:

| Column | Type | Notes |
|---|---|---|
| `year` | int16 | 2015–2023 |
| `relative_path` | string | e.g., `"pm25_gbd2023/2019.tif"`, relative to `data/processed/pollution/` |
| `crs` | string | EPSG code captured at ingest |
| `pixel_size_deg` | float32 | Cell size in degrees from the raster transform |
| `nodata` | float32 | Nodata value from the header |
| `xmin` / `ymin` / `xmax` / `ymax` | float32 × 4 | Bounding box |
| `unit` | category | `"ug_m3"` |
| `source` | string | `"IHME GBD 2023"` |

The 9 raster files are **copied** (not moved) from
`data/raw/gbd/pollution/` to `data/processed/pollution/pm25_gbd2023/`,
renamed to `{year}.tif`. The raw folder retains the original IHME
filenames as an audit trail.

Processed rasters are **gitignored**. The catalog parquet is committed.
The ETL is the canonical regeneration path.

## Natural Earth crosswalk

**Artifact:** `data/processed/boundaries/gbd_to_ne.csv` (committed).

Columns:

| Column | Notes |
|---|---|
| `gbd_location_id` | int |
| `gbd_name` | GBD's English name |
| `ihme_loc_id` | ISO3-ish code from GBD |
| `location_level` | int |
| `ne_country_iso3` | ISO3 |
| `ne_country_uid` | NE `ADM0_A3` |
| `ne_state_uid` | NE state identifier (NULL for country rows) |
| `match_method` | `"iso3"`, `"exact_name"`, `"fuzzy"`, `"manual"`, `"unmatched"` |
| `confidence` | 0–100 (100 for iso3 / exact_name / manual) |
| `notes` | free-text for manual rows |

**Build passes:**
1. **ISO3 match** for country-level rows using `ihme_loc_id` →
   `ne_countries.ADM0_A3`. Confidence = 100, method = `"iso3"`.
2. **Exact name match** for subnational rows against `ne_states`
   filtered to the parent country. Confidence = 100, method =
   `"exact_name"`.
3. **Fuzzy match** (`rapidfuzz`, token-set ratio) for remaining
   subnational rows, threshold 92. Match is written with the score as
   `confidence`; method = `"fuzzy"`.
4. Anything still unmatched gets method = `"unmatched"`, confidence =
   0, and a suggestion in `notes`.

**Manual review gate:** the crosswalk build step exits with a non-zero
code if any row has `match_method = "unmatched"` or `match_method =
"fuzzy"` with `confidence < 98`. The engineer opens the CSV, fixes
flagged rows (setting `match_method = "manual"` and `confidence = 100`),
and re-runs. The downstream tabular and raster ingest steps do not run
until the crosswalk is clean — a partial crosswalk would produce a
partially-resolved parquet that silently fails lookups.

## GHS SMOD spatial join

**Artifact:** `data/processed/boundaries/ghs_smod_to_ne.parquet`
(committed, ~40 KB).

Columns: `ghs_uid`, `ghs_name`, `ne_country_uid`, `ne_state_uid`.

Built once by reading the GHS SMOD shapefile
(`data/raw/boundaries/GHS_SMOD/GHS_SMOD_E2020_GLOBE_R2023A_54009_1000_UC_V2_0.shp`),
taking each urban center's centroid, and running a `geopandas.sjoin`
with `predicate="within"` against Natural Earth country and state
polygons. Border-straddling centers take the country and state their
centroid falls inside.

This is not a name-based crosswalk. It's purely spatial. No manual
review needed.

## ETL structure

**Entry point:** `backend/etl/process_gbd_pollution.py`

Runs the following sub-steps in order:

1. `build_location_crosswalk` (`backend/etl/gbd_pollution/crosswalk.py`)
2. `ingest_tabular` (`backend/etl/gbd_pollution/tabular.py`)
3. `catalog_rasters` (`backend/etl/gbd_pollution/rasters.py`)
4. `build_ghs_smod_spatial_join` (`backend/etl/gbd_pollution/ghs_join.py`)

Idempotent: each sub-step skips if its output already exists unless
`--force` is passed. The crosswalk step must complete successfully
before the tabular step runs — an unclean crosswalk aborts the whole
pipeline.

**Runbook:** `python -m backend.etl.process_gbd_pollution`

**File layout after a successful run:**

```
data/processed/
  boundaries/
    gbd_to_ne.csv                    ← committed
    ghs_smod_to_ne.parquet           ← committed
  pollution/
    gbd_pollution.parquet            ← committed
    pm25_gbd2023/
      catalog.parquet                ← committed
      2015.tif … 2023.tif            ← gitignored
```

## Service layer

**New module:** `backend/services/pollution_exposure.py`

Two public functions.

### `get_default_concentration`

```python
def get_default_concentration(
    pollutant: str,
    year: int,
    *,
    ne_country_uid: str | None = None,
    ne_state_uid: str | None = None,
    ghs_uid: str | None = None,
) -> dict | None
```

Returns a dict with keys `mean`, `lower`, `upper`, `unit`, `source`,
`year_used` — or `None` when no match.

**Resolution order:**
1. If `ne_state_uid` is given, look up
   `(pollutant, year, ne_state_uid)` in `gbd_pollution.parquet`.
2. Else if `ghs_uid` is given, resolve it to `ne_country_uid` /
   `ne_state_uid` via `ghs_smod_to_ne.parquet`, then retry step 1 or 3.
3. Else if `ne_country_uid` is given, look up at the country level.
4. **Ozone fallback:** if `pollutant == "ozone"` and `year > 2021`,
   fall back to year 2021 and set `year_used = 2021`.
5. Return `None` if nothing resolves.

The parquet is lazy-loaded once into a module-level cache, same pattern
as `baseline_rates.py` in the CDC Wonder design.

### `get_default_raster_path`

```python
def get_default_raster_path(pollutant: str, year: int) -> Path | None
```

Consults `pm25_gbd2023/catalog.parquet` and returns the absolute path
to the matching TIF. Only `pollutant == "pm25"` is supported in v1; any
other pollutant returns `None`.

## Router integration

**New endpoints in `backend/routers/data.py`:**

- `GET /api/data/pollution/default?pollutant={pm25|no2|ozone}&year={year}&ne_country_uid={uid}[&ne_state_uid={uid}][&ghs_uid={uid}]`
  Returns the JSON blob from `get_default_concentration`, or HTTP 404
  when nothing resolves.

- `GET /api/data/pollution/raster-catalog?pollutant={pm25}`
  Returns the catalog.parquet contents as a JSON list. Lets the
  frontend show a year-selector dropdown populated from what's actually
  on disk.

**Modification to `backend/routers/compute.py`:**

Add an optional field to `SpatialComputeRequest`:

```python
defaultConcentrationLayer: str | None = None
```

Format: `"{pollutant}_{dataset}_{year}"` (e.g., `"pm25_gbd2023_2019"`).
When provided, the router resolves it via `get_default_raster_path` and
uses the resulting path as the concentration source. When both
`concentrationFileId` and `defaultConcentrationLayer` are set, the
upload wins — explicit user input takes precedence.

No changes needed to the scalar `/api/compute` endpoint. Scalar flows
fetch a value from `/api/data/pollution/default` first and pass it as
`baselineConcentration`.

## Fallback matrix

| Situation | Behavior |
|---|---|
| User picks a country | `/api/data/pollution/default` returns country-level mean |
| User picks a subnational region | Same endpoint with `ne_state_uid`; returns state-level mean if present, else falls back to country |
| User picks a GHS SMOD city | Same endpoint with `ghs_uid`; service resolves via spatial join parquet to enclosing NE country/state |
| User runs spatial analysis without uploading a raster | Request carries `defaultConcentrationLayer="pm25_gbd2023_{year}"`; router loads the GBD TIF |
| User queries NO2 or ozone raster analysis | `get_default_raster_path` returns `None`; router returns HTTP 400 "No default raster available" |
| User queries ozone for 2022 or 2023 | Service falls back to 2021, returns `year_used: 2021` |
| Location unresolved in crosswalk | Service returns `None`; frontend shows "no default available" |
| User uploads a boundary file | Existing spatial pipeline runs zonal stats against the GBD TIF on demand |

## Testing

Six pytest modules under `backend/tests/`:

1. **`test_gbd_pollution_crosswalk.py`** — synthetic 4-row GBD location
   list + minimal NE shapefile fixture. Asserts ISO3 matches, exact
   name subnational matches, fuzzy-match thresholding at 98, and that
   below-threshold rows are flagged in the CSV rather than silently
   assigned.
2. **`test_gbd_pollution_ingest_tabular.py`** — canned 3-row CSV
   fixtures for each pollutant (including one pre-2015 row that must
   be filtered). Asserts year filtering, unit normalization, crosswalk
   join, and NULL handling for unresolved locations.
3. **`test_gbd_pollution_raster_catalog.py`** — tiny 10×10 test TIF.
   Asserts the catalog parquet columns and file renaming.
4. **`test_ghs_smod_spatial_join.py`** — synthetic 3-center GHS fixture
   against synthetic 2-country/3-state NE fixture. Asserts correct
   containment and border-straddling behavior.
5. **`test_pollution_exposure_service.py`** — in-memory parquet
   fixtures. Exercises resolution order (state → country → GHS
   transitive), ozone 2022+ fallback to 2021, and the missing-raster
   case for NO2/ozone.
6. **`test_data_router_pollution.py`** — FastAPI `TestClient` hits
   both new endpoints against in-memory fixtures; asserts JSON shape
   and error-path HTTP status codes.

No tests hit the live IHME site. CI stays deterministic.

## Dependencies on other workstreams

- **Mortality spec amendment:** the committed CDC Wonder / GBD
  mortality spec references `gbd_to_gadm.csv`. That spec should be
  updated (or amended in a follow-up commit) to redirect to
  `gbd_to_ne.csv` when the mortality workstream resumes. Both projects
  now use Natural Earth as the v1 crosswalk target.
- **Shared crosswalk:** the mortality and pollution projects share
  `data/processed/boundaries/gbd_to_ne.csv`. Whichever project builds
  it first owns the ETL; the other project reads it as an input. In
  practice pollution is being built first, so
  `process_gbd_pollution.py` builds the crosswalk and
  `process_gbd_mortality.py` (future) reads it.

## Non-goals / deferred work

- **GADM crosswalk.** Future workstream. New nullable columns
  (`gadm_uid`) can be added alongside the existing `ne_*_uid` columns
  without a schema migration.
- **Population-weighted country-level PM2.5.** Requires WorldPop
  integration. Deferred.
- **NO2 and ozone gridded layers.** No rasters available in the
  download. Slot in as new catalog files when IHME publishes them.
- **Uncertainty propagation into the HIA engine.** We carry `lower`
  and `upper` in the parquet but the engine treats concentrations as
  fixed in the scalar path. Monte Carlo support for concentration
  uncertainty is a future refinement.
- **CAMS / ACAG / van Donkelaar concentration defaults.** Future
  workstreams register additional catalogs under
  `data/processed/pollution/{source}_{year}/catalog.parquet` without
  touching the GBD pipeline.
