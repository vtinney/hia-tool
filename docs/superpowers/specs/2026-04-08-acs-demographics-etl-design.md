# ACS Demographics ETL Design

**Date:** 2026-04-08
**Status:** Draft — pending user review
**Author:** Brainstormed with Claude

## Summary

Add a local ETL pipeline that downloads American Community Survey (ACS) 5-year estimates
for race, ethnicity, and income at the census-tract level (nationwide, including Puerto
Rico) for vintages 2015 through 2024, joins them to TIGER/Line tract geometries, and
writes one Parquet file per vintage under `data/processed/demographics/us/`. The output
is consumed by the HIA tool's existing `GET /api/data/*` auto-discovery endpoint; no
runtime dependency on external services.

Google Earth Engine is **not** used. It was the original framing of the request, but
the HIA tool's architecture is a local Parquet-based ETL pattern (see
`backend/etl/process_pm25.py`), so we match that pattern directly.

## Goals

- Produce one Parquet file per ACS 5-year vintage (2015–2024, 10 files total).
- One row per census tract, nationwide + Puerto Rico.
- Include the standard environmental-justice variable triad: race/ethnicity, median
  household income, and poverty status.
- Automated, rerunnable, single-command per vintage.
- Fits cleanly alongside `process_pm25.py` — same CLI style, same output conventions.

## Non-goals (v1)

- Margins of error (`*_M` fields). Can be added later if the HIA engine needs them for
  uncertainty propagation.
- Block-group or county resolution. Tract only.
- Crosswalking 2010-boundary tracts to 2020 boundaries. Both eras are stored as-is with
  a `boundary_year` column; any crosswalking happens downstream when the use case is
  known.
- Historical vintages before 2015.
- Runtime fetching from the Census API. This is an offline ETL; the API is hit only
  when building the Parquet files.

## Scope decisions

| Decision | Choice | Rationale |
|---|---|---|
| Geographic scope | US + Puerto Rico | Requested; PR adds ~900 tracts, negligible cost |
| Resolution | Census tract | EJ/health study standard; ACS 5-year is reliable here |
| Vintages | 2015 5-yr through 2024 5-yr (10 files) | User wants any vintage touching 2015+ |
| Boundary handling | Store both 2010- and 2020-era tracts as-is | Honest raw data; crosswalking is an analysis-time decision |
| Tables | B03002, B19013, C17002 | EJScreen-style triad |
| Margins of error | Excluded v1 | Keep schema lean |
| Geometry source | TIGER cartographic-boundary (cb) shapefiles | ~10× smaller than full TIGER, adequate for HIA |

## Architecture

### Scripts

- `backend/etl/process_acs.py` — main ETL script. Processes one vintage per invocation.
- Supports `--all` flag that loops 2015–2024 vintages in sequence.

### CLI

```bash
# Single vintage
python -m backend.etl.process_acs \
  --vintage 2022 \
  --output data/processed/demographics/us/2022.parquet \
  [--verbose]

# All vintages 2015–2024
python -m backend.etl.process_acs --all [--verbose]
```

### Data flow (per vintage `Y`)

1. **Fetch ACS tables** via `cenpy` from `api.census.gov`:
   - `B03002` — Hispanic or Latino Origin by Race (non-overlapping race × ethnicity).
   - `B19013` — Median household income (past 12 months, inflation-adjusted).
   - `C17002` — Ratio of income to poverty level (simplified poverty brackets).
   - Geography: `tract:* state:*`, iterated per state (50 + DC + PR).
   - API key read from `CENSUS_API_KEY` environment variable.
2. **Fetch tract geometry** via `pygris.tracts(year=Y, cb=True)`, nationwide.
   - Includes Puerto Rico.
   - Cached under `~/.pygris` by default.
3. **Join** ACS tables to geometry on the 11-digit tract GEOID (state+county+tract).
4. **Derive percentage columns** (see schema below) from raw counts so downstream code
   does not need to.
5. **Tag** the vintage and boundary era (`boundary_year` = 2010 for vintages 2015–2019,
   2020 for vintages 2020–2024).
6. **Write Parquet** atomically (write to `.tmp` file, then rename) to the output path.
   Geometry serialized as WKT in EPSG:4326, matching the `process_pm25.py` convention.

### Output schema

One row per tract. Columns:

| Column | Type | Source |
|---|---|---|
| `geoid` | str (11 chars) | tract GEOID |
| `state_fips` | str (2) | derived from GEOID |
| `county_fips` | str (3) | derived from GEOID |
| `tract_code` | str (6) | derived from GEOID |
| `vintage` | int | ACS 5-year end year |
| `boundary_year` | int | 2010 or 2020 |
| `total_pop` | int | B03002_001E |
| `nh_white` | int | B03002_003E |
| `nh_black` | int | B03002_004E |
| `nh_aian` | int | B03002_005E |
| `nh_asian` | int | B03002_006E |
| `nh_nhpi` | int | B03002_007E |
| `nh_other` | int | B03002_008E + B03002_009E |
| `hispanic` | int | B03002_012E |
| `pct_nh_white` | float | `nh_white / total_pop` |
| `pct_nh_black` | float | `nh_black / total_pop` |
| `pct_hispanic` | float | `hispanic / total_pop` |
| `pct_minority` | float | `1 - pct_nh_white` |
| `median_hh_income` | int (nullable) | B19013_001E |
| `pop_poverty_universe` | int | C17002_001E |
| `pop_below_100_pov` | int | C17002_002E + C17002_003E |
| `pop_below_200_pov` | int | `pop_below_100_pov` + _004E + _005E + _006E + _007E |
| `pct_below_100_pov` | float | `pop_below_100_pov / pop_poverty_universe` |
| `pct_below_200_pov` | float | `pop_below_200_pov / pop_poverty_universe` |
| `geometry` | str (WKT, EPSG:4326) | TIGER cb shapefile |

### Null handling

Census uses sentinel values (e.g., `-666666666`) for "not available" — common in small
tracts with no income data. The script converts these sentinels to `NaN`. Derived
percentage columns return `NaN` when their denominator is zero or null.

### Output directory layout

```
data/processed/demographics/
└── us/
    ├── 2015.parquet     ← 2010-boundary era
    ├── 2016.parquet
    ├── 2017.parquet
    ├── 2018.parquet
    ├── 2019.parquet     ← last 2010-boundary vintage
    ├── 2020.parquet     ← first 2020-boundary vintage, COVID weighting
    ├── 2021.parquet
    ├── 2022.parquet
    ├── 2023.parquet
    └── 2024.parquet
```

The `boundary_year` column makes the 2010/2020 break explicit inside the data so the
HIA tool can pick the right boundary for any analysis year.

### Error handling & rerunnability

- Per-vintage script is idempotent: rerunning overwrites the Parquet file atomically.
- Census API calls retry with exponential backoff (3 tries).
- If any state fails after retries, the script aborts the whole vintage rather than
  writing a partial file. No silent holes.
- `--verbose` prints per-state progress and row counts.
- `pygris` geometry downloads are cached; repeated runs skip the download step.

## Dependencies

Add to `requirements.txt`:

- `cenpy` — Census API client (pin exact version during implementation)
- `pygris` — TIGER/Line shapefile downloader (pin exact version during implementation)

Likely already present (verify during implementation):

- `geopandas` — used by `backend/services/geo_processor.py`
- `pyarrow` — used by `process_pm25.py` for Parquet I/O

Add to `.env.example`:

```
CENSUS_API_KEY=your_key_here  # free at https://api.census.gov/data/key_signup.html
```

Add to `.env` documentation table in `README.md`:

| Variable | Default | Description |
|---|---|---|
| `CENSUS_API_KEY` | *(none)* | Required when running the ACS ETL script. Not read by the backend API at runtime. |

## Integration with existing backend

The existing `GET /api/data/datasets` endpoint auto-discovers datasets under
`data/processed/`. The new `demographics/us/{vintage}.parquet` files should appear
without any code changes, **provided** the endpoint recurses into subdirectories. This
assumption will be verified during implementation planning. If the discovery logic is
flat (only top-level directories), a small extension is in scope.

No changes to `backend/services/hia_engine.py`, the frontend, or the wizard are part of
this ETL work. Those are downstream follow-ups once the data exists.

## Performance expectations

- **ACS API fetch:** ~52 state requests × 3 tables = ~156 calls per vintage. With polite
  rate limiting, ~2–5 minutes per vintage.
- **TIGER geometry fetch:** First vintage downloads ~50 state shapefiles (~200 MB
  uncompressed); subsequent vintages hit the cache.
- **Full `--all` run:** ~20–40 minutes on a cold cache, ~10 minutes on a warm cache.
- **Output size:** Each Parquet file ~50–150 MB (nationwide tracts with WKT geometry).
  Total ~500 MB–1.5 GB for the full series.

## Testing

- Unit test: sentinel-value conversion (`-666666666` → `NaN`).
- Unit test: derived-column math with a small synthetic frame.
- Smoke test: run `--vintage 2022` against a single small state (e.g., Rhode Island) and
  assert row count matches the known tract count for that state and vintage.
- No hitting the live Census API in CI; smoke test is a manual step for the developer.

## Open questions / deferred

- **Does `GET /api/data/datasets` recurse?** To be verified during implementation.
- **Margins of error** — deferred to v2 if the HIA engine's Monte Carlo uncertainty
  path wants them.
- **Block group data** — deferred. Tract is enough for v1.
- **Historical vintages before 2015** — not in scope.
