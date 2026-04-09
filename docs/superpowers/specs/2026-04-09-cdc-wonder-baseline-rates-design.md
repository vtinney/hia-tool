# CDC Wonder Baseline Mortality Rates — Design

**Date:** 2026-04-09
**Status:** Draft for review
**Scope:** Replace hardcoded global mortality baseline incidence rates in the HIA
tool with US county-level rates sourced from CDC Wonder.

## Goal

Give US-based HIA runs defensible, geography-specific baseline mortality rates
(y0) instead of the global fallback values currently hardcoded in each CRF's
`defaultRate` field. The new rates must flow into the existing HIA engine
without disturbing its numerical core or breaking non-US analyses.

## Scope

**In scope:** Mortality endpoints in the CRF library that can be sourced from
the CDC Wonder Underlying Cause of Death databases.

**Out of scope:** Morbidity and healthcare-utilization endpoints (asthma ED
visits, cardiac/respiratory hospital admissions, T2D prevalence, asthma
incidence). These require different sources (HCUP, PLACES, BRFSS) and are
deferred to a later workstream. Their CRFs will continue to use the existing
global `defaultRate` values.

## CRF endpoints covered

The following 12 mortality endpoint strings from
`frontend/src/data/crf-library.json` collapse into 8 unique ICD-10 groups:

| CRF endpoint string | ICD group | ICD-10 codes |
|---|---|---|
| All-cause mortality | all_cause_nonaccidental | A00–R99 |
| All-cause mortality (non-accidental) | all_cause_nonaccidental | A00–R99 |
| All-cause mortality (short-term) | all_cause | A00–Y89 |
| Cardiovascular mortality | cvd | I00–I99 |
| Cardiovascular mortality (short-term) | cvd | I00–I99 |
| Ischemic heart disease | ihd | I20–I25 |
| Stroke (cerebrovascular) | stroke | I60–I69 |
| Respiratory mortality | respiratory | J00–J99 |
| Respiratory mortality (short-term) | respiratory | J00–J99 |
| COPD mortality | copd | J40–J44 |
| Lung cancer | lung_cancer | C33–C34 |
| Lower respiratory infection | lri | J09–J22 |

"Non-accidental" is synthesized from the CDC Wonder query (A00–R99 excluding
external causes S00–Y89). This matches BenMAP and HRAPIE convention.

## Age stratification

Three age buckets: `all`, `25plus`, `65plus`. Each of the 8 ICD groups is pulled
at all three buckets, for a total of 24 query shapes per year.

## Time period

Nine years, 2015–2023, stored per-year so the user can select the analysis
year at runtime. Split across CDC Wonder's two Underlying Cause of Death
databases:

- **2015–2017:** "UCD 1999–2020" (database handle `D76`, Bridged Race coding).
- **2018–2023:** "UCD 2018–2023, Single Race" (database handle `D158`).

No year pooling. No COVID differentiation — years are used as-is. Both
databases use ICD-10 for cause-of-death classification, so the 8 ICD group
definitions are identical across both; only the race coding differs, which
does not affect this work because we are not stratifying by race.

Total query count: 9 years × 8 ICD groups × 3 age buckets = **216 queries**.

## Suppression and missing-data handling

CDC Wonder suppresses any cell with 1–9 deaths (Assurance of Confidentiality).
Suppressed cells are mapped to 0. Counties not returned by CDC Wonder for a
given query are also mapped to 0. No state-level or national fallback is
applied at runtime — zero is a terminal value.

A state-level rollup parquet is produced as an auxiliary artifact for
sanity-checking and future use, but is not consulted at runtime.

## Download mechanism

A single ETL script at `backend/etl/process_cdc_wonder.py`:

1. Holds two XML request templates, one for database `D76` and one for `D158`,
   with placeholders for year, ICD group, and age bucket.
2. Iterates the 216 (database, year, ICD group, age bucket) combinations,
   POSTs each to the CDC Wonder XML endpoint, parses the returned TSV body,
   and writes raw responses to
   `data/raw/cdc_wonder/{database}/{year}/{icd_group}_{age}.tsv`.
3. Caches raw responses on disk. If a raw TSV already exists for a given
   combination, the HTTP call is skipped — this makes the script safely
   resumable after interruption or rate-limit backoff.
4. Rate-limits to ~1 request/second with exponential backoff on HTTP 429 and
   5xx responses.
5. Logs progress as `[i/216] {db} {year} {icd_group} {age} — OK ({n} rows)`.

A purpose-built thin client is used rather than the third-party
`cdc-wonder-api` PyPI wrapper, to avoid a dependency that lags the XML schema.

## Processed storage schema

**Primary artifact:** `data/processed/incidence/us/cdc_wonder_mortality.parquet`

One row per county × year × ICD group × age bucket:

| Column | Type | Notes |
|---|---|---|
| `fips` | string | 5-digit county FIPS, zero-padded |
| `state_fips` | string | 2-digit, for rollups and debugging |
| `year` | int16 | 2015–2023 |
| `icd_group` | category | all_cause, all_cause_nonaccidental, cvd, ihd, stroke, respiratory, copd, lung_cancer, lri |
| `age_bucket` | category | all, 25plus, 65plus |
| `deaths` | int32 | Raw count; suppressed and missing → 0 |
| `population` | int32 | CDC Wonder denominator for the matching stratum |
| `rate_per_person_year` | float32 | `deaths / population`; 0 where population is 0 |

Rate is precomputed so the HIA engine does not divide at request time. Deaths
and population are kept alongside the rate for auditability and to allow
downstream aggregation without re-querying CDC Wonder.

Using CDC Wonder's own population denominators (rather than WorldPop or ACS)
keeps numerator and denominator internally consistent with the death file's
bridged-race / single-race coding.

**Auxiliary artifact:** `data/processed/incidence/us/cdc_wonder_mortality_state.parquet`

Same schema but keyed by `state_fips` (no `fips` column). Produced by group-sum
from the county parquet. Not used at runtime; available for sanity checks and
future analyses.

## HIA engine integration

### New module: `backend/services/baseline_rates.py`

Small focused service exposing a single public function:

```python
def get_baseline_rate(
    crf_endpoint: str,
    year: int,
    fips: str | list[str] | None,
) -> float | np.ndarray | None
```

Behavior:

1. Looks up `(icd_group, age_bucket)` from a static module-level dict
   `CRF_ENDPOINT_TO_BASELINE: dict[str, tuple[str, str]]`.
2. If `crf_endpoint` is not in the dict (non-mortality CRF) → returns `None`.
3. If `fips` is `None` (non-US analysis) → returns `None`.
4. If `fips` is a scalar string → returns a single float from the parquet row
   matching `(fips, year, icd_group, age_bucket)`. Missing row → `0.0`.
5. If `fips` is a list or array → returns a NumPy array aligned to input
   order. Missing rows → `0.0`.
6. The parquet is loaded once into a module-level dict keyed by
   `(icd_group, age_bucket, year)` → `pandas.Series` indexed by `fips`, for
   O(1) per-lookup after the first load.

### CRF endpoint → (ICD group, age bucket) mapping

Lives as a static dict in `backend/services/baseline_rates.py`:

| CRF endpoint string | icd_group | age_bucket |
|---|---|---|
| All-cause mortality | all_cause_nonaccidental | 25plus |
| All-cause mortality (non-accidental) | all_cause_nonaccidental | 25plus |
| All-cause mortality (short-term) | all_cause | all |
| Cardiovascular mortality | cvd | 25plus |
| Cardiovascular mortality (short-term) | cvd | all |
| Ischemic heart disease | ihd | 25plus |
| Stroke (cerebrovascular) | stroke | 25plus |
| Respiratory mortality | respiratory | 25plus |
| Respiratory mortality (short-term) | respiratory | all |
| COPD mortality | copd | 25plus |
| Lung cancer | lung_cancer | 25plus |
| Lower respiratory infection | lri | all |

Rationale:
- Chronic PM2.5 CRFs use adult cohorts (25+), matching GBD/HRAPIE.
- Short-term / daily time-series CRFs use all-ages, matching their estimation
  cohorts.
- LRI uses all-ages because its burden includes children.
- COPD is set to 25+ for cohort consistency with the CRF β, even though the
  mortality burden is dominated by 65+.

### Router integration

The seam is `backend/routers/compute.py`, not `hia_engine.py`. Rationale:
- `hia_engine.py` stays a pure numerical module with no filesystem or parquet
  dependency, matching the JS twin at `frontend/src/lib/hia-engine.js`.
- The router is already the geography-aware layer — it builds the geometry,
  pulls population, and pulls concentrations. Pulling baseline rates alongside
  those is symmetric.

Changes:
- When the incoming request is a US analysis (country code `US` and a county
  FIPS list resolvable from the geometry), the router calls
  `get_baseline_rate(crf["endpoint"], year, fips_list)` for each selected CRF
  before passing the config into `compute_hia`.
- When the return value is not `None`, the router stamps it onto
  `crf["defaultRate"]` on a per-CRF copy (not mutating the library).
- When the return value is `None` (non-US, or non-mortality CRF), the router
  leaves the library's `defaultRate` untouched and the engine behaves exactly
  as it does today.

### Multi-county behavior

When an analysis spans multiple counties, `get_baseline_rate` returns a
NumPy array aligned to the input FIPS order. The HIA engine already broadcasts
y0 × pop in its spatial path, so no engine changes are needed. This is a
deliberate behavioral shift: y0 now varies by county instead of being a flat
per-run value. Output numbers for US runs will differ from pre-CDC-Wonder
runs; this is the point of the integration, not a regression.

## Fallback matrix

| Situation | Behavior |
|---|---|
| US analysis, county present, cell has data | County's computed rate is used |
| US analysis, county present, cell was suppressed | y0 = 0 → 0 cases for that county |
| US analysis, county missing from parquet | y0 = 0 → 0 cases for that county |
| Non-US analysis | Router does not call `get_baseline_rate`; engine uses global `defaultRate` |
| US analysis, non-mortality CRF | `get_baseline_rate` returns `None`; engine uses global `defaultRate` |

## Runbook

```bash
python -m backend.etl.process_cdc_wonder
```

- No arguments. Year range, ICD groups, and age buckets are module-level
  constants, matching `process_pm25.py` and `process_worldpop.py` conventions.
- Cold cache: ~4 minutes wall time (216 × ~1 s).
- Warm cache: seconds — the fetch loop skips any combination whose raw TSV
  already exists on disk.
- After fetching, runs consolidation: reads every raw TSV, normalizes columns,
  joins to the county FIPS master list, fills missing counties with zeros,
  writes the two processed parquets.
- Prints a summary on completion: total counties covered, total suppressed
  cells coerced to zero, and a one-line national sanity check (2019 all-cause
  mortality count within ±1% of the published NCHS value).

## Testing

Three pytest modules under `backend/tests/`:

1. `test_cdc_wonder_parser.py` — canned CDC Wonder TSV fixture → parser;
   asserts row count, `"Suppressed"` string → 0, and 5-digit FIPS padding.
2. `test_baseline_rates.py` — in-memory parquet with 3 known counties;
   exercises `get_baseline_rate` in scalar and array modes and verifies the
   `None`, `0.0`, and fallback branches.
3. `test_hia_engine_with_cdc_rates.py` — end-to-end router test with a mocked
   US request over 2 counties; asserts the engine output reflects differing
   y0 values per county rather than a single flat rate.

No tests hit the live CDC Wonder API.

## Non-goals / deferred work

- Morbidity endpoint baselines (PLACES, BRFSS, HCUP integration).
- Age-standardized rates or full 10-year age-band stratification.
- Year-pooled smoothed rates.
- State-level automatic fallback for suppressed cells (the auxiliary state
  parquet exists but is not consulted at runtime).
- Uncertainty quantification on y0 itself (baseline rates are treated as
  fixed-point values, matching current HIA engine behavior).
