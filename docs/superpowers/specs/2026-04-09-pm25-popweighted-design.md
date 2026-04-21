# Design — Van Donkelaar PM2.5 × WorldPop Population-Weighted Summaries

**Date:** 2026-04-09
**Status:** Approved for planning
**Author:** HIA tool project

## Goal

Produce population-weighted and unweighted annual PM2.5 summaries (2015 – latest Van Donkelaar year) for every boundary feature in the HIA tool's nine uploaded boundary sets, plus matching WorldPop total and age-structured population counts. Output long-format Parquet files for ingestion by the HIA tool.

## Inputs

| Input | Asset / Collection | Years used |
|---|---|---|
| PM2.5 (annual surface) | `projects/sat-io/open-datasets/GLOBAL-SATELLITE-PM2-5/ANNUAL-MEAN/V5GL04` | 2015 → latest (expected 2022) |
| Population (100 m, age × sex) | `projects/sat-io/open-datasets/WORLDPOP/agesex` (sat-io community mirror) | 2015 → latest PM2.5 year, annual, no fallback |
| Natural Earth countries | `projects/hia-tool/assets/ne_countries` | — |
| Natural Earth states/provinces | `projects/hia-tool/assets/ne_states` | — |
| GHS urban (vector, one row per city) | `projects/hia-tool/assets/GHS_SMOD` | — |

Total: **3 boundary feature collections** (GADM deferred — dataset is too
confusing to integrate in this pass).

## Processing logic (per year)

1. `pm25 = Van Donkelaar image for year`, band `PM2.5`.
2. `wp = sat-io WORLDPOP/agesex image for year` (collection covers 2015–2030 annually, no fallback needed).
3. Resample `wp` → PM2.5 grid (~0.01°) with `reduceResolution(ee.Reducer.sum())` followed by `reproject(pm25.projection())`. The sum reducer preserves population totals across the downsampling.
4. Build a multi-band image:
   - `pop_total` ← resampled `population` band
   - `age_0, age_1, age_5, age_10, …, age_90` ← `m_x.add(f_x)` for each of the 20 WorldPop age bins (0–1, 1–4, 5–9, …, 85–89, 90+), using lowercase zero-padded sat-io band names (`m_00`, `f_05`, …), resampled the same way
   - `pop_total` ← per-pixel sum across all 20 age bands (equivalent to the WorldPop total since age bins cover 100% of the population)
   - `pm25_x_pop = pm25.multiply(pop_total)`
5. `reduceRegions` over each boundary FeatureCollection with `ee.Reducer.sum()` over `[pm25_x_pop, pop_total, age_0, …, age_90]`.
6. A separate `reduceRegions` with `ee.Reducer.mean()` over `pm25` gives the unweighted spatial mean for reference.
7. Post-compute (in the local Parquet script): `pm25_popweighted = pm25_x_pop_sum / pop_total_sum`.

## Script structure

### GEE script: `pm25_popweighted.js`

```
CONFIG (years, asset IDs, export folder)
loadPM25(year)            → ee.Image
loadWorldPop(year)        → ee.Image  (handles 2021-22 fallback to 2020)
prepAgeBands(wp)          → ee.Image  (pop_total + 20 age bands)
alignToPM25(img, pm25)    → reduceResolution + reproject
computeStatsForYear(boundaries, year) → ee.FeatureCollection
processBoundarySet(assetId, name)     → loops years, merges, exports CSV
MAIN: loop over 9 boundary assets, call processBoundarySet
```

**Exports** (to Google Drive folder `hia_tool_pm25/`):
`pm25_ne_countries.csv`, `pm25_ne_states.csv`, `pm25_ghs_smod.csv`

### Local Python script: `hia-tool/scripts/pm25_csv_to_parquet.py`

- Reads each CSV from a download folder (configurable path).
- Writes `hia-tool/data/processed/pm25_{name}.parquet` via pandas + pyarrow.
- Validates columns, coerces dtypes, and computes `pm25_popweighted` from the sum columns.

## Output schema (long format; one Parquet file per boundary set)

| Column | Type | Notes |
|---|---|---|
| `feature_id` | string | From the boundary's native ID field |
| `name` | string | From the boundary's name field |
| `year` | int | 2015–2022 |
| `pop_total` | float64 | All ages, both sexes, summed within feature |
| `age_0` | float64 | Ages 0–1 (m_00 + f_00), summed within feature |
| `age_1` | float64 | Ages 1–4 |
| `age_5` | float64 | Ages 5–9 |
| … | … | 5-year bins |
| `age_85` | float64 | Ages 85–89 |
| `age_90` | float64 | Ages 90+ |
| `pm25_mean` | float64 | Unweighted spatial mean (µg/m³) |
| `pm25_popweighted` | float64 | Σ(pm25·pop)/Σ(pop) (µg/m³) |
| `pop_source_year` | int | WorldPop year used (equals `year` for 2015–2030 coverage) |

## Risks / gotchas

- **High-cardinality boundary sets.** `ne_states` and `GHS_SMOD` have thousands of features but generally fit within GEE's default compute limits. If any export times out, bump `tileScale` from 8 to 16 in `CONFIG`.
- **`reduceResolution` input pixel limit.** Default 256 input pixels per output pixel. WorldPop 100 m → PM2.5 ~1 km is ~100:1, comfortably under. If PM2.5 grid is ever coarsened, bump `maxPixels`.
- **Property name collisions.** `reduceRegions` can collide with existing feature properties. Drop non-essential properties from the boundary FC before reducing, and prefix sum outputs with `sum_` if needed.
- **Van Donkelaar end year.** If the V5GL04 collection only runs through 2021, the script stops there automatically (years loop is driven by the collection's actual date range).

## Out of scope

- Sex-disaggregated population (user wants age only).
- Years before 2015.
- Other PM2.5 datasets (ACAG versions beyond V5GL04).
- Health impact calculations — this spec only produces exposure/population inputs.
