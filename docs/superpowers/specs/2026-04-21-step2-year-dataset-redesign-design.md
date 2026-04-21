# Step 2 year-vs-dataset redesign and multi-year runs

**Date:** 2026-04-21
**Status:** Approved for planning

## Problem

The wizard currently lets users pick a country and pollutant in Step 1, then advance to Step 2 without selecting a year. In Step 2's "Built-in Data" tab, the year is silently defaulted to the current calendar year (`new Date().getFullYear()`), and every dataset option sends the request backend-ward with that year. When the user tries Mexico + PM2.5:

- `data/processed/pm25/` is empty on disk — no direct Mexico concentration raster exists.
- EPA AQS fallback is US-only.
- WHO AAP fallback files exist for 2015-2021, but the Mexico row only appears in 2015-2018; 2019-2021 drop it.

The combined effect is that **every built-in dataset option returns 404 for Mexico**, regardless of what the user clicks. The UI shows a generic "Built-in data not yet available" message that doesn't tell the user what *would* work.

A related UX concern: the built-in dropdown seeds from a hardcoded `BUILTIN_DATASETS` list of six labels (GBD 2019, ACAG, WHO AAP, EPA AQS, CAMS, OpenAQ) that don't correspond to actual backend files. The real `fetchDatasets` result replaces them once loaded, but the placeholder list misleads users about what's available.

## Goals

1. Make dataset availability visible *before* the user commits to a click.
2. Constrain year selection to years the chosen dataset actually covers.
3. Let users who upload their own data specify the year.
4. Support trend analysis across 2-10 discrete years — but structurally bias toward single-year runs so users don't over-scope casually.

## Design decisions

### D1. Remove year from Step 1

Step 1 drops its year picker entirely. Step 1 becomes: country, pollutant, optional custom boundary, analysis name/description. Year is set inside Step 2 alongside the concentration dataset.

**Rationale:** Year only becomes a meaningful choice once a dataset is picked (each dataset has its own year coverage). Picking year first forces users to either guess or reverse-engineer from dataset availability.

### D2. Dataset before year in Step 2

Inside each baseline/control scenario section, the order is:

1. Data source tab (Manual / Upload / Built-in).
2. Source-specific control (dataset dropdown, file dropzone, or manual value).
3. Year picker — only rendered after a source has been chosen, constrained to valid years for that source.

Year picker behavior by source:

- **Built-in:** dropdown lists only the years the chosen dataset covers *for the study country*. The label of each dataset in the dataset dropdown includes this coverage inline: `WHO AAP — PM2.5 (Mexico, 2015-2018)`.
- **Upload:** after the file is accepted, an integer year input appears; required before Step 2 is valid.
- **Manual:** year dropdown shows all years in `YEAR_MIN..YEAR_MAX` (no constraint).

### D3. Real dataset availability, no placeholders

The built-in dropdown is populated exclusively from `fetchDatasets({pollutant, type: 'concentration'})`. Drop the `BUILTIN_DATASETS` constant. Until the fetch resolves, show a loading indicator, not fake options.

The backend's `_scan_datasets` already walks `data/processed/` and emits real entries. Extend the entry shape to also include `countries_covered: string[]` for WHO-AAP-style global datasets so the frontend can compute "this dataset exists but not for your country" states.

When the filtered list is empty for the chosen pollutant+country, render an actionable message in place of the dropdown:

> "No built-in PM2.5 data covers Mexico on disk. Try another pollutant, upload a file, or enter a manual value."

### D4. Year cascade to Steps 3 and 4

Step 2's `baseline.year` becomes the *default* for:

- `step2.control.year` (control scenario concentration),
- `step3.year` (population),
- `step4.year` (incidence).

Each downstream field renders its own year picker prefilled with the baseline year. If the user changes a downstream year, show an inline badge next to the changed field: "Differs from baseline concentration year (2018)." Not an error — just a visibility signal, since mismatched years are usually a mistake but occasionally deliberate.

### D5. Upload year applies to all three data steps

Steps 2 (concentration), 3 (population), and 4 (incidence) all support file uploads. All three get a year input alongside the file dropzone, required before that step is valid.

### D6. Multi-year trend is a post-results action, not a Step 2 option

Step 2 only accepts a single year. Multi-year trend is entered from the results page:

- A "Compare another year" card sits below the primary results.
- Clicking it opens a year picker constrained to years the original datasets still cover.
- Submitting clones the full analysis config with one change: every year field across Steps 2/3/4 shifts to the new year (baseline concentration, control concentration, population, incidence). Downstream year overrides from the first run are discarded — multi-year trend analysis assumes year-aligned inputs by design.
- Backend then runs a new compute. Results accumulate as a list: `[{runId, year, ...results}]`, one card per run, year-labeled.
- Hard cap: 10 total runs per analysis.
- The first run *past* 2 triggers a one-time confirmation: "Running multi-year trends is only useful for policy comparisons or robustness checks. Most analyses don't need this — continue?"

**Rationale:** Surfacing multi-year only after a single result exists means every additional year is a deliberate post-hoc decision made with real numbers already on screen. Casual users never see the multi-year machinery.

### D7. Smoothing (range averaging) is out of scope

A range picker that averages concentrations across years (e.g., 2015-2018 mean) is not included in this round. If added later, it fits naturally as a variant of the Step 2 year picker without disturbing the structure established here.

## Data model changes

```js
// BEFORE
step1: { studyArea, pollutant, years: {start, end}, ... }
step2: { baseline: {..., datasetId, value}, control: {...} }
step3: { ..., populationType, totalPopulation }
step4: { ..., rates }
results: { /* single result object */ }

// AFTER
step1: { studyArea, pollutant, /* years removed */, ... }
step2: { baseline: {..., datasetId, value, year}, control: {..., year} }
step3: { ..., year /* prefilled from step2.baseline.year */ }
step4: { ..., year /* prefilled from step2.baseline.year */ }
results: [
  { runId, year, datasets, computeResult },
  // additional runs appended by "Compare another year"
]
```

Zustand store version bumps (`useAnalysisStore.js` version field). Migration: discard persisted state older than the new version to avoid shape mismatches.

## Backend changes

- `_scan_datasets` in `backend/routers/data.py`: add `countries_covered` to WHO AAP and EPA AQS entries by reading the `admin_id` column once and listing distinct ISO-3 codes (or `US-XX` state codes). This lets the frontend render accurate per-country coverage labels without probing year files one by one.
- No new endpoints for multi-year. The results page issues N serial calls to the existing single-year compute endpoint. A future change can batch them if latency matters.

## UX flow walkthrough

User picks Mexico + PM2.5 in Step 1, advances to Step 2.

1. Baseline section shows three tabs (Manual / Upload / Built-in); no year picker yet.
2. User clicks Built-in. Dropdown loads, shows `WHO AAP — PM2.5 (Mexico, 2015-2018)` (the only dataset that covers Mexico).
3. User selects it. Year dropdown appears below with 2015-2018.
4. User picks 2018. Concentration loads and the "Data loaded successfully" card shows.
5. Step 3 inherits 2018 as its default; Step 4 does the same.
6. User runs the analysis. Results appear.
7. Below results, a "Compare another year" card appears. User clicks it, picks 2015. A second result card appears labeled "2015" alongside the first labeled "2018".

## Non-goals and deferred

- Trend chart visualization across multiple run years (side-by-side cards only in this round).
- Smoothing via range averaging.
- Background job queue for long-running or large batches.
- Multi-country or multi-pollutant fan-out.
- Changing the `step1.pollutant` schema to support multiple pollutants.

## Open risks

- **Step 3/4 year pickers add wizard complexity.** Mitigation: prefilled with Step 2's year and visually subdued (no "select year" prompt) so users who don't care never notice.
- **Results-as-list breaks the current single-result components.** Mitigation: wrap existing result components in a list renderer that defaults to rendering a single run; multi-run styling is cosmetic.
- **`countries_covered` metadata requires reading every WHO AAP / EPA AQS parquet at scan time.** Mitigation: cache the scan result; it's already not cached today but the surface of files is small.
