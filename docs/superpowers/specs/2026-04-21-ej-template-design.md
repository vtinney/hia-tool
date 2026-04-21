# EJ-framed HIA template: tract-level Results rendering

**Date:** 2026-04-21
**Status:** Approved for planning

## Problem

The `feature/acs-demographics` branch landed the backend pieces for environmental justice framing: an ACS 5-year demographics parquet (85k US tracts; fields include `pct_minority` and `pct_below_200_pov`) and a FastAPI endpoint `/api/data/demographics/{country}/{year}` that returns tract GeoJSON with demographic fields, filterable by state / county FIPS. As of 2026-04-21, vintages 2015–2024 are all built on disk (~650 MB gitignored).

Nothing in the frontend consumes that data yet. `frontend/src/data/templates/us_tract_pm25_ej.json` exists as a template entry in `Home.jsx`'s "Start from a template" section, but selecting it runs a standard HIA with no EJ-specific rendering — no demographic overlay, no tract-level view, no aggregate EJ stats.

The project scope memory (`project_hia_acs_scope.md`) explicitly constrains v1 of the EJ framing:
- **Opt-in only** — standard HIAs must look unchanged.
- **Two fields** — `pct_minority` and `pct_below_200_pov` only. Aggregate stats outside the EJ format are out of scope.
- **No generic "EJ toggle" UI** until there's a defined EJ analysis format to attach it to.

A parallel workstream is already underway to shift the HIA engine to tract-resolution output (zonal statistics aggregating pollutant rasters to census tracts, CRF evaluated per tract, per-tract attributable cases with CIs). That work is owned by a separate agent; this spec depends on its output shape but does not design it.

## Goals

1. Make the existing EJ template functional: a user who clicks it gets an analysis that looks materially different from a standard HIA on the Results page.
2. Render tract-level demographic context on Results without disturbing standard HIAs.
3. Keep the wizard flow identical to the standard HIA — no new steps, no mode toggles, no Step 1 branching. The EJ-ness of an analysis is carried in the template config alone.
4. Avoid collision with the parallel agent's engine / tract-resolution work and the in-progress Step 1 / YearField changes on master.

## Non-goals (deferred — captured in Obsidian System Map > "HIA Tool - EJ Template Phase 2 Backlog")

- Custom polygon or uploaded boundary study areas under EJ framing.
- Demographics overlay on the Step 1 map.
- Comparison baselines on Results ("vs state / vs national").
- Additional ACS fields (age, language, education, median household income).
- Stratified HIA outcomes (mortality-by-demographic-group).
- Exposure-weighted demographics.
- Aggregate stats helpers in `hia_engine.py`.
- Non-US countries.
- Vector tiles, FastAPI lifespan migration, FIPS regex validation, parquet indexes.

## Design decisions

### D1. EJ framing is a template property, not a mode

The EJ template (`frontend/src/data/templates/us_tract_pm25_ej.json`) gains a top-level marker:

```json
"ejFraming": true
```

No other template carries the flag. `useAnalysisStore`'s `loadFromTemplate` reads it and sets a top-level `ejFraming: boolean` store field (default `false`). The field persists with the rest of the wizard state. `reset()` clears it to `false`.

**Rationale:** Keeps the "EJ-ness" as a pure property of the analysis config, not a global toolkit mode. Home.jsx needs zero changes — the EJ template card is already there.

### D2. No wizard-level constraints on EJ

Steps 1–6 remain identical. No Step 1 country lock, no study-area-type restriction, no EJ-specific validation. The template pre-fills `studyArea.type = 'country'`, `id = 'united-states'`. If the user edits these after loading the template, the Results-page gate silently hides the EJ section.

**Rationale:** Zero branching inside the wizard. The parallel agent's in-progress Step 1 / YearField / store changes are untouched by this work.

### D3. Results page: implicit gate, not explicit mode

`Results.jsx` conditionally renders a new `<EJContextSection />` iff **all** of the following hold:

1. `ejFraming === true` in the store.
2. `step1.studyArea.id` identifies a US geography (country `'united-states'`, or a US state/county id).
3. `step1.studyArea.type ∈ {'country', 'state', 'county'}` — admin boundary, maps 1:1 to the demographics endpoint.
4. The analysis payload contains `per_tract_results` (from the parallel agent's engine work).

If any gate fails, Results renders exactly as for a standard HIA. No warnings, no ghost UI.

**Rationale:** Silent degradation is simpler than wizard enforcement. Users who customize away from EJ-compatible settings get a standard HIA result; users who stay within the gates get the EJ section.

### D4. Aggregation is population-weighted, tract-level

The two headline numbers on the Results page — **Minority population share** and **Population below 200% poverty line** — are computed as:

```
pop_weighted_mean(field) = Σ(tract.population × tract[field]) / Σ(tract.population)
```

…across tracts present in the study area's demographics response, filtered to tracts where `tract[field]` is not `NaN`. Tracts with suppressed ACS values drop from the denominator for that field.

Rationale: matches how EPA EJScreen and most EJ reporting framing aggregate numbers ("among the people living in this study area, X% are minority"). Simpler than exposure-weighted aggregation (v2+) and avoids epi choices the team hasn't committed to.

### D5. Vintage selection

`pickVintage(analysisYear, availableVintages)`:

1. If exact match is available, use it.
2. Else pick the closest vintage **on the same side of the 2020 tract-boundary redraw**. In practice: if `analysisYear ≤ 2019`, prefer 2019, 2018, 2017 in that order; if `analysisYear ≥ 2020`, prefer 2020, 2021, 2022, … up to the max available.
3. The provenance footer on the Results page always shows the vintage actually used.

**Rationale:** 2015–2019 use the pre-2020 tract geometry (~73.7k tracts); 2020+ use the post-decennial redraw (~85k tracts). Falling back across that boundary would silently mismatch tract FIPS, distorting the choropleth and the aggregate numbers.

### D6. Tract-level choropleth as the map view

On the Results page, the EJ section includes a `<TractChoroplethMap />` (MapBox / MapLibre GL) that renders the tract GeoJSON returned by the demographics endpoint. Each tract is styled by one field at a time — a small toggle switches between `pct_minority` and `pct_below_200_pov`. Hover reveals the tract FIPS, total population, and the exact value.

The same tract geometries serve both the demographic choropleth and the HIA outputs (via `per_tract_results`, joined by `tract_fips`). This gives the user tract-level EJ context **and** tract-level HIA outputs on the same map — which is the core product value of the EJ template.

### D7. Geographic filtering reuses the existing endpoint

The frontend derives `state` / `county` FIPS from `step1.studyArea`:

- `studyArea.type === 'country'` → no `state` / `county` filter (full-US response, ~13 MB, accepted for v1).
- `studyArea.type === 'state'` → `state` = 2-digit FIPS derived from `studyArea.id`.
- `studyArea.type === 'county'` → `state` + `county` FIPS derived from `studyArea.id`.

No backend changes required.

## Architecture

### Frontend components

| File | Change |
|---|---|
| `frontend/src/pages/Home.jsx` | **No changes.** EJ template card already present. |
| `frontend/src/data/templates/us_tract_pm25_ej.json` | Add `"ejFraming": true` top-level. |
| `frontend/src/stores/useAnalysisStore.js` | Add top-level `ejFraming: false`. Update `loadFromTemplate` to propagate `config.ejFraming ?? false`. Update `reset()` accordingly. Include in persisted state. |
| `frontend/src/pages/Results.jsx` | Conditionally render `<EJContextSection />` per D3 gates. |
| `frontend/src/components/EJContextSection.jsx` | **New.** Fetch + join + aggregate + render. |
| `frontend/src/components/TractChoroplethMap.jsx` | **New.** Field-agnostic choropleth. |
| `frontend/src/lib/api.js` | Add `fetchDemographics(country, year, { state, county, simplify })`. |
| `frontend/src/lib/demographics.js` | **New.** Pure functions: `populationWeightedMean(tracts, field)`, `pickVintage(analysisYear, availableVintages)`. |

### Backend

No changes.

### Data flow

```
Home.jsx (EJ template card click)
   │
   ▼
useAnalysisStore.loadFromTemplate(tpl)
   │  sets ejFraming: true; step1..7 pre-filled
   ▼
Wizard (Steps 1–6, identical to standard)
   │  user may customize; no EJ-specific branching
   ▼
Step 6 run → backend analysis
   │  engine (parallel agent's work) returns per_tract_results
   ▼
Results.jsx (renders standard tabs)
   │
   ▼ [gate: ejFraming && US admin boundary && per_tract_results present]
EJContextSection
   │  1. pickVintage(step2.baseline.year, availableVintages)
   │  2. fetchDemographics('us', vintage, { state, county })
   │  3. join demographic GeoJSON to per_tract_results by tract_fips
   │  4. populationWeightedMean for pct_minority and pct_below_200_pov
   │  5. render headline stats + TractChoroplethMap + provenance footer
```

### Interface with parallel-agent engine work

This spec depends on the analysis payload carrying `per_tract_results` when the engine is run at tract resolution. The placeholder shape used during implementation:

```ts
per_tract_results: Array<{
  tract_fips: string;          // 11-digit GEOID
  population: number;
  baseline_concentration: number;
  control_concentration: number;
  attributable_cases: {
    mean: number;
    lower95: number;
    upper95: number;
  };
}>
```

**TODO at implementation start:** confirm this shape against the parallel agent's actual output. Differences are absorbed by the join adapter inside `EJContextSection`; `TractChoroplethMap` is intentionally field-agnostic. If the engine work hasn't landed when EJ implementation begins, tests and development proceed against a mocked shape, and the adapter is aligned at integration time.

## Error handling & edge cases

### Data quality

- **Suppressed (`NaN`) ACS values.** `populationWeightedMean` skips tracts where the target field is `NaN`. Choropleth renders those tracts with a neutral hatched "no data" fill.
- **Zero-population tracts.** Weight = 0 → naturally drop out. Denominator is the sum of populations across non-`NaN` tracts for that field, so no division-by-zero.
- **2020 tract-boundary redraw.** Handled in D5 via same-side vintage fallback.

### Join integrity

- **Demographic tract not in `per_tract_results`.** Rendered greyed-out on the choropleth. Excluded from aggregate stats (we only weight across tracts present in the HIA output).
- **`per_tract_results` tract not in demographics.** Choropleth styles it as "no demographic data." Doesn't break the join.
- **`per_tract_results` absent from payload** (engine fell back to zone resolution). EJ section renders a banner: *"EJ context requires tract-resolution output; this analysis ran at zone resolution."* Falls back to no choropleth and no aggregates. Never attempts to aggregate zone results as if they were tracts.

### Endpoint failures

- **Network / 5xx.** Inline error inside the EJ section with a **[Retry]** button. Standard HIA results still render fully.
- **404 for vintage.** `pickVintage` should prevent this; if a built vintage goes missing, fall back to the next-nearest on the same side of 2020.
- **Country-level US fetch (~13 MB).** Accepted for v1. Revisit if slow in practice.

### User workflow edges

- **User loads EJ template, edits country away from US.** D3 gate fails on country check → EJ section silently disappears.
- **User loads EJ template, switches study-area type to custom polygon.** D3 gate fails on type check → EJ section silently disappears. A ghost hint ("EJ framing requires an admin-boundary study area") is a v2 polish item.
- **User loads EJ template, then a non-EJ template.** `loadFromTemplate` resets store state before applying the new config; non-EJ templates don't carry `ejFraming`, so the flag resets to `false`.
- **User starts directly from "Start analysis" button (no template).** Initial state has `ejFraming: false`. EJ section never renders.

## Testing

### Unit (Vitest)

- `lib/demographics.test.js — populationWeightedMean`: all valid, all-`NaN` for target (returns `null`), mixed valid + suppressed, zero populations, empty array.
- `lib/demographics.test.js — pickVintage`: exact match, before range, after range, 2019→2019 same-side fallback, 2020+ gap, no vintages available.

### Component (React Testing Library)

- `EJContextSection` renders headline stats + choropleth + footer given mocked demographics + mocked `per_tract_results`.
- `EJContextSection` renders the zone-resolution fallback banner when `per_tract_results` is absent.
- `EJContextSection` renders inline error + retry on fetch failure.
- `TractChoroplethMap` field toggle updates the styled layer.

### Integration / E2E

- Loading the EJ template from Home sets `ejFraming: true`.
- Loading a non-EJ template keeps `ejFraming: false`.
- Editing country away from US after loading EJ template hides the section on Results.
- Running a standard template never renders the EJ section.

### Manual verification before merge

- End-to-end run against 2022 data using the EJ template.
- Sanity-check headline numbers against known county demographics (e.g., Harris County, TX ≈ 60–65% minority, ≈ 40% below 200% poverty).
- State-level choropleth (~3–5k tracts) renders without jank.
- Field toggle updates the map layer.
- Provenance footer matches `pickVintage` output.
- Standard (non-EJ) template looks visually unchanged on Results.

## Rollout

- Single short-lived branch `feature/ej-template-results` → PR into `master`.
- No feature flag — entry point is the template card, a deliberate user action.
- No database migrations, new dependencies, env variables, or infrastructure changes.
- Depends on the parallel agent's tract-resolution engine work landing first (or at minimum, a stable `per_tract_results` shape). If the engine work isn't ready at implementation time, proceed with mocked `per_tract_results` and align the adapter at integration.

## Related

- Backend that this spec consumes: `backend/routers/data.py` (demographics endpoint), `backend/etl/process_acs.py` (ETL).
- `docs/ACS_NEXT_STEPS.md` — mechanical next-steps for ACS (backfill now complete: all 10 vintages 2015–2024 on disk as of 2026-04-21).
- `docs/superpowers/specs/2026-04-08-acs-demographics-etl-design.md` — original ACS ETL design.
- Obsidian: `System Map > HIA Tool - EJ Template Phase 2 Backlog` — v2+ deferred items.
- Memory: `project_hia_acs_scope.md` — scope decisions this spec implements.
