# Make the built-in US EJ (Census Tract) analysis work end-to-end

**Date:** 2026-06-11
**Status:** approved

## Problem

The "U.S. Census Tract PM₂.₅ (Environmental Justice)" template uses **built-in**
US data (no uploads). Step 6 only calls the backend spatial endpoint
(`runSpatialCompute`) when **upload IDs** are present; otherwise it runs the
client-side scalar engine, which returns a single scalar and **no per-tract
zones**. So the EJ section — gated on per-tract results — can never render from
the template, regardless of analysis level. Additionally, the EJ template does
not set an analysis level, and US runs default to **State** level, so a user
who does reach a spatial path would still be at the wrong grain.

## Goal

A user picks the EJ template, selects a state, runs, and sees the Environmental
Justice Context section populated with that state's tract-level demographics.

## Design

### 1. Route built-in US tract runs to the backend (core fix)
In `Step6Run.handleRunAnalysis`, add a branch **before** the scalar fallback:

> If `studyArea.id === 'USA'` AND `studyArea.analysisLevel === 'tract'` AND
> `step2.baseline.type === 'dataset'` (built-in) → build a `mode:'builtin'`
> request and `runSpatialCompute(...)`.

Request shape (from a pure helper `buildBuiltinSpatialConfig`):
```js
{
  mode: 'builtin',
  pollutant: step1.pollutant,
  country: 'us',                 // map studyArea.id 'USA' → 'us'
  year: step2.baseline.year,
  analysisLevel: 'tract',
  stateFilter: studyArea.stateId,
  controlMode: 'benchmark',
  controlConcentration: step2.control?.value ?? 0,   // counterfactual default = total burden
  selectedCRFs: [...minimal CRF fields...],
  ...(step6.monteCarloIterations >= 100 ? { monteCarloIterations } : {}),
}
```
The response carries `zones` (11-digit tract GEOIDs). `tractResultsFromResponse`
(already shipped) maps them → `{tract_fips}`, the EJ gate passes, and the EJ
section renders. `Results.jsx` already handles the spatial `zones` shape — no
results-page changes.

### 2. Tract requires a state
All-US tract (~85k tracts) is infeasible. Step 1 validation: when
`analysisLevel === 'tract'` for the US, require `studyArea.stateId`; block the
run with a clear message otherwise.

### 3. EJ template auto-selects tract
Add `analysisLevel: 'tract'` (and `type: 'tract'`, `stateId: ''`,
`stateName: ''`) to `us_tract_pm25_ej.json` `step1.studyArea` so it loads at the
right grain.

### 4. Instruction (Step 1, only when `ejFraming`)
- Note by the analysis-level radios: "Environmental Justice analysis runs at
  Census Tract level — pick a state to analyze its tracts."
- Amber warning if `ejFraming` and `analysisLevel !== 'tract'`.
- Amber note if tract is selected with no `stateId`.

### Scope
Route **only built-in US tract** runs to the backend. Built-in state/county
stays on the scalar path (unchanged) to avoid regression; extending those to the
backend is a clean follow-up.

## Components & data flow
- `lib/builtinSpatial.js` (new): pure `buildBuiltinSpatialConfig(step1, step2, step6, crfs)`.
- `Step6Run.jsx`: new dispatch branch using the helper.
- `Step1StudyArea.jsx`: validation + `ejFraming` instruction/warnings.
- `data/templates/us_tract_pm25_ej.json`: `analysisLevel: 'tract'`.

## Error handling
- No state on a tract run → blocked in Step 1 with a message (not a backend error).
- Backend errors surface via the existing `try/catch` → `setError` in Step 6.

## Testing
- TDD `buildBuiltinSpatialConfig`: tract level, `USA→us` mapping, `stateFilter`
  passthrough, counterfactual default 0, CRF field projection, MC inclusion rule.
- The dispatch branch is thin wiring atop the tested helper.
- Manual/live: a US tract run for a state returns `zones` and the EJ section renders.

## Out of scope
- Built-in state/county → backend routing.
- Non-US (adm2) Step 6 wiring.
- Multi-year, PDF export.
