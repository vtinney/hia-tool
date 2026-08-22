// Built-in analyses that run on the backend spatial engine (mode:'builtin')
// rather than the client-side scalar engine, so they return per-zone `zones`
// — real reporting units with their own population and concentration, and
// (at US tract level) the per-tract results the Environmental Justice context
// section needs. This module decides when that path applies and builds the
// request.
//
// Three families route here:
//   - US admin grains (tract / county / state), resolved from ACS tracts.
//   - Non-US admin-2 (GADM), resolved from the global admin-2 parquet.
//   - Non-US urban centres (GHS-SMOD), resolved from the global urban layer.
// US country level stays on the scalar path: dissolving all ~85k US tracts
// into one polygon is slow and a national scalar is just as accurate. Non-US
// "country average" likewise stays scalar (the legacy WHO AAP fallback).

import crfLibrary from '../data/crf-library.json'

const US_SPATIAL_LEVELS = new Set(['tract', 'county', 'state'])

// endpoint label → every library CRF id sharing it. Step 4 keys baseline
// rates by the endpoint's *first* library CRF; a run may select a sibling
// CRF (different framework, same endpoint), and the stored rate must still
// resolve for it.
const IDS_BY_ENDPOINT = crfLibrary.reduce((acc, c) => {
  (acc[c.endpoint] ||= []).push(c.id)
  return acc
}, {})

/**
 * Resolve the baseline rate for each CRF: its own stored rate first, then
 * a rate stored under any same-endpoint sibling, then the library default.
 *
 * @param {object} rates - Step-4 rates keyed by CRF id.
 * @param {Array<object>} crfs - Selected CRF definitions.
 * @returns {object} Map of crf.id → resolved rate.
 */
export function resolveRatesForCRFs(rates, crfs) {
  const out = {}
  for (const crf of crfs || []) {
    let rate = rates?.[crf.id]
    if (rate == null) {
      for (const sibling of IDS_BY_ENDPOINT[crf.endpoint] || []) {
        if (rates?.[sibling] != null) { rate = rates[sibling]; break }
      }
    }
    out[crf.id] = rate ?? crf.defaultRate
  }
  return out
}

/**
 * True when the current run should be routed to the backend `mode:'builtin'`
 * spatial endpoint instead of the in-browser scalar engine:
 *
 *   - built-in US admin-level analysis (tract, county, or state). A state
 *     must be selected: the Step 1 UI only exposes the analysis-level radios
 *     once a state is picked, and the backend needs a state filter to avoid
 *     an all-US dissolve.
 *   - non-US admin-2 analysis (GADM). Step 1 defaults the non-US radio to
 *     adm2, so a study area without analysisLevel routes as adm2 to match
 *     what the UI displays.
 *   - non-US urban-centre analysis (GHS-SMOD). Selected cities route to the
 *     backend with their IDs; empty selection defaults to all centres.
 *
 * @param {object} step1 - Study-area step state.
 * @param {object} step2 - Concentration step state.
 * @returns {boolean}
 */
export function shouldUseBuiltinSpatial(step1, step2) {
  if (step2?.baseline?.type !== 'dataset') return false
  const area = step1?.studyArea
  if (!area?.id) return false
  if (area.id === 'USA') {
    return US_SPATIAL_LEVELS.has(area.analysisLevel) && Boolean(area.stateId)
  }
  const level = area.analysisLevel || 'adm2'
  return level === 'adm2' || level === 'urban'
}

/**
 * Build the `/api/compute/spatial` request body for a built-in run.
 * Assumes shouldUseBuiltinSpatial() already returned true.
 *
 * @param {object} step1 - Study-area step state (pollutant, studyArea).
 * @param {object} step2 - Concentration step state (baseline, control).
 * @param {object} step6 - Run step state (monteCarloIterations).
 * @param {Array<object>} selectedCRFs - Resolved CRF definitions.
 * @param {object} [rates] - Step-4 baseline rates keyed by CRF id; when a
 *   CRF has a numeric rate here it overrides the library defaultRate, so
 *   the engine uses the rates the wizard displays.
 * @returns {object} Spatial compute request.
 */
export function buildBuiltinSpatialConfig(step1, step2, step6, selectedCRFs, rates = {}) {
  const resolvedRates = resolveRatesForCRFs(rates, selectedCRFs)
  const isUS = step1?.studyArea?.id === 'USA'
  const nonUSLevel =
    step1?.studyArea?.analysisLevel === 'urban' ? 'urban' : 'adm2'
  const config = {
    mode: 'builtin',
    pollutant: step1?.pollutant,
    // Backend country: 'us' slug for US runs; ISO3 passthrough for the
    // admin-2 path (the resolver normalizes slugs and ISO3 alike).
    country: isUS ? 'us' : step1?.studyArea?.id,
    year: step2?.baseline?.year ?? null,
    // US: real grain from Step 1 (tract / county / state); default to tract
    // so an older persisted study area without analysisLevel still resolves
    // safely. Non-US: adm2 or urban depending on what was selected.
    analysisLevel: isUS
      ? (step1?.studyArea?.analysisLevel || 'tract')
      : nonUSLevel,
    stateFilter: (isUS && step1?.studyArea?.stateId) || null,
    countyFilter: (isUS && step1?.studyArea?.countyId) || null,
    controlMode: 'benchmark',
    // Unset counterfactual → total burden (0 µg/m³), consistent with the
    // scalar engine's default.
    controlConcentration: step2?.control?.value ?? 0,
    selectedCRFs: (selectedCRFs || []).map((crf) => ({
      id: crf.id,
      source: crf.source,
      endpoint: crf.endpoint,
      beta: crf.beta,
      betaLow: crf.betaLow,
      betaHigh: crf.betaHigh,
      functionalForm: crf.functionalForm,
      // Step-4 rate wins when set — the engine must use the baseline rate
      // the wizard displays, not silently fall back to the library value.
      defaultRate: resolvedRates[crf.id],
      // Backend defaults these to all_cause/mortality when absent, which
      // collapses the totalDeaths/allCauseDeaths split — always pass them.
      cause: crf.cause,
      endpointType: crf.endpointType,
    })),
  }
  // Urban runs: pass the selected centres; empty selection = all centres
  // in the country (backend treats null as no filter).
  if (!isUS && nonUSLevel === 'urban') {
    config.cityIds = step1?.studyArea?.cityIds?.length
      ? [...step1.studyArea.cityIds]
      : null
  }
  // Default to analytical (no Monte Carlo): pass an iteration count only when
  // the user explicitly set one (> 0). Omitting it → backend default of 0 →
  // analytical CIs.
  if (step6?.monteCarloIterations > 0) {
    config.monteCarloIterations = step6.monteCarloIterations
  }
  return config
}
