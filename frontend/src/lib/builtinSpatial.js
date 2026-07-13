// Built-in US analyses (tract / county / state) run on the backend spatial
// engine (mode:'builtin') rather than the client-side scalar engine, so they
// return per-zone `zones` — real dissolved reporting units with their own
// population and concentration, and (at tract level) the per-tract results the
// Environmental Justice context section needs. This module decides when that
// path applies and builds the request.

// US admin grains the backend spatial engine resolves from ACS tracts. Country
// level stays on the scalar path: dissolving all ~85k US tracts into one
// polygon is slow and a national scalar is just as accurate.
const US_SPATIAL_LEVELS = new Set(['tract', 'county', 'state'])

/**
 * True when the current run is a built-in US admin-level analysis (tract,
 * county, or state) that should be routed to the backend `mode:'builtin'`
 * spatial endpoint instead of the in-browser scalar engine.
 *
 * A state must be selected: the Step 1 UI only exposes the analysis-level
 * radios once a state is picked, and the backend needs a state filter to avoid
 * an all-US dissolve. Country-level and non-US runs fall through to the scalar
 * / admin-2 paths.
 *
 * @param {object} step1 - Study-area step state.
 * @param {object} step2 - Concentration step state.
 * @returns {boolean}
 */
export function shouldUseBuiltinSpatial(step1, step2) {
  return (
    step1?.studyArea?.id === 'USA' &&
    US_SPATIAL_LEVELS.has(step1?.studyArea?.analysisLevel) &&
    Boolean(step1?.studyArea?.stateId) &&
    step2?.baseline?.type === 'dataset'
  )
}

/**
 * Build the `/api/compute/spatial` request body for a built-in US tract run.
 * Assumes shouldUseBuiltinSpatial() already returned true.
 *
 * @param {object} step1 - Study-area step state (pollutant, studyArea).
 * @param {object} step2 - Concentration step state (baseline, control).
 * @param {object} step6 - Run step state (monteCarloIterations).
 * @param {Array<object>} selectedCRFs - Resolved CRF definitions.
 * @returns {object} Spatial compute request.
 */
export function buildBuiltinSpatialConfig(step1, step2, step6, selectedCRFs) {
  const config = {
    mode: 'builtin',
    pollutant: step1?.pollutant,
    country: 'us', // studyArea.id 'USA' → backend country slug
    year: step2?.baseline?.year ?? null,
    // Real grain from Step 1 (tract / county / state); default to tract so an
    // older persisted study area without analysisLevel still resolves safely.
    analysisLevel: step1?.studyArea?.analysisLevel || 'tract',
    stateFilter: step1?.studyArea?.stateId || null,
    countyFilter: step1?.studyArea?.countyId || null,
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
      defaultRate: crf.defaultRate,
    })),
  }
  // Default to analytical (no Monte Carlo): pass an iteration count only when
  // the user explicitly set one (> 0). Omitting it → backend default of 0 →
  // analytical CIs.
  if (step6?.monteCarloIterations > 0) {
    config.monteCarloIterations = step6.monteCarloIterations
  }
  return config
}
