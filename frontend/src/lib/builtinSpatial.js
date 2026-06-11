// Built-in US tract analyses run on the backend spatial engine (mode:'builtin')
// rather than the client-side scalar engine, so they return per-tract `zones`
// — required for the Environmental Justice context section. This module decides
// when that path applies and builds the request.

/**
 * True when the current run is a built-in US Census-tract analysis, which
 * should be routed to the backend `mode:'builtin'` spatial endpoint instead of
 * the in-browser scalar engine.
 *
 * @param {object} step1 - Study-area step state.
 * @param {object} step2 - Concentration step state.
 * @returns {boolean}
 */
export function shouldUseBuiltinSpatial(step1, step2) {
  return (
    step1?.studyArea?.id === 'USA' &&
    step1?.studyArea?.analysisLevel === 'tract' &&
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
    analysisLevel: 'tract',
    stateFilter: step1?.studyArea?.stateId || null,
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
