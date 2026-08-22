// "Compare another year" re-runs. The previous implementation posted the
// raw wizard-step blob to /api/compute/spatial, which expects the spatial
// request schema — every live re-run failed validation. This module clones
// the config to the new year and builds the same request Step 6 builds.
//
// Only built-in spatial configs are re-runnable: a manual/scalar config
// carries a concentration value loaded for the original year, so a "new
// year" run would silently reuse stale numbers.

import crfLibrary from '../data/crf-library.json'
import { cloneConfigWithYear, runSpatialCompute, fetchIncidence } from './api'
import { shouldUseBuiltinSpatial, buildBuiltinSpatialConfig } from './builtinSpatial'
import { detailRowsFromSpatial } from './spatialResults'

const CRF_BY_ID = Object.fromEntries(crfLibrary.map((c) => [c.id, c]))

// Same row preference as Step 4's built-in loader: all-ages/both-sex when
// present, else the first row with a rate.
function pickAggregateRate(units) {
  const chosen =
    (units || []).find(
      (u) => u.incidence_rate != null
        && (u.age_group === 'all_ages' || u.age_group == null)
        && (u.sex == null || u.sex === 'both'),
    ) || (units || []).find((u) => u.incidence_rate != null)
  return chosen ? chosen.incidence_rate : null
}

/**
 * True when the config can be honestly re-run for a different year.
 */
export function canRunAnotherYear(config) {
  return shouldUseBuiltinSpatial(config?.step1, config?.step2)
}

/**
 * Re-run the analysis for a new year and return a Results-shaped response
 * (spatial response plus `detail` rows).
 */
export async function runAnalysisForYear(config, year) {
  if (!canRunAnotherYear(config)) {
    throw new Error(
      'Compare another year requires a built-in spatial analysis — manual or uploaded inputs are year-specific.',
    )
  }
  const cfg = cloneConfigWithYear(config, year)
  const crfs = (cfg.step5?.selectedCRFs || [])
    .map((id) => CRF_BY_ID[id])
    .filter(Boolean)

  // Refresh the baseline rates for the new year (mortality rates move
  // year to year); fall back to the primary run's rate when the lookup
  // has nothing for that year.
  const rates = { ...(cfg.step4?.rates || {}) }
  const causes = [...new Set(crfs.map((c) => c.cause).filter(Boolean))]
  const byCause = {}
  await Promise.all(causes.map(async (cause) => {
    try {
      byCause[cause] = await fetchIncidence(
        cfg.step1?.studyArea?.id, cause, year, { aggregate: true },
      )
    } catch {
      byCause[cause] = null
    }
  }))
  for (const crf of crfs) {
    const rate = pickAggregateRate(byCause[crf.cause]?.units)
    if (rate != null) rates[crf.id] = rate
  }

  const request = buildBuiltinSpatialConfig(
    cfg.step1, cfg.step2, cfg.step6, crfs, rates,
  )
  const res = await runSpatialCompute(request)
  return { ...res, detail: detailRowsFromSpatial(res, CRF_BY_ID) }
}
