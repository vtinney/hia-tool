// Adapters that reshape a backend /api/compute/spatial response into the
// pieces the Results page renders. Spatial responses carry per-zone results
// plus an aggregate block; the Detail/Export tabs and the hero number expect
// the flat row shape the scalar engine produces, so spatial runs adapt here.

/**
 * Flatten a spatial response's aggregate CRF results into the detail rows
 * the Results table, endpoint breakdown, and CSV/PDF export consume.
 *
 * @param {object} response - /api/compute/spatial response.
 * @param {object} frameworkById - crf-library lookup keyed by CRF id.
 * @returns {Array<object>} Detail rows (empty when the response has none).
 */
export function detailRowsFromSpatial(response, frameworkById = {}) {
  const results = response?.aggregate?.results
  if (!Array.isArray(results)) return []
  return results.map((r) => ({
    crfStudy: r.study,
    framework: frameworkById[r.crfId]?.framework || '',
    endpoint: r.endpoint,
    attributableCases: r.attributableCases?.mean ?? 0,
    lower95: r.attributableCases?.lower95 ?? null,
    upper95: r.attributableCases?.upper95 ?? null,
    attributableFraction: r.attributableFraction?.mean ?? null,
    ratePer100k: r.attributableRate?.mean ?? null,
  }))
}

/**
 * Pick the mortality total the hero number should display for a spatial run.
 *
 * The backend keeps two totals that must never be summed (that would double
 * count): `allCauseDeaths` (all-cause CRFs only) and `totalDeaths`
 * (cause-specific mortality CRFs only). Prefer all-cause when present —
 * it's the headline most runs want — otherwise use the cause-specific total.
 * A zero/absent pair returns null so the hero falls back to the top detail
 * row instead of animating a large zero.
 *
 * @param {object} response - /api/compute/spatial response.
 * @returns {{mean: number, lower95: number, upper95: number} | null}
 */
/**
 * Attributable fraction / rate for the secondary stat tiles on a spatial
 * run. Must describe the same CRF the hero number reflects: when the
 * headline is the all-cause (or cause-specific) mortality total, prefer
 * the aggregate row whose cases match that total; otherwise fall back to
 * the highest-impact CRF. Without this, a run mixing mortality and
 * incidence endpoints (e.g. NO₂ ACM + pediatric asthma) shows a deaths
 * headline next to the asthma CRF's fraction and rate.
 *
 * @param {object} response - /api/compute/spatial response.
 * @returns {{attributableFraction: number|null, attributableRate: number|null}}
 */
export function spatialSummaryStats(response) {
  const results = response?.aggregate?.results
  if (!Array.isArray(results) || results.length === 0) {
    return { attributableFraction: null, attributableRate: null }
  }
  const headline = spatialHeadlineDeaths(response)
  const matching = headline
    ? results.find(
        (r) => Math.abs((r.attributableCases?.mean ?? NaN) - headline.mean) < 0.5,
      )
    : null
  const chosen = matching ?? [...results].sort(
    (a, b) => (b.attributableCases?.mean ?? 0) - (a.attributableCases?.mean ?? 0),
  )[0]
  return {
    attributableFraction: chosen.attributableFraction?.mean ?? null,
    attributableRate: chosen.attributableRate?.mean ?? null,
  }
}

export function spatialHeadlineDeaths(response) {
  const allCause = response?.allCauseDeaths
  if (allCause && allCause.mean > 0) return allCause
  const causeSpecific = response?.totalDeaths
  if (causeSpecific && causeSpecific.mean > 0) return causeSpecific
  return null
}
