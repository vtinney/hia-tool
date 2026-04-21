/**
 * Population-weighted mean of a field across an array of tract-like objects.
 *
 * Skips tracts where the target field is NaN / null / undefined. Tracts with
 * zero population naturally drop out of the weighted mean. Returns null when
 * no valid (field, weight) pairs exist.
 *
 * @param {Array<{total_pop: number}>} tracts
 * @param {string} field - Property name on each tract to aggregate.
 * @returns {number|null}
 */
export function populationWeightedMean(tracts, field) {
  let numerator = 0
  let denominator = 0
  for (const t of tracts) {
    const v = t[field]
    if (v == null || Number.isNaN(v)) continue
    const w = Number(t.total_pop) || 0
    numerator += w * v
    denominator += w
  }
  if (denominator === 0) return null
  return numerator / denominator
}

/**
 * Select the best ACS vintage for an analysis year, preferring an exact
 * match, then the nearest vintage on the same side of the 2020 tract-
 * boundary redraw, then crossing the boundary only as a last resort.
 *
 * Why: 2015-2019 use the pre-2020 tract geometry (~73.7k tracts), and
 * 2020+ use the post-decennial redraw (~85k tracts). Silent cross-boundary
 * fallback would mismatch tract FIPS and distort downstream joins.
 *
 * @param {number} analysisYear
 * @param {number[]} availableVintages
 * @returns {number|null}
 */
export function pickVintage(analysisYear, availableVintages) {
  if (!availableVintages || availableVintages.length === 0) return null
  if (availableVintages.includes(analysisYear)) return analysisYear

  const sideOf = (y) => (y < 2020 ? 'pre' : 'post')
  const targetSide = sideOf(analysisYear)

  const sameSide = availableVintages.filter((y) => sideOf(y) === targetSide)
  const pool = sameSide.length > 0 ? sameSide : availableVintages

  let best = pool[0]
  let bestDist = Math.abs(best - analysisYear)
  for (const y of pool) {
    const d = Math.abs(y - analysisYear)
    if (d < bestDist) {
      best = y
      bestDist = d
    }
  }
  return best
}
