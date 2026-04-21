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
