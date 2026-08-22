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
/**
 * Adapt a spatial compute response into the per-tract shape the EJ context
 * section consumes. The backend returns computed units under `zones`
 * (`{ zoneId, population, ... }`); the EJ join keys on an 11-digit tract
 * GEOID, so only tract-level zones qualify. Non-tract runs (state/county
 * FIPS) yield null, which makes the EJ gate fail cleanly. A response that
 * already carries an explicit `per_tract_results` array is passed through.
 *
 * @param {object|null} results - Analysis results payload.
 * @returns {Array<{tract_fips: string, population: number}>|null}
 */
export function tractResultsFromResponse(results) {
  if (Array.isArray(results?.per_tract_results)) return results.per_tract_results
  const zones = results?.zones
  if (!Array.isArray(zones) || zones.length === 0) return null
  const tracts = zones
    .filter((z) => typeof z?.zoneId === 'string' && z.zoneId.length === 11)
    .map((z) => ({ tract_fips: z.zoneId, population: z.population }))
  return tracts.length > 0 ? tracts : null
}

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
 * Tie-breaking: when two same-side vintages are equidistant from the target
 * year, the earliest-encountered vintage in `availableVintages` wins (strict
 * `<` on distance, so the first match stays). Deterministic given a stable
 * input order.
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

/**
 * Derive a demographics endpoint filter object from a study area.
 *
 * Returns:
 *   - {} for nationwide US (type='country', id='united-states')
 *   - {state: 'XX'} for type='state' with id 'us-XX'
 *   - {state: 'XX', county: 'YYY'} for type='county' with id 'us-XX-YYY'
 *   - null for non-US or non-admin-boundary types, which should NOT render
 *     the EJ section.
 *
 * FIPS codes are returned as strings (state 2 digits, county 3 digits) to
 * match the `/api/data/demographics` query param shape.
 *
 * @param {{type: string, id: string}|null|undefined} studyArea
 * @returns {{state?: string, county?: string}|null}
 */
/**
 * Compute a [minLon, minLat, maxLon, maxLat] bounding box for a GeoJSON
 * FeatureCollection of (Multi)Polygons, so the choropleth can fit the
 * study area instead of opening on the continental default view.
 *
 * @param {object|null} fc - FeatureCollection.
 * @returns {[number, number, number, number]|null} bbox, or null when empty.
 */
export function bboxOfFeatureCollection(fc) {
  let minLon = Infinity, minLat = Infinity, maxLon = -Infinity, maxLat = -Infinity
  const visit = (coords) => {
    if (typeof coords[0] === 'number') {
      const [lon, lat] = coords
      if (lon < minLon) minLon = lon
      if (lat < minLat) minLat = lat
      if (lon > maxLon) maxLon = lon
      if (lat > maxLat) maxLat = lat
      return
    }
    for (const c of coords) visit(c)
  }
  for (const f of fc?.features ?? []) {
    if (f?.geometry?.coordinates) visit(f.geometry.coordinates)
  }
  if (!Number.isFinite(minLon)) return null
  return [minLon, minLat, maxLon, maxLat]
}

export function studyAreaToFilter(studyArea) {
  if (!studyArea) return null
  const { type, id } = studyArea

  // Current Step-1 shape: id 'USA' with stateId / countyId fields (the
  // legacy us-XX composite ids below are kept for older persisted state).
  if (id === 'USA') {
    if (!studyArea.stateId) return {}
    const filter = { state: studyArea.stateId }
    if (studyArea.countyId) filter.county = studyArea.countyId
    return filter
  }

  if (type === 'country' && id === 'united-states') return {}

  if (type === 'state') {
    const m = /^us-(\d{2})$/.exec(id ?? '')
    if (!m) return null
    return { state: m[1] }
  }

  if (type === 'county') {
    const m = /^us-(\d{2})-(\d{3})$/.exec(id ?? '')
    if (!m) return null
    return { state: m[1], county: m[2] }
  }

  return null
}
