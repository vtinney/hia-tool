// Each row is a single canonical country: every form that should match
// it (ISO3, ISO2, lowercase slug, hyphenated display name) is listed.
// Lowercased internally — case doesn't matter at call sites.
const COUNTRY_ALIASES = [
  ['USA', 'us', 'usa', 'united-states', 'united_states', 'united states'],
  ['MEX', 'mx', 'mex', 'mexico'],
  ['BRA', 'br', 'bra', 'brazil'],
  ['CAN', 'ca', 'can', 'canada'],
  ['IND', 'in', 'ind', 'india'],
  ['CHN', 'cn', 'chn', 'china'],
  ['GBR', 'gb', 'gbr', 'uk', 'united-kingdom', 'united_kingdom', 'united kingdom'],
]

// any-form (lowercased) → Set of equivalent identifiers (uppercased).
const EQUIVALENTS = new Map()
for (const group of COUNTRY_ALIASES) {
  const set = new Set(group.map((s) => String(s).toUpperCase()))
  for (const id of group) EQUIVALENTS.set(String(id).toLowerCase(), set)
}

function equivalentsFor(countryId) {
  if (!countryId) return null
  const key = String(countryId).toLowerCase()
  return EQUIVALENTS.get(key) || new Set([String(countryId).toUpperCase()])
}

export function datasetCoversCountry(dataset, countryId) {
  if (!dataset?.countries_covered?.length || !countryId) return false
  const equivalents = equivalentsFor(countryId)
  for (const entry of dataset.countries_covered) {
    const upper = String(entry).toUpperCase()
    if (equivalents.has(upper)) return true
    // US sub-state codes ("US-CA", "US-NY", ...) belong to the US group.
    if (equivalents.has('USA') && upper.startsWith('US-')) return true
  }
  return false
}

export function yearsFor(dataset, countryId) {
  if (!datasetCoversCountry(dataset, countryId)) return []
  return [...(dataset.years || [])]
}
