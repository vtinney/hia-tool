const ISO3_TO_SLUG = {
  USA: ['us', 'usa'],
  MEX: ['mexico', 'mex'],
}

export function datasetCoversCountry(dataset, countryIso3) {
  if (!dataset?.countries_covered?.length || !countryIso3) return false
  const iso = countryIso3.toUpperCase()
  const aliases = ISO3_TO_SLUG[iso] || []
  for (const entry of dataset.countries_covered) {
    const upper = String(entry).toUpperCase()
    if (upper === iso) return true
    if (iso === 'USA' && upper.startsWith('US-')) return true
    if (aliases.includes(String(entry).toLowerCase())) return true
  }
  return false
}

export function yearsFor(dataset, countryIso3) {
  if (!datasetCoversCountry(dataset, countryIso3)) return []
  return [...(dataset.years || [])]
}
