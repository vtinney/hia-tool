// Pure helpers for the Step 1 urban-centre picker. The centres array comes
// from GET /api/data/urban-centres/{country} already sorted by population
// descending — topNIds relies on that order.

export function filterCentres(centres, query) {
  if (!query) return centres
  const q = String(query).toLowerCase()
  return centres.filter((c) => (c.name || '').toLowerCase().includes(q))
}

export function topNIds(centres, n) {
  if (!n || n <= 0) return []
  return centres.slice(0, n).map((c) => c.id)
}
