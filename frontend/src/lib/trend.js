// Assemble a year-over-year trend series from the primary analysis run plus
// any "Compare another year" runs stacked on the Results page. Each point is
// { year, mean, lower95, upper95 }. Points whose pooled total can't be read
// (e.g. pooling: none) are omitted, since there's nothing to plot.

function extractTotal(results) {
  const t = results?.totalDeaths ?? results?.summary?.totalDeaths ?? null
  if (!t || t.mean == null) return null
  return {
    mean: t.mean,
    lower95: t.lower95 ?? null,
    upper95: t.upper95 ?? null,
  }
}

export function buildTrendSeries(primaryResults, primaryYear, additionalRuns = []) {
  const points = []
  const seen = new Set()

  const add = (year, results) => {
    if (year == null || seen.has(year)) return
    const t = extractTotal(results)
    if (!t) return
    seen.add(year)
    points.push({ year, mean: t.mean, lower95: t.lower95, upper95: t.upper95 })
  }

  // Primary added first so it wins on a year collision with an additional run.
  add(primaryYear, primaryResults)
  for (const run of additionalRuns) add(run?.year, run?.results)

  return points.sort((a, b) => a.year - b.year)
}
