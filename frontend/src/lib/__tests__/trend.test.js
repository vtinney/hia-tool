import { describe, it, expect } from 'vitest'
import { buildTrendSeries } from '../trend'

const total = (mean, lower95, upper95) => ({
  totalDeaths: { mean, lower95, upper95 },
})

describe('buildTrendSeries', () => {
  it('returns an empty array when there is no primary mean and no additional runs', () => {
    expect(buildTrendSeries(null, null, [])).toEqual([])
    expect(buildTrendSeries({}, 2020, [])).toEqual([])
  })

  it('returns a single point for the primary run', () => {
    const series = buildTrendSeries(total(100, 80, 120), 2020, [])
    expect(series).toEqual([{ year: 2020, mean: 100, lower95: 80, upper95: 120 }])
  })

  it('combines the primary run with additional runs sorted ascending by year', () => {
    const series = buildTrendSeries(total(100, 80, 120), 2020, [
      { runId: 'a', year: 2022, results: total(140, 110, 170) },
      { runId: 'b', year: 2018, results: total(90, 70, 110) },
    ])
    expect(series.map((p) => p.year)).toEqual([2018, 2020, 2022])
    expect(series[2]).toEqual({ year: 2022, mean: 140, lower95: 110, upper95: 170 })
  })

  it('falls back to summary.totalDeaths when top-level totalDeaths is absent', () => {
    const primary = { summary: { totalDeaths: { mean: 55, lower95: 40, upper95: 70 } } }
    const series = buildTrendSeries(primary, 2019, [])
    expect(series).toEqual([{ year: 2019, mean: 55, lower95: 40, upper95: 70 }])
  })

  it('omits points whose mean cannot be extracted (no pooled total)', () => {
    const series = buildTrendSeries(total(100, 80, 120), 2020, [
      { runId: 'a', year: 2021, results: {} },
    ])
    expect(series.map((p) => p.year)).toEqual([2020])
  })

  it('omits a run with a null year', () => {
    const series = buildTrendSeries(total(100, 80, 120), 2020, [
      { runId: 'a', year: null, results: total(140, 110, 170) },
    ])
    expect(series.map((p) => p.year)).toEqual([2020])
  })

  it('dedupes by year, keeping the primary run over an additional run', () => {
    const series = buildTrendSeries(total(100, 80, 120), 2020, [
      { runId: 'a', year: 2020, results: total(999, 900, 1100) },
    ])
    expect(series).toEqual([{ year: 2020, mean: 100, lower95: 80, upper95: 120 }])
  })

  it('carries null CI bounds through without dropping the point', () => {
    const series = buildTrendSeries(total(100, null, null), 2020, [])
    expect(series).toEqual([{ year: 2020, mean: 100, lower95: null, upper95: null }])
  })
})
