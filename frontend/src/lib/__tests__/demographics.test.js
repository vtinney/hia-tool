import { describe, it, expect } from 'vitest'
import { populationWeightedMean, pickVintage } from '../demographics'

describe('populationWeightedMean', () => {
  it('computes population-weighted mean across tracts', () => {
    const tracts = [
      { total_pop: 1000, pct_minority: 0.5 },
      { total_pop: 3000, pct_minority: 0.9 },
    ]
    // (1000*0.5 + 3000*0.9) / 4000 = 3200/4000 = 0.8
    expect(populationWeightedMean(tracts, 'pct_minority')).toBeCloseTo(0.8, 5)
  })

  it('skips tracts where the target field is NaN', () => {
    const tracts = [
      { total_pop: 1000, pct_minority: 0.5 },
      { total_pop: 3000, pct_minority: NaN },
    ]
    expect(populationWeightedMean(tracts, 'pct_minority')).toBeCloseTo(0.5, 5)
  })

  it('skips tracts where the target field is null or undefined', () => {
    const tracts = [
      { total_pop: 1000, pct_minority: 0.5 },
      { total_pop: 3000, pct_minority: null },
      { total_pop: 2000 },
    ]
    expect(populationWeightedMean(tracts, 'pct_minority')).toBeCloseTo(0.5, 5)
  })

  it('returns null when all tracts have NaN for the field', () => {
    const tracts = [
      { total_pop: 1000, pct_minority: NaN },
      { total_pop: 3000, pct_minority: NaN },
    ]
    expect(populationWeightedMean(tracts, 'pct_minority')).toBeNull()
  })

  it('returns null for empty tract array', () => {
    expect(populationWeightedMean([], 'pct_minority')).toBeNull()
  })

  it('treats zero-population tracts as weight 0', () => {
    const tracts = [
      { total_pop: 0, pct_minority: 0.99 },
      { total_pop: 1000, pct_minority: 0.3 },
    ]
    expect(populationWeightedMean(tracts, 'pct_minority')).toBeCloseTo(0.3, 5)
  })

  it('returns null when all weights sum to zero', () => {
    const tracts = [
      { total_pop: 0, pct_minority: 0.5 },
      { total_pop: 0, pct_minority: 0.8 },
    ]
    expect(populationWeightedMean(tracts, 'pct_minority')).toBeNull()
  })
})

describe('pickVintage', () => {
  const ALL = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]

  it('returns exact match when available', () => {
    expect(pickVintage(2022, ALL)).toBe(2022)
  })

  it('for pre-2020 years with no exact match, falls back down same side', () => {
    // Pretend 2017 is missing; year 2017 should fall back to 2016 (closer than 2018 on the other side is still fine because both are pre-2020)
    expect(pickVintage(2017, [2015, 2016, 2018, 2019, 2020, 2021])).toBe(2016)
  })

  it('never crosses the 2019 → 2020 boundary', () => {
    // Ask for 2019 but only 2020+ available: return the closest post-2020, NOT cross back.
    // pickVintage is about minimizing tract-boundary mismatch; crossing is the last resort.
    expect(pickVintage(2019, [2020, 2021, 2022])).toBe(2020)
  })

  it('prefers same-side-of-2020 match over closer across-boundary match', () => {
    // Ask 2019, available = [2018, 2020]. Both are distance 1, but 2018 is same-side (pre-2020) so win.
    expect(pickVintage(2019, [2018, 2020])).toBe(2018)
  })

  it('for 2020+ years, prefers closest post-2020 vintage', () => {
    expect(pickVintage(2023, [2020, 2021, 2022])).toBe(2022)
  })

  it('returns null when availableVintages is empty', () => {
    expect(pickVintage(2022, [])).toBeNull()
  })

  it('returns the only available vintage even if far from target', () => {
    expect(pickVintage(2030, [2020])).toBe(2020)
  })
})
