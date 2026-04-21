import { describe, it, expect } from 'vitest'
import { populationWeightedMean } from '../demographics'

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
