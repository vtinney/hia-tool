import { describe, it, expect } from 'vitest'
import { cloneConfigWithYear } from '../api'

describe('cloneConfigWithYear', () => {
  it('replaces every year field and leaves everything else alone', () => {
    const base = {
      step1: { pollutant: 'pm25', studyArea: { id: 'MEX' } },
      step2: {
        baseline: { type: 'dataset', datasetId: 'who', year: 2018, value: 15.2 },
        control: { type: 'none', year: null },
      },
      step3: { populationType: 'manual', totalPopulation: 1e6, year: 2018 },
      step4: { incidenceType: 'manual', year: 2018 },
      step5: { selectedCRFs: [] },
      step6: { poolingMethod: 'separate' },
      step7: { runValuation: false },
    }

    const cloned = cloneConfigWithYear(base, 2016)

    expect(cloned.step2.baseline.year).toBe(2016)
    expect(cloned.step3.year).toBe(2016)
    expect(cloned.step4.year).toBe(2016)

    // Untouched fields
    expect(cloned.step1.pollutant).toBe('pm25')
    expect(cloned.step2.baseline.datasetId).toBe('who')
    expect(cloned.step3.totalPopulation).toBe(1e6)
    expect(cloned.step5).toEqual({ selectedCRFs: [] })

    // Should not have mutated input
    expect(base.step2.baseline.year).toBe(2018)
  })

  it('leaves null control.year null when source config had it null', () => {
    const base = {
      step1: {},
      step2: {
        baseline: { year: 2018 },
        control: { year: null },
      },
      step3: { year: 2018 },
      step4: { year: 2018 },
    }
    const cloned = cloneConfigWithYear(base, 2020)
    expect(cloned.step2.control.year).toBe(null)
  })

  it('updates control.year when the source config had a non-null control year', () => {
    const base = {
      step1: {},
      step2: {
        baseline: { year: 2018 },
        control: { type: 'file', year: 2019 },
      },
      step3: { year: 2018 },
      step4: { year: 2018 },
    }
    const cloned = cloneConfigWithYear(base, 2020)
    expect(cloned.step2.control.year).toBe(2020)
  })
})
