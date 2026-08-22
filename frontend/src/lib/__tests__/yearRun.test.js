import { describe, it, expect, vi, beforeEach } from 'vitest'

vi.mock('../api', async (importOriginal) => {
  const mod = await importOriginal()
  return { ...mod, runSpatialCompute: vi.fn(), fetchIncidence: vi.fn() }
})

import { runSpatialCompute, fetchIncidence } from '../api'
import { canRunAnotherYear, runAnalysisForYear } from '../yearRun'

const builtinTractConfig = {
  step1: {
    pollutant: 'pm25',
    analysisName: 'Test EJ',
    studyArea: { id: 'USA', analysisLevel: 'tract', stateId: '48' },
  },
  step2: {
    baseline: { type: 'dataset', datasetId: 'epa_aqs_pm25', year: 2020 },
    control: { type: 'none', value: null },
  },
  step4: { rates: { epa_pm25_acm_adult: 0.0102 }, year: 2020 },
  step5: { selectedCRFs: ['epa_pm25_acm_adult'] },
  step6: { monteCarloIterations: 0 },
}

const spatialResponse = {
  zones: [{ zoneId: '48001950100' }],
  aggregate: {
    results: [{
      crfId: 'epa_pm25_acm_adult', study: 'Turner', endpoint: 'All-cause mortality',
      attributableCases: { mean: 10, lower95: 5, upper95: 15 },
      attributableFraction: { mean: 0.05, lower95: 0.03, upper95: 0.07 },
      attributableRate: { mean: 40, lower95: 20, upper95: 60 },
    }],
  },
  totalDeaths: { mean: 0, lower95: 0, upper95: 0 },
  allCauseDeaths: { mean: 10, lower95: 5, upper95: 15 },
}

describe('canRunAnotherYear', () => {
  it('is true for a built-in spatial config', () => {
    expect(canRunAnotherYear(builtinTractConfig)).toBe(true)
  })
  it('is false for a manual/scalar config (concentration would be stale)', () => {
    const scalar = {
      ...builtinTractConfig,
      step2: { baseline: { type: 'manual', value: 12 }, control: {} },
    }
    expect(canRunAnotherYear(scalar)).toBe(false)
  })
})

describe('runAnalysisForYear', () => {
  beforeEach(() => {
    runSpatialCompute.mockReset()
    fetchIncidence.mockReset()
    fetchIncidence.mockResolvedValue(null)
  })

  it('builds a real spatial request for the new year (not the raw wizard blob)', async () => {
    runSpatialCompute.mockResolvedValue(spatialResponse)
    await runAnalysisForYear(builtinTractConfig, 2016)
    const sent = runSpatialCompute.mock.calls[0][0]
    expect(sent).toMatchObject({
      mode: 'builtin', pollutant: 'pm25', country: 'us',
      year: 2016, analysisLevel: 'tract', stateFilter: '48',
    })
    // Step-4 rate carried through when no year-specific rate is found
    expect(sent.selectedCRFs[0].defaultRate).toBe(0.0102)
    expect(sent.selectedCRFs[0].cause).toBe('all_cause')
    // No raw step blob leaked into the request
    expect(sent.step1).toBeUndefined()
  })

  it('refreshes the baseline rate for the new year when available', async () => {
    runSpatialCompute.mockResolvedValue(spatialResponse)
    fetchIncidence.mockResolvedValue({
      units: [{ incidence_rate: 0.0099, age_group: 'all_ages', sex: 'both' }],
    })
    await runAnalysisForYear(builtinTractConfig, 2016)
    expect(fetchIncidence).toHaveBeenCalledWith('USA', 'all_cause', 2016, { aggregate: true })
    const sent = runSpatialCompute.mock.calls[0][0]
    expect(sent.selectedCRFs[0].defaultRate).toBe(0.0099)
  })

  it('returns the response with detail rows attached', async () => {
    runSpatialCompute.mockResolvedValue(spatialResponse)
    const res = await runAnalysisForYear(builtinTractConfig, 2016)
    expect(res.detail).toHaveLength(1)
    expect(res.detail[0].endpoint).toBe('All-cause mortality')
  })

  it('rejects a config it cannot re-run', async () => {
    const scalar = {
      ...builtinTractConfig,
      step2: { baseline: { type: 'manual', value: 12 }, control: {} },
    }
    await expect(runAnalysisForYear(scalar, 2016)).rejects.toThrow(/built-in/)
  })
})
