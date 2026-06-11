import { describe, it, expect } from 'vitest'
import { shouldUseBuiltinSpatial, buildBuiltinSpatialConfig } from '../builtinSpatial'

const crf = {
  id: 'gbd_pm25_acm_adult', source: 'GBD 2023 MR-BRT (IHME)', endpoint: 'All-cause mortality',
  beta: 0.0062, betaLow: 0.0044, betaHigh: 0.008, functionalForm: 'mr-brt', defaultRate: 0.008,
  extraneous: 'should be dropped',
}
const usTract = {
  step1: { pollutant: 'pm25', studyArea: { id: 'USA', analysisLevel: 'tract', stateId: '48' } },
  step2: { baseline: { type: 'dataset', year: 2018, value: 19.67 }, control: { type: 'none', value: null } },
  step6: { monteCarloIterations: 0 },
}

describe('shouldUseBuiltinSpatial', () => {
  it('is true for a built-in US tract run', () => {
    expect(shouldUseBuiltinSpatial(usTract.step1, usTract.step2)).toBe(true)
  })
  it('is false for non-US, non-tract, or manual (non-dataset) baseline', () => {
    expect(shouldUseBuiltinSpatial({ studyArea: { id: 'MEX', analysisLevel: 'adm2' } }, usTract.step2)).toBe(false)
    expect(shouldUseBuiltinSpatial({ studyArea: { id: 'USA', analysisLevel: 'state' } }, usTract.step2)).toBe(false)
    expect(shouldUseBuiltinSpatial(usTract.step1, { baseline: { type: 'manual', value: 19.67 } })).toBe(false)
  })
})

describe('buildBuiltinSpatialConfig', () => {
  const cfg = buildBuiltinSpatialConfig(usTract.step1, usTract.step2, usTract.step6, [crf])

  it('builds a builtin tract request mapping USA→us with the chosen year/pollutant/state', () => {
    expect(cfg).toMatchObject({
      mode: 'builtin', pollutant: 'pm25', country: 'us', year: 2018,
      analysisLevel: 'tract', stateFilter: '48', controlMode: 'benchmark',
    })
  })

  it('defaults the counterfactual to 0 (total burden) when no control is set', () => {
    expect(cfg.controlConcentration).toBe(0)
    const withCtrl = buildBuiltinSpatialConfig(
      usTract.step1,
      { ...usTract.step2, control: { type: 'manual', value: 5 } },
      usTract.step6, [crf],
    )
    expect(withCtrl.controlConcentration).toBe(5)
  })

  it('projects only the CRF fields the backend needs', () => {
    expect(cfg.selectedCRFs).toEqual([{
      id: 'gbd_pm25_acm_adult', source: 'GBD 2023 MR-BRT (IHME)', endpoint: 'All-cause mortality',
      beta: 0.0062, betaLow: 0.0044, betaHigh: 0.008, functionalForm: 'mr-brt', defaultRate: 0.008,
    }])
    expect(cfg.selectedCRFs[0]).not.toHaveProperty('extraneous')
  })

  it('includes monteCarloIterations only when the user set a positive count (else analytical)', () => {
    expect(cfg).not.toHaveProperty('monteCarloIterations') // step6 mc=0 → omitted → backend analytical
    const mc = buildBuiltinSpatialConfig(usTract.step1, usTract.step2, { monteCarloIterations: 500 }, [crf])
    expect(mc.monteCarloIterations).toBe(500)
  })
})
