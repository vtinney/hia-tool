import { describe, it, expect } from 'vitest'
import { shouldUseBuiltinSpatial, buildBuiltinSpatialConfig, resolveRatesForCRFs } from '../builtinSpatial'

const crf = {
  id: 'gbd_pm25_acm_adult', source: 'GBD 2023 MR-BRT (IHME)', endpoint: 'All-cause mortality',
  beta: 0.0062, betaLow: 0.0044, betaHigh: 0.008, functionalForm: 'mr-brt', defaultRate: 0.008,
  cause: 'all_cause', endpointType: 'mortality',
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
  it('is true for built-in US state and county runs with a state selected', () => {
    expect(shouldUseBuiltinSpatial(
      { studyArea: { id: 'USA', analysisLevel: 'state', stateId: '48' } }, usTract.step2,
    )).toBe(true)
    expect(shouldUseBuiltinSpatial(
      { studyArea: { id: 'USA', analysisLevel: 'county', stateId: '48' } }, usTract.step2,
    )).toBe(true)
  })
  it('is true for a non-US admin-2 run with a built-in dataset', () => {
    expect(shouldUseBuiltinSpatial({ studyArea: { id: 'MEX', analysisLevel: 'adm2' } }, usTract.step2)).toBe(true)
    // Step 1 defaults the non-US radio to adm2, so a study area persisted
    // without analysisLevel must route the same way the UI displays it.
    expect(shouldUseBuiltinSpatial({ studyArea: { id: 'MEX' } }, usTract.step2)).toBe(true)
  })
  it('is false for country level, no state selected, or manual baseline', () => {
    // non-US country average stays on the scalar path
    expect(shouldUseBuiltinSpatial({ studyArea: { id: 'MEX', analysisLevel: 'country' } }, usTract.step2)).toBe(false)
    // US country level stays on the scalar path (avoids an all-US dissolve)
    expect(shouldUseBuiltinSpatial({ studyArea: { id: 'USA', analysisLevel: 'country', stateId: '48' } }, usTract.step2)).toBe(false)
    // state level but no state chosen yet → not routable to the backend
    expect(shouldUseBuiltinSpatial({ studyArea: { id: 'USA', analysisLevel: 'state' } }, usTract.step2)).toBe(false)
    expect(shouldUseBuiltinSpatial(usTract.step1, { baseline: { type: 'manual', value: 19.67 } })).toBe(false)
    expect(shouldUseBuiltinSpatial({ studyArea: { id: 'MEX', analysisLevel: 'adm2' } }, { baseline: { type: 'manual', value: 15 } })).toBe(false)
  })
})

describe('buildBuiltinSpatialConfig', () => {
  const cfg = buildBuiltinSpatialConfig(usTract.step1, usTract.step2, usTract.step6, [crf])

  it('builds a builtin tract request mapping USA→us with the chosen year/pollutant/state', () => {
    expect(cfg).toMatchObject({
      mode: 'builtin', pollutant: 'pm25', country: 'us', year: 2018,
      analysisLevel: 'tract', stateFilter: '48', countyFilter: null, controlMode: 'benchmark',
    })
  })

  it('passes the real analysis level (state / county) instead of hardcoding tract', () => {
    const stateCfg = buildBuiltinSpatialConfig(
      { ...usTract.step1, studyArea: { id: 'USA', analysisLevel: 'state', stateId: '48' } },
      usTract.step2, usTract.step6, [crf],
    )
    expect(stateCfg.analysisLevel).toBe('state')
    const countyCfg = buildBuiltinSpatialConfig(
      { ...usTract.step1, studyArea: { id: 'USA', analysisLevel: 'county', stateId: '48', countyId: '201' } },
      usTract.step2, usTract.step6, [crf],
    )
    expect(countyCfg).toMatchObject({ analysisLevel: 'county', stateFilter: '48', countyFilter: '201' })
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
      cause: 'all_cause', endpointType: 'mortality',
    }])
    expect(cfg.selectedCRFs[0]).not.toHaveProperty('extraneous')
  })

  it('uses the Step-4 rate for a CRF when one is set, else the library default', () => {
    // The wizard displays Step-4 baseline rates (built-in GBD load or
    // manual entry); the spatial engine must use what the UI shows.
    const out = buildBuiltinSpatialConfig(
      usTract.step1, usTract.step2, usTract.step6, [crf],
      { gbd_pm25_acm_adult: 0.0102 },
    )
    expect(out.selectedCRFs[0].defaultRate).toBe(0.0102)
    const noRate = buildBuiltinSpatialConfig(
      usTract.step1, usTract.step2, usTract.step6, [crf],
      { some_other_crf: 0.5 },
    )
    expect(noRate.selectedCRFs[0].defaultRate).toBe(0.008)
    const nullRate = buildBuiltinSpatialConfig(
      usTract.step1, usTract.step2, usTract.step6, [crf],
      { gbd_pm25_acm_adult: null },
    )
    expect(nullRate.selectedCRFs[0].defaultRate).toBe(0.008)
  })

  it('resolves a rate stored under a sibling CRF of the same endpoint', () => {
    // Step 4 keys rates by the endpoint's first library CRF (e.g.
    // epa_pm25_ihd_adult); a template may select a different CRF for the
    // same endpoint (gbd_pm25_ihd). The rate must still apply.
    const resolved = resolveRatesForCRFs(
      { epa_pm25_ihd_adult: 0.0014 },
      [{ id: 'gbd_pm25_ihd', endpoint: 'Ischemic heart disease', defaultRate: 0.0025 }],
    )
    expect(resolved.gbd_pm25_ihd).toBe(0.0014)
    // No stored rate anywhere for the endpoint → library default
    const fallback = resolveRatesForCRFs(
      {},
      [{ id: 'gbd_pm25_ihd', endpoint: 'Ischemic heart disease', defaultRate: 0.0025 }],
    )
    expect(fallback.gbd_pm25_ihd).toBe(0.0025)
  })

  it('applies a sibling-keyed Step-4 rate in the built config', () => {
    const gbdIhd = {
      ...crf, id: 'gbd_pm25_ihd', endpoint: 'Ischemic heart disease',
      cause: 'ihd', defaultRate: 0.0025,
    }
    const out = buildBuiltinSpatialConfig(
      usTract.step1, usTract.step2, usTract.step6, [gbdIhd],
      { epa_pm25_ihd_adult: 0.0014 },
    )
    expect(out.selectedCRFs[0].defaultRate).toBe(0.0014)
  })

  it('carries cause and endpointType so the backend mortality split is correct', () => {
    // Without these the backend defaults every CRF to cause=all_cause,
    // which zeroes the cause-specific totalDeaths the Results hero reads.
    const ihd = { ...crf, id: 'epa_pm25_ihd_adult', cause: 'ihd', endpointType: 'mortality' }
    const out = buildBuiltinSpatialConfig(usTract.step1, usTract.step2, usTract.step6, [ihd])
    expect(out.selectedCRFs[0].cause).toBe('ihd')
    expect(out.selectedCRFs[0].endpointType).toBe('mortality')
  })

  it('builds a non-US admin-2 request with the real ISO3 country and no US filters', () => {
    const adm2Cfg = buildBuiltinSpatialConfig(
      { pollutant: 'pm25', studyArea: { id: 'MEX', analysisLevel: 'adm2' } },
      usTract.step2, usTract.step6, [crf],
    )
    expect(adm2Cfg).toMatchObject({
      mode: 'builtin', pollutant: 'pm25', country: 'MEX', year: 2018,
      analysisLevel: 'adm2', stateFilter: null, countyFilter: null, controlMode: 'benchmark',
      controlConcentration: 0,
    })
  })

  it('defaults a non-US study area without analysisLevel to adm2', () => {
    const adm2Cfg = buildBuiltinSpatialConfig(
      { pollutant: 'pm25', studyArea: { id: 'MEX' } },
      usTract.step2, usTract.step6, [crf],
    )
    expect(adm2Cfg).toMatchObject({ country: 'MEX', analysisLevel: 'adm2' })
  })

  it('includes monteCarloIterations only when the user set a positive count (else analytical)', () => {
    expect(cfg).not.toHaveProperty('monteCarloIterations') // step6 mc=0 → omitted → backend analytical
    const mc = buildBuiltinSpatialConfig(usTract.step1, usTract.step2, { monteCarloIterations: 500 }, [crf])
    expect(mc.monteCarloIterations).toBe(500)
  })
})

describe('urban-centre routing', () => {
  const step2Dataset = { baseline: { type: 'dataset', year: 2020 }, control: {} }

  it('routes non-US urban runs to the backend', () => {
    const step1 = { studyArea: { id: 'MEX', analysisLevel: 'urban' } }
    expect(shouldUseBuiltinSpatial(step1, step2Dataset)).toBe(true)
  })

  it('builds an urban config with cityIds', () => {
    const step1 = {
      pollutant: 'pm25',
      studyArea: { id: 'MEX', analysisLevel: 'urban', cityIds: ['101', '102'] },
    }
    const config = buildBuiltinSpatialConfig(step1, step2Dataset, {}, [])
    expect(config.analysisLevel).toBe('urban')
    expect(config.cityIds).toEqual(['101', '102'])
    expect(config.country).toBe('MEX')
  })

  it('omits cityIds when none are selected (= all centres)', () => {
    const step1 = {
      pollutant: 'pm25',
      studyArea: { id: 'MEX', analysisLevel: 'urban', cityIds: [] },
    }
    const config = buildBuiltinSpatialConfig(step1, step2Dataset, {}, [])
    expect(config.cityIds).toBeNull()
  })

  it('still defaults missing analysisLevel to adm2', () => {
    const step1 = { pollutant: 'pm25', studyArea: { id: 'MEX' } }
    const config = buildBuiltinSpatialConfig(step1, step2Dataset, {}, [])
    expect(config.analysisLevel).toBe('adm2')
    expect(config.cityIds).toBeUndefined()
  })
})
