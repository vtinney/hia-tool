import { describe, it, expect } from 'vitest'
import { datasetCoversCountry, yearsFor } from '../datasets'

describe('datasetCoversCountry', () => {
  it('matches direct country slug', () => {
    const ds = { countries_covered: ['mexico'] }
    expect(datasetCoversCountry(ds, 'MEX')).toBe(true)
    expect(datasetCoversCountry(ds, 'USA')).toBe(false)
  })

  it('matches ISO-3 in coverage list', () => {
    const ds = { countries_covered: ['MEX', 'USA', 'CAN'] }
    expect(datasetCoversCountry(ds, 'MEX')).toBe(true)
    expect(datasetCoversCountry(ds, 'FRA')).toBe(false)
  })

  it('treats US-XX state codes as US coverage', () => {
    const ds = { countries_covered: ['US-CA', 'US-NY'] }
    expect(datasetCoversCountry(ds, 'USA')).toBe(true)
    expect(datasetCoversCountry(ds, 'MEX')).toBe(false)
  })

  it('returns false when coverage list is missing', () => {
    expect(datasetCoversCountry({}, 'MEX')).toBe(false)
    expect(datasetCoversCountry(null, 'MEX')).toBe(false)
  })

  it('matches Mexico across ISO3 / lowercase slug / display name', () => {
    const ds = { countries_covered: ['MEX', 'USA'] }
    expect(datasetCoversCountry(ds, 'MEX')).toBe(true)
    expect(datasetCoversCountry(ds, 'mex')).toBe(true)
    expect(datasetCoversCountry(ds, 'mx')).toBe(true)
    expect(datasetCoversCountry(ds, 'mexico')).toBe(true)
    expect(datasetCoversCountry(ds, 'Mexico')).toBe(true)
  })

  it('matches USA across ISO3 / ISO2 / hyphenated slug', () => {
    const ds = { countries_covered: ['USA', 'MEX'] }
    expect(datasetCoversCountry(ds, 'USA')).toBe(true)
    expect(datasetCoversCountry(ds, 'us')).toBe(true)
    expect(datasetCoversCountry(ds, 'united-states')).toBe(true)
    expect(datasetCoversCountry(ds, 'United States')).toBe(true)
  })

  it('still maps US-XX state codes for any US identifier form', () => {
    const ds = { countries_covered: ['US-CA', 'US-NY'] }
    expect(datasetCoversCountry(ds, 'us')).toBe(true)
    expect(datasetCoversCountry(ds, 'united-states')).toBe(true)
  })
})

describe('yearsFor', () => {
  it('returns the dataset years when country is covered', () => {
    const ds = { countries_covered: ['MEX'], years: [2015, 2016, 2017] }
    expect(yearsFor(ds, 'MEX')).toEqual([2015, 2016, 2017])
  })

  it('returns an empty array when country is not covered', () => {
    const ds = { countries_covered: ['USA'], years: [2018, 2019] }
    expect(yearsFor(ds, 'MEX')).toEqual([])
  })

  it('prefers years_by_country when present, scoped to the country', () => {
    // FRA only has 2018 even though the dataset union covers 2017+2018.
    const ds = {
      countries_covered: ['MEX', 'USA', 'CAN', 'FRA'],
      years: [2017, 2018],
      years_by_country: {
        MEX: [2017],
        USA: [2017, 2018],
        CAN: [2017],
        FRA: [2018],
      },
    }
    expect(yearsFor(ds, 'FRA')).toEqual([2018])
    expect(yearsFor(ds, 'MEX')).toEqual([2017])
    expect(yearsFor(ds, 'USA')).toEqual([2017, 2018])
  })

  it('matches per-country lookup across ISO3 / ISO2 / slug forms', () => {
    const ds = {
      countries_covered: ['MEX'],
      years: [2017, 2018],
      years_by_country: { MEX: [2017, 2018] },
    }
    expect(yearsFor(ds, 'mex')).toEqual([2017, 2018])
    expect(yearsFor(ds, 'mx')).toEqual([2017, 2018])
    expect(yearsFor(ds, 'mexico')).toEqual([2017, 2018])
  })

  it('unions US-XX state entries when country is the US', () => {
    // EPA AQS shape: per-state coverage. NY drops out in 2021.
    const ds = {
      countries_covered: ['US-CA', 'US-NY'],
      years: [2020, 2021],
      years_by_country: {
        'US-CA': [2020, 2021],
        'US-NY': [2020],
      },
    }
    expect(yearsFor(ds, 'USA')).toEqual([2020, 2021])
    expect(yearsFor(ds, 'us')).toEqual([2020, 2021])
    expect(yearsFor(ds, 'united-states')).toEqual([2020, 2021])
  })

  it('returns empty when country has no entry in years_by_country', () => {
    const ds = {
      countries_covered: ['MEX'],
      years: [2017],
      years_by_country: { MEX: [2017] },
    }
    expect(yearsFor(ds, 'BRA')).toEqual([])
  })

  it('falls back to dataset.years when years_by_country is absent', () => {
    // Direct/legacy datasets without per-country breakdown still work.
    const ds = { countries_covered: ['MEX'], years: [2015, 2016] }
    expect(yearsFor(ds, 'MEX')).toEqual([2015, 2016])
  })
})
