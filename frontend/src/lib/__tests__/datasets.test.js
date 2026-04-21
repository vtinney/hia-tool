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
})
