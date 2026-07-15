import { describe, it, expect } from 'vitest'
import { filterCentres, topNIds } from '../urbanCentres'

const CENTRES = [
  { id: '1', name: 'Mexico City', population: 19_000_000 },
  { id: '2', name: 'Guadalajara', population: 5_000_000 },
  { id: '3', name: 'Monterrey', population: 4_800_000 },
]

describe('filterCentres', () => {
  it('matches case-insensitively on name', () => {
    expect(filterCentres(CENTRES, 'monte')).toEqual([CENTRES[2]])
  })
  it('returns all centres for an empty query', () => {
    expect(filterCentres(CENTRES, '')).toEqual(CENTRES)
    expect(filterCentres(CENTRES, null)).toEqual(CENTRES)
  })
  it('handles null names without throwing', () => {
    const withNull = [...CENTRES, { id: '4', name: null, population: 10 }]
    expect(filterCentres(withNull, 'mex')).toEqual([CENTRES[0]])
  })
})

describe('topNIds', () => {
  it('returns the N most-populous ids (input already sorted desc)', () => {
    expect(topNIds(CENTRES, 2)).toEqual(['1', '2'])
  })
  it('caps at the list length', () => {
    expect(topNIds(CENTRES, 99)).toEqual(['1', '2', '3'])
  })
  it('returns [] for non-positive N', () => {
    expect(topNIds(CENTRES, 0)).toEqual([])
  })
})
