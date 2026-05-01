import { describe, it, expect } from 'vitest'
import library from '../crf-library.json'

const VALID_CAUSES = new Set([
  'all_cause', 'ihd', 'stroke', 'lung_cancer', 'copd', 'lri', 'dm2',
  'dementia', 'asthma', 'asthma_ed', 'respiratory_mortality',
  'respiratory_hosp', 'cardiovascular', 'cardiovascular_hosp',
  'cardiac_hosp', 'birth_weight', 'gestational_age',
])

const VALID_ENDPOINT_TYPES = new Set([
  'mortality', 'hospitalization', 'ed_visit', 'incidence', 'prevalence',
])

describe('crf-library', () => {
  it('every CRF has a valid cause', () => {
    for (const crf of library) {
      expect(VALID_CAUSES.has(crf.cause)).toBe(true)
    }
  })

  it('every CRF has a valid endpointType', () => {
    for (const crf of library) {
      expect(VALID_ENDPOINT_TYPES.has(crf.endpointType)).toBe(true)
    }
  })

  it('ids are unique', () => {
    const ids = library.map((c) => c.id)
    expect(new Set(ids).size).toBe(ids.length)
  })
})
