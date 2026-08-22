import { describe, it, expect } from 'vitest'
import { detailRowsFromSpatial, spatialHeadlineDeaths, spatialSummaryStats } from '../spatialResults'

const ci = (mean, lower95, upper95) => ({ mean, lower95, upper95 })

const response = {
  zones: [{ zoneId: '48201554002' }],
  aggregate: {
    results: [
      {
        crfId: 'epa_pm25_acm_adult',
        study: 'Turner et al. 2016 (ACS CPS-II)',
        endpoint: 'All-cause mortality',
        attributableCases: ci(12179, 8344, 15926),
        attributableFraction: ci(0.0531, 0.0364, 0.0695),
        attributableRate: ci(42.5, 29.1, 55.6),
      },
      {
        crfId: 'epa_pm25_ihd_adult',
        study: 'Pope et al. 2004 (ACS CPS-II)',
        endpoint: 'Ischemic heart disease',
        attributableCases: ci(9648, 5000, 14000),
        attributableFraction: ci(0.14, 0.09, 0.19),
        attributableRate: ci(33.7, 17.5, 48.9),
      },
    ],
  },
  totalDeaths: ci(9648, 5000, 14000),
  allCauseDeaths: ci(12179, 8344, 15926),
}

describe('detailRowsFromSpatial', () => {
  const lookup = {
    epa_pm25_acm_adult: { framework: 'EPA Standard' },
    epa_pm25_ihd_adult: { framework: 'EPA Standard' },
  }

  it('maps aggregate CRF results to the Results-page detail row shape', () => {
    const rows = detailRowsFromSpatial(response, lookup)
    expect(rows).toHaveLength(2)
    expect(rows[0]).toEqual({
      crfStudy: 'Turner et al. 2016 (ACS CPS-II)',
      framework: 'EPA Standard',
      endpoint: 'All-cause mortality',
      attributableCases: 12179,
      lower95: 8344,
      upper95: 15926,
      attributableFraction: 0.0531,
      ratePer100k: 42.5,
    })
  })

  it('tolerates a missing framework lookup', () => {
    const rows = detailRowsFromSpatial(response, {})
    expect(rows[1].framework).toBe('')
  })

  it('returns [] for a response without aggregate results', () => {
    expect(detailRowsFromSpatial({}, {})).toEqual([])
    expect(detailRowsFromSpatial(null, {})).toEqual([])
  })
})

describe('spatialSummaryStats', () => {
  it('takes fraction and rate from the CRF behind the headline total', () => {
    expect(spatialSummaryStats(response)).toEqual({
      attributableFraction: 0.0531,
      attributableRate: 42.5,
    })
  })

  it('follows the headline CRF even when another endpoint has more cases', () => {
    // NO₂-style run: pediatric asthma incidence (68k cases) dwarfs the
    // all-cause deaths headline (19.7k) — the tiles must describe the
    // mortality CRF the hero shows, not the asthma row.
    const mixed = {
      zones: [{}],
      aggregate: {
        results: [
          {
            crfId: 'epa_no2_acm_adult', endpoint: 'All-cause mortality',
            attributableCases: ci(19701, 8094, 30722),
            attributableFraction: ci(0.067, 0.028, 0.105),
            attributableRate: ci(68.8, 28.3, 107.3),
          },
          {
            crfId: 'gbd_no2_asthma_child', endpoint: 'Asthma incidence (pediatric)',
            attributableCases: ci(68454, -84784, 166286),
            attributableFraction: ci(0.202, -0.25, 0.49),
            attributableRate: ci(239.1, -296.1, 580.8),
          },
        ],
      },
      totalDeaths: ci(0, 0, 0),
      allCauseDeaths: ci(19701, 8094, 30722),
    }
    expect(spatialSummaryStats(mixed)).toEqual({
      attributableFraction: 0.067,
      attributableRate: 68.8,
    })
  })

  it('returns nulls for an empty response', () => {
    expect(spatialSummaryStats({})).toEqual({
      attributableFraction: null,
      attributableRate: null,
    })
  })
})

describe('spatialHeadlineDeaths', () => {
  it('prefers allCauseDeaths when an all-cause CRF was in the run', () => {
    expect(spatialHeadlineDeaths(response)).toEqual(ci(12179, 8344, 15926))
  })

  it('falls back to cause-specific totalDeaths when no all-cause CRF ran', () => {
    const causeOnly = { ...response, allCauseDeaths: null }
    expect(spatialHeadlineDeaths(causeOnly)).toEqual(ci(9648, 5000, 14000))
  })

  it('returns null when both totals are empty so the hero can use detail rows', () => {
    const empty = { ...response, allCauseDeaths: null, totalDeaths: ci(0, 0, 0) }
    expect(spatialHeadlineDeaths(empty)).toBeNull()
    expect(spatialHeadlineDeaths(null)).toBeNull()
  })
})
