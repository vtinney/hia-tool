import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import EJContextSection from '../EJContextSection'

// Stub the choropleth — MapBox doesn't render in jsdom.
vi.mock('../TractChoroplethMap', () => ({
  default: ({ field }) => <div data-testid="choropleth" data-field={field} />,
}))

function mockGeojson(features) {
  return { type: 'FeatureCollection', features }
}

function tract(geoid, total_pop, pct_minority, pct_below_200_pov) {
  return {
    type: 'Feature',
    properties: { geoid, total_pop, pct_minority, pct_below_200_pov },
    geometry: { type: 'Polygon', coordinates: [[[0,0],[0,1],[1,1],[1,0],[0,0]]] },
  }
}

describe('EJContextSection', () => {
  let fetchSpy

  beforeEach(() => {
    fetchSpy = vi.spyOn(global, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
  })

  function okJson(payload) {
    return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(payload) })
  }

  const studyArea = { type: 'country', id: 'united-states', name: 'United States' }
  const perTractResults = [
    { tract_fips: '01', population: 1000, baseline_concentration: 12, control_concentration: 9,
      attributable_cases: { mean: 2, lower95: 1, upper95: 3 } },
    { tract_fips: '02', population: 3000, baseline_concentration: 14, control_concentration: 9,
      attributable_cases: { mean: 8, lower95: 5, upper95: 11 } },
  ]

  it('renders fallback banner when perTractResults is absent', () => {
    render(
      <EJContextSection
        studyArea={studyArea}
        analysisYear={2022}
        perTractResults={null}
        availableVintages={[2020, 2021, 2022]}
      />,
    )
    expect(screen.getByText(/EJ context requires tract-resolution output/i)).toBeInTheDocument()
    expect(fetchSpy).not.toHaveBeenCalled()
  })

  it('fetches, computes, and renders aggregate stats + choropleth', async () => {
    fetchSpy.mockReturnValueOnce(okJson(mockGeojson([
      tract('01', 1000, 0.5, 0.3),
      tract('02', 3000, 0.9, 0.4),
    ])))
    render(
      <EJContextSection
        studyArea={studyArea}
        analysisYear={2022}
        perTractResults={perTractResults}
        availableVintages={[2020, 2021, 2022]}
      />,
    )
    await waitFor(() => expect(screen.getByTestId('choropleth')).toBeInTheDocument())
    // Weighted: (1000*0.5 + 3000*0.9)/4000 = 0.8
    expect(screen.getByTestId('pct-minority-value').textContent).toMatch(/80\.0%/)
    // Weighted: (1000*0.3 + 3000*0.4)/4000 = 0.375
    expect(screen.getByTestId('pct-below-200-pov-value').textContent).toMatch(/37\.5%/)
  })

  it('renders error + retry button on fetch failure', async () => {
    fetchSpy.mockReturnValueOnce(Promise.resolve({ ok: false, status: 500 }))
    render(
      <EJContextSection
        studyArea={studyArea}
        analysisYear={2022}
        perTractResults={perTractResults}
        availableVintages={[2020, 2021, 2022]}
      />,
    )
    await waitFor(() =>
      expect(screen.getByText(/couldn't load demographic data/i)).toBeInTheDocument(),
    )
    expect(screen.getByRole('button', { name: /retry/i })).toBeInTheDocument()
  })

  it('shows the chosen vintage in the provenance footer', async () => {
    fetchSpy.mockReturnValueOnce(okJson(mockGeojson([tract('01', 1000, 0.5, 0.3)])))
    render(
      <EJContextSection
        studyArea={studyArea}
        analysisYear={2025}
        perTractResults={perTractResults}
        availableVintages={[2020, 2021, 2022]}
      />,
    )
    // 2025 falls back to 2022 (post-2020 side, closest).
    await waitFor(() => expect(screen.getByText(/2022 ACS/i)).toBeInTheDocument())
  })

  it('excludes tracts without HIA results from aggregate stats', async () => {
    // Demographics returns 3 tracts but per-tract HIA results only covers 2.
    // Aggregate must weight only over the 2 that the engine computed (01, 02),
    // NOT include the extra tract '99' that has demographics but no HIA result.
    fetchSpy.mockReturnValueOnce(okJson(mockGeojson([
      tract('01', 1000, 0.5, 0.3),
      tract('02', 3000, 0.9, 0.4),
      tract('99', 5000, 0.1, 0.05),  // has demographics but no HIA result
    ])))
    render(
      <EJContextSection
        studyArea={studyArea}
        analysisYear={2022}
        perTractResults={perTractResults}  // only covers 01 and 02
        availableVintages={[2020, 2021, 2022]}
      />,
    )
    await waitFor(() => expect(screen.getByTestId('choropleth')).toBeInTheDocument())
    // Should still be 0.8 (weighted over 01 and 02 only), NOT diluted by tract 99.
    // If '99' were included: (1000*0.5 + 3000*0.9 + 5000*0.1)/9000 = 0.411 — wrong.
    expect(screen.getByTestId('pct-minority-value').textContent).toMatch(/80\.0%/)
  })

  it('toggles the map field when user clicks the toggle', async () => {
    fetchSpy.mockReturnValueOnce(okJson(mockGeojson([
      tract('01', 1000, 0.5, 0.3),
      tract('02', 3000, 0.9, 0.4),
    ])))
    const { findByTestId, getByRole } = render(
      <EJContextSection
        studyArea={studyArea}
        analysisYear={2022}
        perTractResults={perTractResults}
        availableVintages={[2020, 2021, 2022]}
      />,
    )
    const cp = await findByTestId('choropleth')
    expect(cp.getAttribute('data-field')).toBe('pct_minority')
    getByRole('button', { name: /below 200% poverty/i }).click()
    await waitFor(() =>
      expect(cp.getAttribute('data-field')).toBe('pct_below_200_pov'),
    )
  })
})
