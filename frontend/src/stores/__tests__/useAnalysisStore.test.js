import { describe, it, expect, beforeEach } from 'vitest'
import useAnalysisStore from '../useAnalysisStore'

describe('useAnalysisStore — ejFraming', () => {
  beforeEach(() => {
    useAnalysisStore.getState().reset()
  })

  it('initial state has ejFraming=false', () => {
    expect(useAnalysisStore.getState().ejFraming).toBe(false)
  })

  it('loadFromTemplate propagates ejFraming=true when template sets it', () => {
    useAnalysisStore.getState().loadFromTemplate({
      ejFraming: true,
      step1: { studyArea: { type: 'country', id: 'united-states', name: 'United States' } },
    })
    expect(useAnalysisStore.getState().ejFraming).toBe(true)
  })

  it('loadFromTemplate defaults ejFraming to false when template omits it', () => {
    useAnalysisStore.getState().loadFromTemplate({
      step1: { studyArea: { type: 'country', id: 'mexico', name: 'Mexico' } },
    })
    expect(useAnalysisStore.getState().ejFraming).toBe(false)
  })

  it('reset() clears ejFraming back to false', () => {
    useAnalysisStore.getState().loadFromTemplate({ ejFraming: true })
    expect(useAnalysisStore.getState().ejFraming).toBe(true)
    useAnalysisStore.getState().reset()
    expect(useAnalysisStore.getState().ejFraming).toBe(false)
  })

  it('the us_tract_pm25_ej template carries ejFraming=true', async () => {
    const tpl = await import('../../data/templates/us_tract_pm25_ej.json')
    expect(tpl.default.ejFraming).toBe(true)
  })

  it('the us_national_pm25 template does NOT carry ejFraming', async () => {
    const tpl = await import('../../data/templates/us_national_pm25.json')
    expect(tpl.default.ejFraming).toBeUndefined()
  })

  it('exportConfig includes ejFraming so user-saved templates replay as EJ', () => {
    useAnalysisStore.getState().loadFromTemplate({
      ejFraming: true,
      step1: { studyArea: { type: 'country', id: 'united-states', name: 'United States' } },
    })
    const config = useAnalysisStore.getState().exportConfig()
    expect(config.ejFraming).toBe(true)
  })

  it('exportConfig emits ejFraming=false for non-EJ runs', () => {
    useAnalysisStore.getState().loadFromTemplate({
      step1: { studyArea: { type: 'country', id: 'mexico', name: 'Mexico' } },
    })
    const config = useAnalysisStore.getState().exportConfig()
    expect(config.ejFraming).toBe(false)
  })
})
