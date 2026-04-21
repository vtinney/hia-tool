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
})
