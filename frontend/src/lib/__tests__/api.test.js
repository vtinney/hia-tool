import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { fetchDemographics } from '../api'

describe('fetchDemographics', () => {
  let fetchSpy

  beforeEach(() => {
    fetchSpy = vi.spyOn(global, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
  })

  function ok(payload) {
    return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(payload) })
  }

  it('calls the endpoint with no query params for nationwide', async () => {
    fetchSpy.mockReturnValueOnce(ok({ type: 'FeatureCollection', features: [] }))
    const res = await fetchDemographics('us', 2022)
    expect(fetchSpy).toHaveBeenCalledWith('/api/data/demographics/us/2022')
    expect(res).toEqual({ type: 'FeatureCollection', features: [] })
  })

  it('includes state when provided', async () => {
    fetchSpy.mockReturnValueOnce(ok({ type: 'FeatureCollection', features: [] }))
    await fetchDemographics('us', 2022, { state: '48' })
    expect(fetchSpy).toHaveBeenCalledWith('/api/data/demographics/us/2022?state=48')
  })

  it('includes state and county when both provided', async () => {
    fetchSpy.mockReturnValueOnce(ok({ type: 'FeatureCollection', features: [] }))
    await fetchDemographics('us', 2022, { state: '48', county: '201' })
    expect(fetchSpy).toHaveBeenCalledWith(
      '/api/data/demographics/us/2022?state=48&county=201',
    )
  })

  it('includes simplify when provided', async () => {
    fetchSpy.mockReturnValueOnce(ok({ type: 'FeatureCollection', features: [] }))
    await fetchDemographics('us', 2022, { state: '48', simplify: 0 })
    expect(fetchSpy).toHaveBeenCalledWith(
      '/api/data/demographics/us/2022?state=48&simplify=0',
    )
  })

  it('returns null on 404', async () => {
    fetchSpy.mockReturnValueOnce(Promise.resolve({ ok: false, status: 404 }))
    const res = await fetchDemographics('us', 1999)
    expect(res).toBeNull()
  })

  it('throws on non-404 error', async () => {
    fetchSpy.mockReturnValueOnce(Promise.resolve({ ok: false, status: 500 }))
    await expect(fetchDemographics('us', 2022)).rejects.toThrow(/500/)
  })
})
