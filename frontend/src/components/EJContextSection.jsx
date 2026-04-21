import { useEffect, useMemo, useState, useCallback } from 'react'
import TractChoroplethMap from './TractChoroplethMap'
import { fetchDemographics } from '../lib/api'
import {
  populationWeightedMean,
  pickVintage,
  studyAreaToFilter,
} from '../lib/demographics'

function fmtPct(v) {
  if (v == null || Number.isNaN(v)) return '—'
  return `${(v * 100).toFixed(1)}%`
}

export default function EJContextSection({
  studyArea,
  analysisYear,
  perTractResults,
  availableVintages,
}) {
  const [geojson, setGeojson] = useState(null)
  const [error, setError] = useState(null)
  const [field, setField] = useState('pct_minority')
  const [fetchNonce, setFetchNonce] = useState(0)

  const vintage = useMemo(
    () => pickVintage(analysisYear, availableVintages ?? []),
    [analysisYear, availableVintages],
  )
  const filter = useMemo(() => studyAreaToFilter(studyArea), [studyArea])

  const hasTractResults = Array.isArray(perTractResults) && perTractResults.length > 0

  useEffect(() => {
    if (!hasTractResults) return
    if (!vintage || filter == null) return
    let cancelled = false
    setError(null)
    setGeojson(null)
    fetchDemographics('us', vintage, filter)
      .then((data) => {
        if (cancelled) return
        if (!data) {
          setError(new Error(`Demographics not available for ${vintage}`))
          return
        }
        setGeojson(data)
      })
      .catch((err) => {
        if (!cancelled) setError(err)
      })
    return () => {
      cancelled = true
    }
  }, [vintage, filter, hasTractResults, fetchNonce])

  const retry = useCallback(() => setFetchNonce((n) => n + 1), [])

  // Join demographics features to per-tract HIA results by tract FIPS,
  // then compute population-weighted aggregates on the joined set.
  const joinedTracts = useMemo(() => {
    if (!geojson) return []
    const byFips = new Map(
      (perTractResults ?? []).map((r) => [String(r.tract_fips), r]),
    )
    return geojson.features.map((f) => {
      const hia = byFips.get(String(f.properties?.geoid))
      return {
        geoid: f.properties?.geoid,
        total_pop: f.properties?.total_pop,
        pct_minority: f.properties?.pct_minority,
        pct_below_200_pov: f.properties?.pct_below_200_pov,
        hia,
      }
    })
  }, [geojson, perTractResults])

  const pctMinority = useMemo(
    () => populationWeightedMean(joinedTracts, 'pct_minority'),
    [joinedTracts],
  )
  const pctBelow200Pov = useMemo(
    () => populationWeightedMean(joinedTracts, 'pct_below_200_pov'),
    [joinedTracts],
  )

  if (!hasTractResults) {
    return (
      <section className="mt-12 border-t border-zinc-200/80 pt-10">
        <h2 className="text-[22px] font-semibold tracking-tight mb-4">Environmental Justice Context</h2>
        <div className="rounded-xl border border-amber-300 bg-amber-50 p-5 text-[14px] text-amber-900">
          EJ context requires tract-resolution output; this analysis ran at zone resolution.
        </div>
      </section>
    )
  }

  return (
    <section className="mt-12 border-t border-zinc-200/80 pt-10">
      <div className="flex items-baseline justify-between mb-6">
        <h2 className="text-[22px] font-semibold tracking-tight">Environmental Justice Context</h2>
        <span className="font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-500">
          {studyArea?.name} · {vintage ? `${vintage} ACS` : 'vintage unavailable'}
        </span>
      </div>

      {error ? (
        <div className="rounded-xl border border-rose-300 bg-rose-50 p-5 text-[14px] text-rose-900 flex items-center justify-between">
          <span>Couldn't load demographic data.</span>
          <button
            type="button"
            onClick={retry}
            className="font-mono text-[11px] uppercase tracking-[0.12em] bg-rose-700 text-white px-3 py-1.5 rounded"
          >
            Retry
          </button>
        </div>
      ) : (
        <>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
            <div className="rounded-xl border border-zinc-200 p-6">
              <p className="font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-500 mb-2">
                Minority population share
              </p>
              <p
                data-testid="pct-minority-value"
                className="font-mono text-[40px] tabular-nums font-semibold text-ink leading-none"
              >
                {fmtPct(pctMinority)}
              </p>
            </div>
            <div className="rounded-xl border border-zinc-200 p-6">
              <p className="font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-500 mb-2">
                Population below 200% poverty line
              </p>
              <p
                data-testid="pct-below-200-pov-value"
                className="font-mono text-[40px] tabular-nums font-semibold text-ink leading-none"
              >
                {fmtPct(pctBelow200Pov)}
              </p>
            </div>
          </div>

          <div className="flex items-center gap-2 mb-4">
            <button
              type="button"
              onClick={() => setField('pct_minority')}
              className={`font-mono text-[11px] uppercase tracking-[0.12em] px-3 py-1.5 rounded border ${
                field === 'pct_minority'
                  ? 'bg-ink text-paper border-ink'
                  : 'bg-paper text-zinc-600 border-zinc-300'
              }`}
            >
              % Minority
            </button>
            <button
              type="button"
              onClick={() => setField('pct_below_200_pov')}
              className={`font-mono text-[11px] uppercase tracking-[0.12em] px-3 py-1.5 rounded border ${
                field === 'pct_below_200_pov'
                  ? 'bg-ink text-paper border-ink'
                  : 'bg-paper text-zinc-600 border-zinc-300'
              }`}
            >
              Below 200% poverty
            </button>
          </div>

          {geojson ? (
            <TractChoroplethMap geojson={geojson} field={field} />
          ) : (
            <div className="h-[480px] rounded-xl border border-zinc-200 bg-zinc-50 flex items-center justify-center text-zinc-400 font-mono text-[11px] uppercase tracking-[0.14em]">
              Loading demographics…
            </div>
          )}

          <p className="mt-4 font-mono text-[10px] uppercase tracking-[0.14em] text-zinc-400">
            Source: U.S. Census ACS 5-year estimates, vintage {vintage}
          </p>
        </>
      )}
    </section>
  )
}
