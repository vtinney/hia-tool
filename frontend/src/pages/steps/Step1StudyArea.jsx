import { useEffect, useRef, useState, useCallback, useMemo } from 'react'
import useAnalysisStore from '../../stores/useAnalysisStore'
import { uploadFile, fetchUrbanCentres } from '../../lib/api'
import { filterCentres, topNIds } from '../../lib/urbanCentres'
import countries from '../../data/countries.json'
import usStates from '../../data/us-states.json'

const MAPBOX_TOKEN = import.meta.env.VITE_MAPBOX_TOKEN

// ── Pollutant definitions ───────────────────────────────────────

const POLLUTANTS = [
  {
    id: 'pm25',
    label: 'PM2.5',
    metric: 'Annual average',
    tip: 'Fine particulate matter ≤2.5 μm. Annual mean concentration in μg/m³, the standard metric for chronic exposure studies.',
  },
  {
    id: 'ozone',
    label: 'Ozone',
    metric: 'Seasonal daily 8-hr max',
    tip: 'Daily maximum 8-hour average during the ozone season (Apr–Sep), measured in ppb.',
  },
  {
    id: 'no2',
    label: 'NO₂',
    metric: 'Annual average',
    tip: 'Nitrogen dioxide annual mean in ppb. Strongly correlated with traffic-related air pollution.',
  },
]

const US_ANALYSIS_LEVELS = [
  { id: 'state', label: 'State level' },
  { id: 'county', label: 'County level' },
  { id: 'tract', label: 'Census Tract level' },
]

// Non-US countries get a coarser set: country-scalar (legacy WHO AAP fallback)
// or GADM admin-2. Admin-2 is the default — country scalar stays available
// for the older path during the transition.
const NON_US_ANALYSIS_LEVELS = [
  { id: 'adm2', label: 'Admin-2 (district / municipality)' },
  { id: 'urban', label: 'Urban centres (cities)' },
  { id: 'country', label: 'Country average' },
]

// ── Sorted country list (USA pinned to top) ─────────────────────

const sortedCountries = [
  countries.find((c) => c.iso === 'USA'),
  ...countries.filter((c) => c.iso !== 'USA').sort((a, b) => a.name.localeCompare(b.name)),
]

// ── Tooltip component ───────────────────────────────────────────

function InfoTooltip({ text }) {
  const [open, setOpen] = useState(false)

  return (
    <span className="relative inline-block ml-1">
      <button
        type="button"
        className="w-4 h-4 rounded-full bg-gray-200 text-gray-500 text-[10px] font-bold
                   leading-none inline-flex items-center justify-center hover:bg-gray-300
                   focus:outline-none focus:ring-2 focus:ring-blue-400"
        onMouseEnter={() => setOpen(true)}
        onMouseLeave={() => setOpen(false)}
        onFocus={() => setOpen(true)}
        onBlur={() => setOpen(false)}
        aria-label="More info"
      >
        ?
      </button>
      {open && (
        <div
          className="absolute z-50 bottom-full left-1/2 -translate-x-1/2 mb-2 w-56 px-3 py-2
                     bg-gray-800 text-white text-xs rounded-lg shadow-lg leading-relaxed
                     pointer-events-none"
        >
          {text}
          <div className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-gray-800" />
        </div>
      )}
    </span>
  )
}

// ── City picker component ───────────────────────────────────────

// Searchable multi-select for GHS-UCDB urban centres. Empty selection = all
// centres in the country. Centres arrive population-sorted from the API.
function CityPicker({ countryIso, cityIds, onChange }) {
  const [centres, setCentres] = useState(null)   // null = loading
  const [error, setError] = useState(null)
  const [query, setQuery] = useState('')
  const [topN, setTopN] = useState(10)

  useEffect(() => {
    let cancelled = false
    setCentres(null)
    setError(null)
    fetchUrbanCentres(countryIso)
      .then((data) => {
        if (cancelled) return
        if (!data) setError('No urban-centre data for this country.')
        else setCentres(data.centres)
      })
      .catch((err) => { if (!cancelled) setError(err.message) })
    return () => { cancelled = true }
  }, [countryIso])

  if (error) return <p className="text-xs text-red-600 mt-2">{error}</p>
  if (!centres) return <p className="text-xs text-gray-400 mt-2">Loading urban centres…</p>

  const selected = new Set(cityIds || [])
  const visible = filterCentres(centres, query).slice(0, 200)

  const toggle = (id) => {
    const next = new Set(selected)
    if (next.has(id)) next.delete(id)
    else next.add(id)
    onChange([...next])
  }

  return (
    <div className="mt-3 space-y-2">
      <div className="flex items-center gap-2 text-xs text-gray-600">
        <span>
          {selected.size === 0
            ? `All ${centres.length} centres`
            : `${selected.size} of ${centres.length} centres selected`}
        </span>
        {selected.size > 0 && (
          <button type="button" onClick={() => onChange([])}
                  className="text-blue-600 underline">
            Clear (use all)
          </button>
        )}
      </div>
      <div className="flex gap-2">
        <input
          type="text" value={query} placeholder="Search cities…"
          onChange={(e) => setQuery(e.target.value)}
          className="flex-1 rounded-lg border border-gray-300 px-3 py-1.5 text-sm"
        />
        <input
          type="number" min="1" value={topN}
          onChange={(e) => setTopN(Number(e.target.value))}
          className="w-16 rounded-lg border border-gray-300 px-2 py-1.5 text-sm"
          aria-label="Top N count"
        />
        <button
          type="button"
          onClick={() => onChange(topNIds(centres, topN))}
          className="rounded-lg border border-gray-300 px-3 py-1.5 text-sm hover:bg-gray-50"
        >
          Top {topN} by population
        </button>
      </div>
      <div className="max-h-48 overflow-y-auto rounded-lg border border-gray-200 divide-y divide-gray-100">
        {visible.map((c) => (
          <label key={c.id}
                 className="flex items-center gap-2 px-3 py-1.5 text-sm cursor-pointer hover:bg-gray-50">
            <input
              type="checkbox"
              checked={selected.has(c.id)}
              onChange={() => toggle(c.id)}
              className="text-blue-600 focus:ring-blue-500"
            />
            <span className="flex-1">{c.name || `Centre ${c.id}`}</span>
            <span className="text-xs text-gray-400">
              {Math.round(c.population).toLocaleString()}
            </span>
          </label>
        ))}
        {visible.length === 0 && (
          <p className="px-3 py-2 text-xs text-gray-400">No matches.</p>
        )}
      </div>
    </div>
  )
}

// ── Map component ───────────────────────────────────────────────

function StudyAreaMap({ selectedCountry, selectedState }) {
  const containerRef = useRef(null)
  const mapRef = useRef(null)
  const mapReady = useRef(false)

  // Initialize map
  useEffect(() => {
    if (!MAPBOX_TOKEN || mapRef.current) return

    let cancelled = false

    import('mapbox-gl').then((mapboxgl) => {
      if (cancelled || !containerRef.current) return

      mapboxgl.default.accessToken = MAPBOX_TOKEN

      const map = new mapboxgl.default.Map({
        container: containerRef.current,
        style: 'mapbox://styles/mapbox/light-v11',
        center: [0, 20],
        zoom: 1.5,
        projection: 'mercator',
      })

      map.addControl(new mapboxgl.default.NavigationControl(), 'top-right')

      map.on('load', () => {
        if (cancelled) return
        mapReady.current = true

        // Country highlight layer using Mapbox boundaries tileset
        map.addSource('country-boundaries', {
          type: 'vector',
          url: 'mapbox://mapbox.country-boundaries-v1',
        })

        map.addLayer({
          id: 'country-fill',
          type: 'fill',
          source: 'country-boundaries',
          'source-layer': 'country_boundaries',
          paint: {
            'fill-color': '#3b82f6',
            'fill-opacity': 0.15,
          },
          filter: ['==', 'iso_3166_1', ''],
        })

        map.addLayer({
          id: 'country-outline',
          type: 'line',
          source: 'country-boundaries',
          'source-layer': 'country_boundaries',
          paint: {
            'line-color': '#2563eb',
            'line-width': 2,
          },
          filter: ['==', 'iso_3166_1', ''],
        })
      })

      mapRef.current = map
    })

    return () => {
      cancelled = true
      if (mapRef.current) {
        mapRef.current.remove()
        mapRef.current = null
        mapReady.current = false
      }
    }
  }, [])

  // Update highlight when country changes
  useEffect(() => {
    const map = mapRef.current
    if (!map || !mapReady.current) return

    const iso2 = selectedCountry?.iso2 || ''

    map.setFilter('country-fill', ['==', 'iso_3166_1', iso2])
    map.setFilter('country-outline', ['==', 'iso_3166_1', iso2])

    if (selectedCountry?.bbox) {
      const [w, s, e, n] = selectedCountry.bbox
      map.fitBounds(
        [[w, s], [e, n]],
        { padding: 40, duration: 1200, maxZoom: 8 },
      )
    }
  }, [selectedCountry])

  // Zoom tighter for US state selection
  useEffect(() => {
    const map = mapRef.current
    if (!map || !mapReady.current || !selectedState?.bbox) return

    const [w, s, e, n] = selectedState.bbox
    map.fitBounds(
      [[w, s], [e, n]],
      { padding: 40, duration: 1000, maxZoom: 10 },
    )
  }, [selectedState])

  if (!MAPBOX_TOKEN) {
    return (
      <div className="w-full h-full rounded-xl bg-gray-100 flex items-center justify-center text-gray-400 text-sm text-center p-4">
        Set VITE_MAPBOX_TOKEN in your .env file to enable the map.
      </div>
    )
  }

  return (
    <div ref={containerRef} className="w-full h-full rounded-xl overflow-hidden" />
  )
}

// ── Main step component ─────────────────────────────────────────

export default function Step1StudyArea() {
  const { step1, setStep1, setStepValidity, ejFraming } = useAnalysisStore()
  const { studyArea, pollutant, analysisName, analysisDescription } = step1

  // Derived lookup objects for map
  const selectedCountry = useMemo(
    () => countries.find((c) => c.iso === studyArea.id) ?? null,
    [studyArea.id],
  )

  const selectedState = useMemo(
    () => usStates.find((s) => s.fips === studyArea.stateId) ?? null,
    [studyArea.stateId],
  )

  const isUSA = studyArea.id === 'USA'

  // ── Validation ─────────────────────────────────────────────────

  useEffect(() => {
    // A US tract-level run must scope to a state (all-US tract is infeasible),
    // so it isn't valid until a state is chosen.
    const needsState = isUSA && (studyArea.analysisLevel || 'state') === 'tract'
    const valid =
      Boolean(studyArea.id) &&
      Boolean(pollutant) &&
      (!needsState || Boolean(studyArea.stateId))
    setStepValidity(1, valid)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [studyArea.id, pollutant, isUSA, studyArea.analysisLevel, studyArea.stateId])

  // ── Handlers ───────────────────────────────────────────────────

  const handleCountryChange = useCallback((iso) => {
    const country = countries.find((c) => c.iso === iso)
    if (!country) {
      setStep1({ studyArea: { type: 'country', id: '', name: '', geometry: null } })
      return
    }
    setStep1({
      studyArea: {
        type: 'country',
        id: country.iso,
        name: country.name,
        geometry: null,
        // US gets the existing tract/county/state path. Non-US defaults to
        // GADM admin-2 (the new global finer-than-country path); the user
        // can fall back to country-scalar via the analysis-level radios.
        ...(country.iso === 'USA'
          ? { stateId: '', stateName: '', analysisLevel: 'state' }
          : { analysisLevel: 'adm2', cityIds: [] }),
      },
    })
  }, [setStep1])

  const handleStateChange = useCallback((fips) => {
    const state = usStates.find((s) => s.fips === fips)
    setStep1({
      studyArea: {
        ...step1.studyArea,
        stateId: state?.fips || '',
        stateName: state?.name || '',
        type: state ? step1.studyArea.analysisLevel || 'state' : 'country',
      },
    })
  }, [setStep1, step1.studyArea])

  const handleAnalysisLevel = useCallback((level) => {
    setStep1({
      studyArea: { ...step1.studyArea, type: level, analysisLevel: level },
    })
  }, [setStep1, step1.studyArea])

  const [boundaryUploading, setBoundaryUploading] = useState(false)
  const [boundaryError, setBoundaryError] = useState(null)
  const [boundaryMeta, setBoundaryMeta] = useState(null)
  const boundaryInputRef = useRef(null)

  const handleBoundaryUpload = useCallback(async (file) => {
    const ext = file.name.split('.').pop().toLowerCase()
    if (!['zip', 'gpkg', 'geojson'].includes(ext)) {
      setBoundaryError('Accepted formats: .zip (shapefile), .gpkg, .geojson')
      return
    }
    if (file.size > 500 * 1024 * 1024) {
      setBoundaryError('File exceeds 500 MB limit.')
      return
    }
    setBoundaryUploading(true)
    setBoundaryError(null)
    try {
      const result = await uploadFile(file, 'boundary')
      setStep1({
        studyArea: {
          ...step1.studyArea,
          type: 'custom',
          id: 'custom',
          name: file.name,
          boundaryUploadId: result.id,
        },
      })
      setBoundaryMeta(result.metadata_json)
    } catch (err) {
      setBoundaryError(err.message)
    } finally {
      setBoundaryUploading(false)
    }
  }, [setStep1, step1.studyArea])

  const handleClearBoundary = useCallback(() => {
    setStep1({
      studyArea: { ...step1.studyArea, type: 'country', id: '', name: '', boundaryUploadId: null },
    })
    setBoundaryMeta(null)
    setBoundaryError(null)
  }, [setStep1, step1.studyArea])

  // ── Render ─────────────────────────────────────────────────────

  return (
    <>
      <h1 className="text-2xl font-bold text-gray-900 mb-6">Define Study Area</h1>

      <div className="flex flex-col lg:flex-row gap-6">
        {/* ── Left column (form) ─────────────────────────────── */}
        <div className="lg:w-2/3 space-y-6">
          {/* Country */}
          <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
            <legend className="text-sm font-semibold text-gray-700 px-1">Country / Region</legend>
            <select
              value={studyArea.id}
              onChange={(e) => handleCountryChange(e.target.value)}
              className="w-full rounded-lg border border-gray-300 px-3 py-2.5 text-sm
                         focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
            >
              <option value="">Select a country…</option>
              {sortedCountries.map((c) => (
                <option key={c.iso} value={c.iso}>{c.name}</option>
              ))}
            </select>

            {/* US-specific drilldown */}
            {isUSA && (
              <div className="mt-4 space-y-3">
                {ejFraming && (
                  <div className="rounded-lg border border-indigo-200 bg-indigo-50 px-3 py-2 text-xs text-indigo-800">
                    Environmental Justice analysis runs at <span className="font-medium">Census Tract level</span> — select a state below to analyze its tracts.
                  </div>
                )}
                <select
                  value={studyArea.stateId || ''}
                  onChange={(e) => handleStateChange(e.target.value)}
                  className="w-full rounded-lg border border-gray-300 px-3 py-2.5 text-sm
                             focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
                >
                  <option value="">Select a state…</option>
                  {usStates
                    .filter((s) => s.abbr !== 'DC' || true)
                    .sort((a, b) => a.name.localeCompare(b.name))
                    .map((s) => (
                      <option key={s.fips} value={s.fips}>{s.name}</option>
                    ))}
                </select>

                {studyArea.stateId && (
                  <div>
                    <p className="text-xs text-gray-500 mb-2">Analysis level</p>
                    <div className="flex flex-wrap gap-3">
                      {US_ANALYSIS_LEVELS.map(({ id, label }) => (
                        <label key={id} className="flex items-center gap-1.5 text-sm cursor-pointer">
                          <input
                            type="radio"
                            name="analysisLevel"
                            value={id}
                            checked={(studyArea.analysisLevel || 'state') === id}
                            onChange={() => handleAnalysisLevel(id)}
                            className="text-blue-600 focus:ring-blue-500"
                          />
                          {label}
                        </label>
                      ))}
                    </div>
                    {ejFraming && (studyArea.analysisLevel || 'state') !== 'tract' && (
                      <div className="mt-2 rounded-lg border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-800">
                        The Environmental Justice context section only appears for <span className="font-medium">Census Tract level</span>. Select it above to see the screening.
                      </div>
                    )}
                  </div>
                )}
              </div>
            )}

            {/* Non-US drilldown: analysis-level radios for the GADM admin-2
                / country-scalar split. Only shown once a country is picked. */}
            {!isUSA && studyArea.id && (
              <div className="mt-4">
                <p className="text-xs text-gray-500 mb-2">Analysis level</p>
                <div className="flex flex-wrap gap-3">
                  {NON_US_ANALYSIS_LEVELS.map(({ id, label }) => {
                    const disabled = id === 'urban' && pollutant !== 'pm25'
                    return (
                      <label key={id}
                             className={`flex items-center gap-1.5 text-sm ${disabled ? 'opacity-40 cursor-not-allowed' : 'cursor-pointer'}`}>
                        <input
                          type="radio"
                          name="analysisLevel"
                          value={id}
                          disabled={disabled}
                          checked={(studyArea.analysisLevel || 'adm2') === id}
                          onChange={() => handleAnalysisLevel(id)}
                          className="text-blue-600 focus:ring-blue-500"
                        />
                        {label}
                        {disabled && <InfoTooltip text="Urban-centre data is PM2.5 only." />}
                      </label>
                    )
                  })}
                </div>
                {(studyArea.analysisLevel || 'adm2') === 'urban' && (
                  <CityPicker
                    countryIso={studyArea.id}
                    cityIds={studyArea.cityIds}
                    onChange={(ids) =>
                      setStep1({ studyArea: { ...step1.studyArea, cityIds: ids } })
                    }
                  />
                )}
              </div>
            )}
          </fieldset>

          {/* Custom Boundary Upload */}
          <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
            <legend className="text-sm font-semibold text-gray-700 px-1">
              Custom Boundary <span className="text-xs font-normal text-gray-400">(optional)</span>
            </legend>
            <p className="text-xs text-gray-500 mb-3">
              Upload a shapefile (.zip), GeoPackage (.gpkg), or GeoJSON to define custom study area boundaries for spatial analysis.
            </p>

            {studyArea.boundaryUploadId ? (
              <div className="space-y-2">
                <div className="flex items-center justify-between p-3 bg-green-50 border border-green-200 rounded-lg">
                  <div className="flex items-center gap-2 text-sm text-green-800">
                    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                    </svg>
                    <span className="font-medium">{studyArea.name}</span>
                  </div>
                  <button onClick={handleClearBoundary} className="text-green-600 hover:text-green-800 text-sm underline">
                    Remove
                  </button>
                </div>
                {boundaryMeta && (
                  <div className="px-3 py-2 bg-gray-50 border border-gray-200 rounded-lg text-xs text-gray-600 space-y-0.5">
                    <p><span className="font-medium">Features:</span> {boundaryMeta.feature_count} zones</p>
                    {boundaryMeta.geometry_types && (
                      <p><span className="font-medium">Geometry:</span> {boundaryMeta.geometry_types.join(', ')}</p>
                    )}
                    {boundaryMeta.columns?.length > 0 && (
                      <p><span className="font-medium">Columns:</span> {boundaryMeta.columns.slice(0, 5).join(', ')}{boundaryMeta.columns.length > 5 ? '...' : ''}</p>
                    )}
                  </div>
                )}
              </div>
            ) : (
              <div>
                <button
                  type="button"
                  onClick={() => boundaryInputRef.current?.click()}
                  disabled={boundaryUploading}
                  className="px-4 py-2 text-sm font-medium rounded-lg border border-gray-300 text-gray-700
                             hover:bg-gray-50 transition-colors disabled:opacity-50"
                >
                  {boundaryUploading ? 'Uploading...' : 'Upload Boundary File'}
                </button>
                <input
                  ref={boundaryInputRef}
                  type="file"
                  accept=".zip,.gpkg,.geojson"
                  className="hidden"
                  onChange={(e) => { if (e.target.files?.[0]) handleBoundaryUpload(e.target.files[0]) }}
                />
                {boundaryError && (
                  <p className="mt-2 text-sm text-red-600">{boundaryError}</p>
                )}
              </div>
            )}
          </fieldset>

          {/* Pollutant */}
          <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
            <legend className="text-sm font-semibold text-gray-700 px-1">Pollutant</legend>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              {POLLUTANTS.map((p) => (
                <label
                  key={p.id}
                  className={`
                    flex items-start gap-3 p-3 rounded-lg border cursor-pointer transition-colors
                    ${pollutant === p.id
                      ? 'border-blue-500 bg-blue-50'
                      : 'border-gray-200 hover:border-gray-300 hover:bg-gray-50'}
                  `}
                >
                  <input
                    type="radio"
                    name="pollutant"
                    value={p.id}
                    checked={pollutant === p.id}
                    onChange={() => setStep1({ pollutant: p.id })}
                    className="mt-0.5 text-blue-600 focus:ring-blue-500"
                  />
                  <div className="min-w-0">
                    <span className="text-sm font-medium text-gray-900">
                      {p.label}
                      <InfoTooltip text={p.tip} />
                    </span>
                    <p className="text-xs text-gray-500 mt-0.5">{p.metric}</p>
                  </div>
                </label>
              ))}
            </div>
          </fieldset>

          {/* Name & description */}
          <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5 space-y-4">
            <legend className="text-sm font-semibold text-gray-700 px-1">Analysis Details</legend>
            <div>
              <label htmlFor="analysisName" className="block text-sm text-gray-600 mb-1">
                Analysis name
              </label>
              <input
                id="analysisName"
                type="text"
                value={analysisName}
                onChange={(e) => setStep1({ analysisName: e.target.value })}
                placeholder="e.g. PM2.5 health impact — Cook County 2020"
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm
                           focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              />
            </div>
            <div>
              <label htmlFor="analysisDesc" className="block text-sm text-gray-600 mb-1">
                Description <span className="text-gray-400">(optional)</span>
              </label>
              <textarea
                id="analysisDesc"
                rows={3}
                value={analysisDescription}
                onChange={(e) => setStep1({ analysisDescription: e.target.value })}
                placeholder="Briefly describe the purpose of this analysis…"
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm
                           resize-y focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              />
            </div>
          </fieldset>
        </div>

        {/* ── Right column (map) ─────────────────────────────── */}
        <div className="lg:w-1/3">
          <div className="sticky top-6 h-[420px] lg:h-[560px]">
            <StudyAreaMap
              selectedCountry={selectedCountry}
              selectedState={selectedState}
            />
          </div>
        </div>
      </div>
    </>
  )
}
