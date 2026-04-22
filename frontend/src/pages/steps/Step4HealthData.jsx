import { useEffect, useState, useCallback, useMemo, useRef } from 'react'
import useAnalysisStore from '../../stores/useAnalysisStore'
import { fetchIncidence } from '../../lib/api'
import crfLibrary from '../../data/crf-library.json'
import YearField from '../../components/YearField'

// ── Constants ──────────────────────────────────────────────────────

const POLLUTANT_LABELS = {
  pm25: 'PM2.5',
  ozone: 'Ozone',
  no2: 'NO₂',
}

const CSV_EXPECTED_COLUMNS = ['endpoint', 'age_group', 'rate']

// ── Tab bar ────────────────────────────────────────────────────────

function TabBar({ tabs, activeTab, onTabChange }) {
  return (
    <div className="flex border-b border-gray-200 mb-4" role="tablist">
      {tabs.map((tab) => (
        <button
          key={tab.id}
          role="tab"
          aria-selected={activeTab === tab.id}
          onClick={() => onTabChange(tab.id)}
          className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors -mb-px
            ${activeTab === tab.id
              ? 'border-blue-500 text-blue-600'
              : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'}`}
        >
          {tab.label}
        </button>
      ))}
    </div>
  )
}

// ── CSV upload with preview ────────────────────────────────────────

function CsvUpload({ fileData, onFile, onClear }) {
  const [dragging, setDragging] = useState(false)
  const inputRef = useRef(null)

  const parsePreview = (file) => {
    const reader = new FileReader()
    reader.onload = (e) => {
      const text = e.target.result
      const lines = text.split(/\r?\n/).filter((l) => l.trim())
      if (lines.length === 0) {
        onFile({ name: file.name, size: file.size, error: 'File is empty.' })
        return
      }

      const headers = lines[0].split(',').map((h) => h.trim().toLowerCase())
      const missing = CSV_EXPECTED_COLUMNS.filter((c) => !headers.includes(c))
      if (missing.length > 0) {
        onFile({
          name: file.name,
          size: file.size,
          error: `Missing required columns: ${missing.join(', ')}. Expected: ${CSV_EXPECTED_COLUMNS.join(', ')}.`,
        })
        return
      }

      const rows = lines.slice(1, 11).map((line) => {
        const vals = line.split(',').map((v) => v.trim())
        const row = {}
        headers.forEach((h, i) => { row[h] = vals[i] || '' })
        return row
      })

      onFile({
        name: file.name,
        size: file.size,
        headers,
        preview: rows,
        totalRows: lines.length - 1,
        error: null,
      })
    }
    reader.readAsText(file)
  }

  const handleFile = (file) => {
    const ext = file.name.split('.').pop().toLowerCase()
    if (ext !== 'csv') {
      onFile({ name: file.name, size: file.size, error: 'Only CSV files are accepted.' })
      return
    }
    parsePreview(file)
  }

  const handleDrop = (e) => {
    e.preventDefault()
    setDragging(false)
    if (e.dataTransfer.files?.[0]) handleFile(e.dataTransfer.files[0])
  }

  const handleDragOver = (e) => { e.preventDefault(); setDragging(true) }
  const handleDragLeave = () => setDragging(false)

  if (fileData?.name && !fileData.error) {
    return (
      <div className="space-y-3">
        <div className="flex items-center justify-between p-3 bg-green-50 border border-green-200 rounded-lg">
          <div className="flex items-center gap-2 text-sm text-green-800">
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
            </svg>
            <span className="font-medium">{fileData.name}</span>
            <span className="text-green-600">
              ({(fileData.size / 1024).toFixed(1)} KB — {fileData.totalRows} rows)
            </span>
          </div>
          <button onClick={onClear} className="text-green-600 hover:text-green-800 text-sm underline">
            Remove
          </button>
        </div>

        {fileData.preview?.length > 0 && (
          <div className="overflow-x-auto">
            <p className="text-xs text-gray-500 mb-1">
              Preview (first {fileData.preview.length} of {fileData.totalRows} rows)
            </p>
            <table className="w-full text-xs border border-gray-200 rounded-lg overflow-hidden">
              <thead>
                <tr className="bg-gray-50">
                  {fileData.headers.map((h) => (
                    <th key={h} className="px-3 py-2 text-left font-medium text-gray-600 border-b border-gray-200">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {fileData.preview.map((row, i) => (
                  <tr key={i} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                    {fileData.headers.map((h) => (
                      <td key={h} className="px-3 py-1.5 text-gray-700 border-b border-gray-100">
                        {row[h]}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    )
  }

  return (
    <div>
      <div
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onClick={() => inputRef.current?.click()}
        className={`border-2 border-dashed rounded-lg p-8 text-center cursor-pointer transition-colors
          ${dragging ? 'border-blue-400 bg-blue-50' : 'border-gray-300 hover:border-gray-400 bg-gray-50'}`}
      >
        <svg className="mx-auto w-8 h-8 text-gray-400 mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
            d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
        </svg>
        <p className="text-sm text-gray-600">
          Drag & drop or <span className="text-blue-600 font-medium">browse</span>
        </p>
        <p className="text-xs text-gray-400 mt-1">
          CSV with columns: {CSV_EXPECTED_COLUMNS.join(', ')}
        </p>
        <input
          ref={inputRef}
          type="file"
          accept=".csv"
          className="hidden"
          onChange={(e) => { if (e.target.files?.[0]) handleFile(e.target.files[0]) }}
        />
      </div>
      {fileData?.error && (
        <p className="mt-2 text-sm text-red-600">{fileData.error}</p>
      )}
    </div>
  )
}

// ── Manual rate entry for a single endpoint ────────────────────────

function EndpointRateRow({ crf, value, onChange }) {
  // Baseline-rate context — strip CRF-method suffixes like "MR-BRT" from
  // the source string. The full label (with MR-BRT) is still shown on
  // Step 5 where the CRF method is the relevant attribute.
  const baselineSource = crf.source
    ? crf.source.replace(/\s*MR-BRT\s*/gi, ' ').replace(/\s{2,}/g, ' ').trim()
    : ''
  return (
    <div className="flex items-start gap-4 p-3 rounded-lg border border-gray-200 bg-gray-50">
      <div className="flex-1 min-w-0">
        <p className="text-sm font-medium text-gray-900">{crf.endpoint}</p>
        <p className="text-xs text-gray-500 mt-0.5">
          Age range: {crf.ageRange} &middot; Source: {baselineSource}
        </p>
      </div>
      <div className="flex items-center gap-2 shrink-0">
        <input
          type="number"
          min="0"
          max="1"
          step="0.0001"
          value={value ?? ''}
          onChange={(e) => onChange(e.target.value)}
          placeholder={crf.defaultRate != null ? String(crf.defaultRate) : '0.000'}
          className="w-28 rounded-lg border border-gray-300 px-2 py-1.5 text-sm text-right
                     focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
        />
        <span className="text-xs text-gray-400 w-20">per person/yr</span>
      </div>
    </div>
  )
}

// ── Built-in incidence loader ─────────────────────────────────────

function BuiltinIncidenceLoader({ studyArea, year, uniqueEndpoints, onDataLoaded }) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [loadedCount, setLoadedCount] = useState(0)

  const country = studyArea?.id || ''

  useEffect(() => {
    if (!country || !year) return

    setLoading(true)
    setError(null)
    setLoadedCount(0)

    // Use the CRF library's `cause` field directly — it already matches
    // GBD cause codes ("ihd", "lung_cancer", "all_cause", …). Deriving
    // a slug from the endpoint label produced mismatches like
    // "ischemic-heart-disease" vs "ihd", so built-in lookups silently
    // failed (e.g., Mexico 2018).
    const causes = [...new Set(uniqueEndpoints.map((ep) => ep.cause).filter(Boolean))]

    Promise.all(
      causes.map((cause) =>
        fetchIncidence(country, cause, year)
          .catch(() => null)
          .then((r) => [cause, r]),
      ),
    )
      .then((entries) => {
        const byCause = Object.fromEntries(entries)
        if (Object.values(byCause).every((r) => r == null)) {
          setError(`No built-in incidence data for ${studyArea?.name || country} in ${year}.`)
          return
        }
        const ratesMap = {}
        let matched = 0
        for (const ep of uniqueEndpoints) {
          const units = byCause[ep.cause]?.units || []
          // Prefer all-age, both-sex row when present; else the first
          // row with a rate. GBD fallback returns one row per age_group.
          const chosen =
            units.find(
              (u) => u.incidence_rate != null
                && (u.age_group === 'all_ages' || u.age_group == null)
                && (u.sex == null || u.sex === 'both'),
            ) || units.find((u) => u.incidence_rate != null)
          if (chosen) {
            ratesMap[ep.id] = chosen.incidence_rate
            matched++
          }
        }
        setLoadedCount(matched)
        if (matched > 0) onDataLoaded(ratesMap)
        else setError(`No built-in incidence data matched endpoints for ${studyArea?.name || country} in ${year}.`)
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false))
  }, [country, year]) // eslint-disable-line react-hooks/exhaustive-deps

  if (!year) {
    return (
      <div className="p-3 bg-gray-50 border border-gray-200 rounded-lg text-sm text-gray-500">
        Set a year above to load incidence data.
      </div>
    )
  }

  return (
    <div className="space-y-3">
      {loading && (
        <div className="flex items-center gap-2 p-3 bg-blue-50 border border-blue-200 rounded-lg text-sm text-blue-700">
          <svg className="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
          Loading incidence data…
        </div>
      )}
      {error && (
        <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg text-sm text-amber-700">{error}</div>
      )}
      {loadedCount > 0 && !error && !loading && (
        <div className="p-3 bg-green-50 border border-green-200 rounded-lg text-sm text-green-700">
          <p className="font-medium">Incidence data loaded</p>
          <p className="text-xs text-green-600 mt-1">
            Pre-filled rates for {loadedCount} of {uniqueEndpoints.length} endpoints
          </p>
        </div>
      )}
    </div>
  )
}

// ── Main component ─────────────────────────────────────────────────

export default function Step4HealthData() {
  const { step1, step2, step4, setStep4, setStepValidity } = useAnalysisStore()
  const baselineYear = step2?.baseline?.year ?? null
  const effectiveYear = step4.year ?? baselineYear
  const { incidenceType, rates } = step4
  const pollutant = step1.pollutant

  const pollutantLabel = POLLUTANT_LABELS[pollutant] || 'Pollutant'

  // Filter CRF library to get unique endpoints for the selected pollutant
  const availableCRFs = useMemo(
    () => crfLibrary.filter((c) => c.pollutant === pollutant),
    [pollutant],
  )

  const uniqueEndpoints = useMemo(() => {
    const seen = new Set()
    return availableCRFs.filter((c) => {
      if (seen.has(c.endpoint)) return false
      seen.add(c.endpoint)
      return true
    })
  }, [availableCRFs])

  const selectedEndpoints = step4.selectedEndpoints || []
  const selectedEndpointsSet = useMemo(
    () => new Set(selectedEndpoints),
    [selectedEndpoints],
  )
  const visibleEndpoints = useMemo(
    () => uniqueEndpoints.filter((ep) => selectedEndpointsSet.has(ep.endpoint)),
    [uniqueEndpoints, selectedEndpointsSet],
  )

  // Local UI state
  const [activeTab, setActiveTab] = useState(() => {
    if (incidenceType === 'file' && step4.fileData?.name) return 'upload'
    if (incidenceType === 'manual' && rates && Object.values(rates).some((v) => v != null && v !== '' && v > 0)) return 'manual'
    return 'builtin'
  })

  // Rates come from the store; when a row has no entry yet, fall back to
  // the CRF's defaultRate so the input shows a sensible placeholder.
  const currentRates = useMemo(() => {
    if (rates && typeof rates === 'object' && !Array.isArray(rates)) return rates
    return {}
  }, [rates])

  // ── Built-in availability probe ───────────────────────────────
  // When the user chooses the "Built-in (GBD)" dataset for a
  // country+year, probe each pollutant-relevant cause to see which
  // endpoints have data. Drives the disabled state on endpoint
  // checkboxes below.
  const country = step1.studyArea?.id || ''
  const [builtinAvailability, setBuiltinAvailability] = useState({})
  const [availabilityLoading, setAvailabilityLoading] = useState(false)

  useEffect(() => {
    if (activeTab !== 'builtin') {
      setBuiltinAvailability({})
      return
    }
    if (!country || !effectiveYear || uniqueEndpoints.length === 0) {
      setBuiltinAvailability({})
      return
    }
    const causes = [...new Set(uniqueEndpoints.map((ep) => ep.cause).filter(Boolean))]
    setAvailabilityLoading(true)
    Promise.all(
      causes.map((cause) =>
        fetchIncidence(country, cause, effectiveYear)
          .catch(() => null)
          .then((r) => [cause, r != null && (r.units || []).some((u) => u.incidence_rate != null)]),
      ),
    )
      .then((entries) => setBuiltinAvailability(Object.fromEntries(entries)))
      .finally(() => setAvailabilityLoading(false))
  }, [activeTab, country, effectiveYear, uniqueEndpoints])

  const isUnavailable = useCallback(
    (ep) => activeTab === 'builtin' && builtinAvailability[ep.cause] === false,
    [activeTab, builtinAvailability],
  )

  // Clear stale selected endpoints when pollutant changes (different
  // pollutants expose different endpoint sets).
  useEffect(() => {
    if (!pollutant) return
    const validEndpointNames = new Set(uniqueEndpoints.map((ep) => ep.endpoint))
    const filtered = selectedEndpoints.filter((name) => validEndpointNames.has(name))
    if (filtered.length !== selectedEndpoints.length) {
      setStep4({ selectedEndpoints: filtered })
    }
  }, [pollutant]) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Validation ─────────────────────────────────────────────────

  useEffect(() => {
    if (!pollutant) {
      setStepValidity(4, false)
      return
    }
    if (visibleEndpoints.length === 0) {
      setStepValidity(4, false)
      return
    }
    const hasYear = effectiveYear != null
    let valid = false
    if (incidenceType === 'manual') {
      // Every selected endpoint needs a positive rate (either in
      // currentRates or via its defaultRate fallback).
      valid = visibleEndpoints.every((ep) => {
        const r = currentRates[ep.id] ?? ep.defaultRate
        return r != null && r !== '' && r > 0
      })
    } else if (incidenceType === 'file') {
      valid = step4.fileData?.name && !step4.fileData?.error && hasYear
    } else if (incidenceType === 'dataset') {
      valid = hasYear
    }
    setStepValidity(4, valid)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [incidenceType, currentRates, step4.fileData, effectiveYear, pollutant, visibleEndpoints])

  // ── Handlers ───────────────────────────────────────────────────

  const handleTabChange = useCallback((tab) => {
    setActiveTab(tab)
    const typeMap = { manual: 'manual', upload: 'file', builtin: 'dataset' }
    setStep4({ incidenceType: typeMap[tab] || 'manual' })
  }, [setStep4])

  const handleRateChange = useCallback((crfId, val) => {
    const num = val === '' ? null : Number(val)
    setStep4({ rates: { ...currentRates, [crfId]: num } })
  }, [currentRates, setStep4])

  const handlePrefillDefaults = useCallback(() => {
    const defaults = { ...currentRates }
    visibleEndpoints.forEach((crf) => {
      defaults[crf.id] = crf.defaultRate ?? null
    })
    setStep4({ rates: defaults })
  }, [visibleEndpoints, currentRates, setStep4])

  const handleClearRates = useCallback(() => {
    const cleared = { ...currentRates }
    visibleEndpoints.forEach((crf) => {
      cleared[crf.id] = null
    })
    setStep4({ rates: cleared })
  }, [visibleEndpoints, currentRates, setStep4])

  const handleToggleEndpoint = useCallback((endpointName) => {
    const next = selectedEndpointsSet.has(endpointName)
      ? selectedEndpoints.filter((n) => n !== endpointName)
      : [...selectedEndpoints, endpointName]
    setStep4({ selectedEndpoints: next })
  }, [selectedEndpoints, selectedEndpointsSet, setStep4])

  const handleSelectAllEndpoints = useCallback(() => {
    // When the Built-in dataset is active, "select all" only picks
    // endpoints that actually have data for the chosen country/year.
    const selectable = uniqueEndpoints.filter((ep) => !isUnavailable(ep))
    const selectableNames = selectable.map((ep) => ep.endpoint)
    const allSelected = selectableNames.length > 0 &&
      selectableNames.every((n) => selectedEndpointsSet.has(n))
    setStep4({ selectedEndpoints: allSelected ? [] : selectableNames })
  }, [uniqueEndpoints, selectedEndpointsSet, setStep4, isUnavailable])

  const handleFile = useCallback((fileData) => {
    setStep4({ fileData, incidenceType: 'file' })
  }, [setStep4])

  const handleClearFile = useCallback(() => {
    setStep4({ fileData: null })
  }, [setStep4])

  // ── Tab definitions ────────────────────────────────────────────

  const tabs = [
    { id: 'builtin', label: 'Built-in Data' },
    { id: 'manual', label: 'Manual Entry' },
    { id: 'upload', label: 'File Upload' },
  ]

  // ── Render ─────────────────────────────────────────────────────

  if (!pollutant) {
    return (
      <>
        <h1 className="text-2xl font-bold text-gray-900 mb-1">Health Data</h1>
        <div className="mt-6 p-6 bg-amber-50 border border-amber-200 rounded-xl text-center">
          <p className="text-amber-700">
            Please select a pollutant in Step 1 before configuring health data.
          </p>
        </div>
      </>
    )
  }

  return (
    <>
      <h1 className="text-2xl font-bold text-gray-900 mb-1">Health Data</h1>
      <p className="text-sm text-gray-500 mb-6">
        Provide baseline incidence rates for health endpoints associated with{' '}
        <span className="font-medium text-gray-700">{pollutantLabel}</span>.
        These rates represent the background rate of each health outcome in your study population.
      </p>

      <div className="space-y-6">
        {/* ── Incidence Dataset (year + dataset tabs) ───────────── */}
        <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
          <legend className="text-sm font-semibold text-gray-700 px-1">Incidence Dataset</legend>
          <p className="text-xs text-gray-500 mb-3">
            Choose the source of baseline incidence rates first. The endpoint
            list below is filtered to what the chosen dataset supports for
            your study area and year.
          </p>

          <div className="mb-4">
            <YearField
              id="step4-year"
              label="Year"
              value={effectiveYear}
              baselineYear={baselineYear}
              required
              onChange={(y) => setStep4({ year: y })}
            />
            {incidenceType === 'file' && (
              <p className="mt-1 text-xs text-gray-500">
                Applies to the uploaded rate file.
              </p>
            )}
          </div>

          <TabBar tabs={tabs} activeTab={activeTab} onTabChange={handleTabChange} />

          {activeTab === 'builtin' && (
            <p className="text-xs text-gray-500">
              Built-in rates are served from the bundled GBD 2023 reference
              dataset. Endpoints without data for {step1.studyArea?.name || country || 'the selected country'}
              {effectiveYear ? ` in ${effectiveYear}` : ''} are greyed out below.
              {availabilityLoading && <span className="ml-1 italic">Checking availability…</span>}
            </p>
          )}
          {activeTab === 'manual' && (
            <p className="text-xs text-gray-500">
              Enter the baseline incidence rate for each selected endpoint below.
            </p>
          )}
          {activeTab === 'upload' && (
            <p className="text-xs text-gray-500">
              Upload a CSV with one row per endpoint × age group.
            </p>
          )}
        </fieldset>

        {/* ── Endpoint selection (filtered by dataset) ──────────── */}
        <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
          <legend className="text-sm font-semibold text-gray-700 px-1">
            Health Endpoints
          </legend>
          <p className="text-xs text-gray-500 mb-3">
            Pick which outcomes you want to analyze for{' '}
            <span className="font-medium text-gray-700">{pollutantLabel}</span>.
            Only selected endpoints will appear as baseline-rate inputs below and as
            CRF options in Step 5.
          </p>

          {uniqueEndpoints.length === 0 ? (
            <p className="text-sm text-gray-400">
              No endpoints found for {pollutantLabel} in the CRF library.
            </p>
          ) : (
            <>
              <div className="flex items-center justify-between mb-3">
                <label className="flex items-center gap-2 text-xs text-gray-600 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={
                      uniqueEndpoints.length > 0 &&
                      uniqueEndpoints
                        .filter((ep) => !isUnavailable(ep))
                        .every((ep) => selectedEndpointsSet.has(ep.endpoint))
                    }
                    onChange={handleSelectAllEndpoints}
                    className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                  />
                  Select all available
                </label>
                <span className="text-xs text-gray-400">
                  {selectedEndpoints.length}/{uniqueEndpoints.length} selected
                </span>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                {uniqueEndpoints.map((ep) => {
                  const checked = selectedEndpointsSet.has(ep.endpoint)
                  const unavailable = isUnavailable(ep)
                  return (
                    <label
                      key={ep.endpoint}
                      aria-disabled={unavailable}
                      className={`flex items-start gap-2 p-2.5 rounded-lg border transition-colors
                        ${unavailable
                          ? 'border-gray-200 bg-gray-50 opacity-60 cursor-not-allowed'
                          : checked
                            ? 'border-blue-500 bg-blue-50 cursor-pointer'
                            : 'border-gray-200 hover:border-gray-300 hover:bg-gray-50 cursor-pointer'}`}
                    >
                      <input
                        type="checkbox"
                        checked={checked}
                        disabled={unavailable}
                        onChange={() => handleToggleEndpoint(ep.endpoint)}
                        className="mt-0.5 rounded border-gray-300 text-blue-600 focus:ring-blue-500 disabled:cursor-not-allowed"
                      />
                      <div className="min-w-0">
                        <p className="text-sm font-medium text-gray-900">{ep.endpoint}</p>
                        <p className="text-xs text-gray-500">
                          Age range: {ep.ageRange}
                          {unavailable && (
                            <span className="ml-2 text-amber-600">
                              · not in this dataset
                            </span>
                          )}
                        </p>
                      </div>
                    </label>
                  )
                })}
              </div>
            </>
          )}
        </fieldset>

        {/* ── Rates input (conditional on dataset) ──────────────── */}
        <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
          <legend className="text-sm font-semibold text-gray-700 px-1">Baseline Incidence Rates</legend>

          {/* Manual Entry */}
          {activeTab === 'manual' && (
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <p className="text-xs text-gray-500">
                  Enter the baseline incidence rate for each endpoint (per person per year).
                </p>
                <div className="flex gap-2">
                  <button
                    type="button"
                    onClick={handlePrefillDefaults}
                    className="px-2.5 py-1 text-xs rounded border border-gray-300 text-gray-600
                               hover:bg-gray-50 transition-colors"
                  >
                    Fill defaults
                  </button>
                  <button
                    type="button"
                    onClick={handleClearRates}
                    className="px-2.5 py-1 text-xs rounded border border-gray-300 text-gray-600
                               hover:bg-gray-50 transition-colors"
                  >
                    Clear all
                  </button>
                </div>
              </div>

              {visibleEndpoints.length === 0 ? (
                <p className="text-sm text-gray-400 text-center py-4">
                  Select at least one endpoint above to enter its baseline rate.
                </p>
              ) : (
                <div className="space-y-2">
                  {visibleEndpoints.map((crf) => (
                    <EndpointRateRow
                      key={crf.id}
                      crf={crf}
                      value={currentRates[crf.id] ?? crf.defaultRate}
                      onChange={(val) => handleRateChange(crf.id, val)}
                    />
                  ))}
                </div>
              )}
            </div>
          )}

          {/* File Upload */}
          {activeTab === 'upload' && (
            <CsvUpload
              fileData={step4.fileData}
              onFile={handleFile}
              onClear={handleClearFile}
            />
          )}

          {/* Built-in Data */}
          {activeTab === 'builtin' && (
            visibleEndpoints.length === 0 ? (
              <p className="text-sm text-gray-400 text-center py-4">
                Select at least one endpoint above to load incidence data.
              </p>
            ) : (
              <BuiltinIncidenceLoader
                studyArea={step1.studyArea}
                year={effectiveYear}
                uniqueEndpoints={visibleEndpoints}
                onDataLoaded={(ratesMap) => {
                  setStep4({ rates: { ...currentRates, ...ratesMap }, incidenceType: 'dataset' })
                }}
              />
            )
          )}
        </fieldset>
      </div>
    </>
  )
}
