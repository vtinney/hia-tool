import { useEffect, useState, useCallback, useMemo, useRef } from 'react'
import useAnalysisStore from '../../stores/useAnalysisStore'
import { fetchIncidence } from '../../lib/api'
import crfLibrary from '../../data/crf-library.json'

// ── Constants ──────────────────────────────────────────────────────

const POLLUTANT_LABELS = {
  pm25: 'PM2.5',
  ozone: 'Ozone',
  no2: 'NO₂',
  so2: 'SO₂',
}

const CSV_EXPECTED_COLUMNS = ['endpoint', 'age_group', 'rate']

const BUILTIN_DATASETS = [
  { id: 'gbd2019_rates', label: 'GBD 2019 — Baseline Incidence Rates' },
  { id: 'benmap_rates', label: 'BenMAP-CE Default Health Incidence Rates' },
  { id: 'who_ghe_2020', label: 'WHO Global Health Estimates 2020' },
  { id: 'cdc_wonder', label: 'CDC WONDER Mortality Data (U.S.)' },
]

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
  return (
    <div className="flex items-start gap-4 p-3 rounded-lg border border-gray-200 bg-gray-50">
      <div className="flex-1 min-w-0">
        <p className="text-sm font-medium text-gray-900">{crf.endpoint}</p>
        <p className="text-xs text-gray-500 mt-0.5">
          Age range: {crf.ageRange} &middot; Source: {crf.source}
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

function BuiltinIncidenceLoader({ studyArea, years, uniqueEndpoints, selectedDatasetId, onSelect, onDataLoaded }) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [loadedCount, setLoadedCount] = useState(0)

  const country = studyArea?.id || studyArea?.name?.toLowerCase().replace(/\s+/g, '-') || ''
  const year = years?.start || years?.end || new Date().getFullYear()

  // Fetch incidence data when a dataset is selected
  useEffect(() => {
    if (!selectedDatasetId || !country) return

    setLoading(true)
    setError(null)
    setLoadedCount(0)

    // Try fetching incidence for each unique endpoint's cause
    const causes = [...new Set(uniqueEndpoints.filter((ep) => ep.endpoint).map((ep) =>
      ep.endpoint.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, ''),
    ))]

    // Try a general "all" cause first, then individual causes
    const causesToTry = ['all', ...causes]

    Promise.all(
      causesToTry.map((cause) =>
        fetchIncidence(country, cause, year).catch(() => null),
      ),
    )
      .then((results) => {
        // Merge all non-null results
        const allUnits = results.filter(Boolean).flatMap((r) => r.units || [])

        if (allUnits.length === 0) {
          setError(`Built-in data not yet available for ${studyArea?.name || country}. Please use manual entry or upload.`)
          return
        }

        // Map incidence rates to CRF endpoint IDs
        const ratesMap = {}
        let matched = 0
        for (const ep of uniqueEndpoints) {
          const epLower = (ep.endpoint || '').toLowerCase()
          // Find a matching unit by endpoint/cause name
          const match = allUnits.find((u) => {
            const cause = (u.cause || '').toLowerCase()
            return epLower.includes(cause) || cause.includes(epLower.split(' ')[0])
          })
          if (match && match.incidence_rate != null) {
            ratesMap[ep.id] = match.incidence_rate
            matched++
          }
        }

        setLoadedCount(matched)
        if (matched > 0) {
          onDataLoaded(ratesMap)
        } else {
          setError(`Built-in data not yet available for ${studyArea?.name || country}. Please use manual entry or upload.`)
        }
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false))
  }, [selectedDatasetId, country, year]) // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="space-y-3">
      <select
        value={selectedDatasetId || ''}
        onChange={(e) => onSelect(e.target.value)}
        className="w-full rounded-lg border border-gray-300 px-3 py-2.5 text-sm
                   focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
      >
        <option value="">Select a dataset…</option>
        {BUILTIN_DATASETS.map((d) => (
          <option key={d.id} value={d.id}>{d.label}</option>
        ))}
      </select>

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
        <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg text-sm text-amber-700">
          {error}
        </div>
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
  const { step1, step4, setStep4, setStepValidity } = useAnalysisStore()
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

  // Local UI state
  const [activeTab, setActiveTab] = useState(
    incidenceType === 'file' ? 'upload'
      : incidenceType === 'dataset' ? 'builtin'
      : 'manual',
  )

  // Initialize rates from defaults when pollutant changes
  const currentRates = useMemo(() => {
    if (rates && typeof rates === 'object' && !Array.isArray(rates)) return rates
    const defaults = {}
    uniqueEndpoints.forEach((crf) => {
      defaults[crf.id] = crf.defaultRate ?? null
    })
    return defaults
  }, [rates, uniqueEndpoints])

  // Sync default rates into store on pollutant change
  useEffect(() => {
    if (incidenceType === 'manual' && (!rates || typeof rates !== 'object')) {
      const defaults = {}
      uniqueEndpoints.forEach((crf) => {
        defaults[crf.id] = crf.defaultRate ?? null
      })
      setStep4({ rates: defaults })
    }
  }, [pollutant]) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Validation ─────────────────────────────────────────────────

  useEffect(() => {
    if (!pollutant) {
      setStepValidity(4, false)
      return
    }

    let valid = false
    if (incidenceType === 'manual') {
      // At least one rate must be filled
      valid = currentRates && Object.values(currentRates).some((v) => v != null && v !== '' && v > 0)
    } else if (incidenceType === 'file') {
      valid = step4.fileData?.name && !step4.fileData?.error
    } else if (incidenceType === 'dataset') {
      valid = step4.datasetId != null
    }
    setStepValidity(4, valid)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [incidenceType, currentRates, step4.fileData, step4.datasetId, pollutant])

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
    const defaults = {}
    uniqueEndpoints.forEach((crf) => {
      defaults[crf.id] = crf.defaultRate ?? null
    })
    setStep4({ rates: defaults })
  }, [uniqueEndpoints, setStep4])

  const handleClearRates = useCallback(() => {
    const cleared = {}
    uniqueEndpoints.forEach((crf) => {
      cleared[crf.id] = null
    })
    setStep4({ rates: cleared })
  }, [uniqueEndpoints, setStep4])

  const handleFile = useCallback((fileData) => {
    setStep4({ fileData, incidenceType: 'file' })
  }, [setStep4])

  const handleClearFile = useCallback(() => {
    setStep4({ fileData: null })
  }, [setStep4])

  const handleDataset = useCallback((datasetId) => {
    setStep4({ datasetId: datasetId || null, incidenceType: 'dataset' })
  }, [setStep4])

  // ── Tab definitions ────────────────────────────────────────────

  const tabs = [
    { id: 'manual', label: 'Manual Entry' },
    { id: 'upload', label: 'File Upload' },
    { id: 'builtin', label: 'Built-in Data' },
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
        {/* ── Endpoint Summary ───────────────────────────────────── */}
        <div className="flex items-center gap-3 p-3 bg-blue-50 border border-blue-200 rounded-lg">
          <svg className="w-5 h-5 text-blue-500 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <p className="text-sm text-blue-700">
            <span className="font-medium">{uniqueEndpoints.length} health endpoint{uniqueEndpoints.length !== 1 ? 's' : ''}</span>{' '}
            available for {pollutantLabel} from the CRF library. Rates entered here will be paired
            with concentration-response functions in Step 5.
          </p>
        </div>

        {/* ── Incidence Rate Input ───────────────────────────────── */}
        <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
          <legend className="text-sm font-semibold text-gray-700 px-1">Baseline Incidence Rates</legend>

          <TabBar tabs={tabs} activeTab={activeTab} onTabChange={handleTabChange} />

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

              <div className="space-y-2">
                {uniqueEndpoints.map((crf) => (
                  <EndpointRateRow
                    key={crf.id}
                    crf={crf}
                    value={currentRates[crf.id]}
                    onChange={(val) => handleRateChange(crf.id, val)}
                  />
                ))}
              </div>

              {uniqueEndpoints.length === 0 && (
                <p className="text-sm text-gray-400 text-center py-4">
                  No endpoints found for {pollutantLabel} in the CRF library.
                </p>
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
            <BuiltinIncidenceLoader
              studyArea={step1.studyArea}
              years={step1.years}
              uniqueEndpoints={uniqueEndpoints}
              selectedDatasetId={step4.datasetId}
              onSelect={handleDataset}
              onDataLoaded={(ratesMap) => {
                setStep4({ rates: { ...currentRates, ...ratesMap }, incidenceType: 'dataset' })
              }}
            />
          )}
        </fieldset>

        {/* ── Available Endpoints Reference ──────────────────────── */}
        <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
          <legend className="text-sm font-semibold text-gray-700 px-1">
            CRF Endpoint Reference — {pollutantLabel}
          </legend>
          <p className="text-xs text-gray-500 mb-3">
            Health endpoints from the CRF library for the selected pollutant.
            Full CRF configuration (beta coefficients, pooling) happens in Step 5.
          </p>

          <div className="overflow-x-auto">
            <table className="w-full text-xs border border-gray-200 rounded-lg overflow-hidden">
              <thead>
                <tr className="bg-gray-50">
                  <th className="px-3 py-2 text-left font-medium text-gray-600 border-b border-gray-200">Endpoint</th>
                  <th className="px-3 py-2 text-left font-medium text-gray-600 border-b border-gray-200">Age Range</th>
                  <th className="px-3 py-2 text-left font-medium text-gray-600 border-b border-gray-200">Source</th>
                  <th className="px-3 py-2 text-right font-medium text-gray-600 border-b border-gray-200">Default Rate</th>
                </tr>
              </thead>
              <tbody>
                {availableCRFs.map((crf, i) => (
                  <tr key={crf.id} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                    <td className="px-3 py-1.5 text-gray-900 border-b border-gray-100 font-medium">
                      {crf.endpoint}
                    </td>
                    <td className="px-3 py-1.5 text-gray-600 border-b border-gray-100 font-mono">
                      {crf.ageRange}
                    </td>
                    <td className="px-3 py-1.5 text-gray-500 border-b border-gray-100">
                      {crf.source}
                    </td>
                    <td className="px-3 py-1.5 text-gray-700 border-b border-gray-100 text-right font-mono">
                      {crf.defaultRate != null ? crf.defaultRate.toFixed(4) : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </fieldset>
      </div>
    </>
  )
}
