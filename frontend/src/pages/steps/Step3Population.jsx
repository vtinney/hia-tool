import { useEffect, useState, useCallback, useMemo, useRef } from 'react'
import useAnalysisStore from '../../stores/useAnalysisStore'
import { uploadFile, fetchPopulation } from '../../lib/api'

// ── Constants ──────────────────────────────────────────────────────

const AGE_BINS = [
  '0–4', '5–9', '10–14', '15–19', '20–24', '25–29', '30–34',
  '35–39', '40–44', '45–49', '50–54', '55–59', '60–64',
  '65–69', '70–74', '75–79', '80+',
]

const PRESET_DISTRIBUTIONS = {
  us_national: {
    label: 'U.S. National',
    values: {
      '0–4': 5.9, '5–9': 6.1, '10–14': 6.3, '15–19': 6.3, '20–24': 6.5,
      '25–29': 7.0, '30–34': 6.8, '35–39': 6.5, '40–44': 6.1, '45–49': 6.0,
      '50–54': 6.2, '55–59': 6.5, '60–64': 6.3, '65–69': 5.5, '70–74': 4.5,
      '75–79': 3.2, '80+': 4.3,
    },
  },
  global_average: {
    label: 'Global Average',
    values: {
      '0–4': 8.7, '5–9': 8.5, '10–14': 8.2, '15–19': 8.0, '20–24': 7.8,
      '25–29': 7.4, '30–34': 7.0, '35–39': 6.5, '40–44': 5.9, '45–49': 5.4,
      '50–54': 4.8, '55–59': 4.2, '60–64': 3.6, '65–69': 3.0, '70–74': 2.3,
      '75–79': 1.6, '80+': 1.5,
    },
  },
}

const BUILTIN_DATASETS = [
  { id: 'census_acs', label: 'US Census ACS 5-Year Estimates (2015–2024)' },
]

const CSV_EXPECTED_COLUMNS = ['spatial_unit_id', 'age_group', 'population']

// ── Tab bar (matches Step 2 pattern) ───────────────────────────────

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

// ── CSV file upload with preview ───────────────────────────────────

function CsvUpload({ fileData, onFile, onClear }) {
  const [dragging, setDragging] = useState(false)
  const inputRef = useRef(null)

  const parsePreview = (file) => {
    const reader = new FileReader()
    reader.onload = (e) => {
      const text = e.target.result
      const lines = text.split(/\r?\n/).filter((l) => l.trim())
      if (lines.length === 0) {
        onFile({ name: file.name, size: file.size, error: 'File is empty.' }, null)
        return
      }

      const headers = lines[0].split(',').map((h) => h.trim().toLowerCase())
      const missing = CSV_EXPECTED_COLUMNS.filter((c) => !headers.includes(c))
      if (missing.length > 0) {
        onFile({
          name: file.name,
          size: file.size,
          error: `Missing required columns: ${missing.join(', ')}. Expected: ${CSV_EXPECTED_COLUMNS.join(', ')}.`,
        }, null)
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
      }, null)
    }
    reader.readAsText(file)
  }

  const handleFile = (file) => {
    const ext = file.name.split('.').pop().toLowerCase()
    if (!['csv', 'tif', 'tiff'].includes(ext)) {
      onFile({ name: file.name, size: file.size, error: 'Accepted: CSV or GeoTIFF (.tif, .tiff).' }, null)
      return
    }
    if (file.size > 500 * 1024 * 1024) {
      onFile({ name: file.name, size: file.size, error: 'File exceeds 500 MB limit.' }, null)
      return
    }
    if (ext === 'csv') {
      parsePreview(file)
    } else {
      // GeoTIFF — pass to parent for backend upload
      onFile({ name: file.name, size: file.size, type: ext, error: null }, file)
    }
  }

  const handleDrop = (e) => {
    e.preventDefault()
    setDragging(false)
    if (e.dataTransfer.files?.[0]) handleFile(e.dataTransfer.files[0])
  }

  const handleDragOver = (e) => { e.preventDefault(); setDragging(true) }
  const handleDragLeave = () => setDragging(false)

  // Show uploaded file with preview
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

        {/* Raster metadata */}
        {fileData.crs && (
          <div className="px-3 py-2 bg-gray-50 border border-gray-200 rounded-lg text-xs text-gray-600 space-y-0.5">
            <p><span className="font-medium">CRS:</span> {fileData.crs}</p>
            {fileData.metadata?.resolution && (
              <p><span className="font-medium">Resolution:</span> {fileData.metadata.resolution.map(r => r.toFixed(4)).join(' x ')}</p>
            )}
            {fileData.metadata?.width && (
              <p><span className="font-medium">Size:</span> {fileData.metadata.width} x {fileData.metadata.height} pixels</p>
            )}
          </div>
        )}

        {/* Preview table */}
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

  // Dropzone
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
          CSV with columns: {CSV_EXPECTED_COLUMNS.join(', ')} — or GeoTIFF population raster
        </p>
        <input
          ref={inputRef}
          type="file"
          accept=".csv,.tif,.tiff"
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

// ── Age distribution editor ────────────────────────────────────────

function AgeDistributionEditor({ ageGroups, onChange }) {
  const [presetId, setPresetId] = useState(null)

  const values = ageGroups || Object.fromEntries(AGE_BINS.map((b) => [b, 0]))
  const total = useMemo(
    () => Object.values(values).reduce((s, v) => s + (Number(v) || 0), 0),
    [values],
  )
  const isBalanced = Math.abs(total - 100) < 0.5

  const handlePreset = (id) => {
    setPresetId(id)
    if (PRESET_DISTRIBUTIONS[id]) {
      onChange({ ...PRESET_DISTRIBUTIONS[id].values })
    }
  }

  const handleBinChange = (bin, val) => {
    setPresetId(null)
    onChange({ ...values, [bin]: val === '' ? 0 : Number(val) })
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-3">
        <span className="text-sm text-gray-600">Preset:</span>
        {Object.entries(PRESET_DISTRIBUTIONS).map(([id, dist]) => (
          <button
            key={id}
            type="button"
            onClick={() => handlePreset(id)}
            className={`px-3 py-1 text-sm rounded-lg border transition-colors
              ${presetId === id
                ? 'border-blue-500 bg-blue-50 text-blue-700 font-medium'
                : 'border-gray-300 text-gray-700 hover:border-gray-400 hover:bg-gray-50'}`}
          >
            {dist.label}
          </button>
        ))}
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-x-4 gap-y-2">
        {AGE_BINS.map((bin) => (
          <div key={bin} className="flex items-center gap-2">
            <label className="text-xs text-gray-600 w-12 shrink-0">{bin}</label>
            <input
              type="number"
              min="0"
              max="100"
              step="0.1"
              value={values[bin] ?? 0}
              onChange={(e) => handleBinChange(bin, e.target.value)}
              className="w-full rounded border border-gray-300 px-2 py-1 text-xs text-right
                         focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
            />
            <span className="text-xs text-gray-400">%</span>
          </div>
        ))}
      </div>

      <div className={`text-xs font-medium ${isBalanced ? 'text-green-600' : 'text-amber-600'}`}>
        Total: {total.toFixed(1)}%
        {!isBalanced && ' — should sum to 100%'}
      </div>
    </div>
  )
}

// ── Crosswalk table ────────────────────────────────────────────────

function CrosswalkTable() {
  return (
    <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
      <legend className="text-sm font-semibold text-gray-700 px-1">
        Age-Bin Crosswalk
      </legend>
      <p className="text-xs text-gray-500 mb-3">
        Population 5-year bins will be mapped to concentration-response function (CRF) age ranges in Step 5.
        This table shows the standard bins used for alignment.
      </p>

      <div className="overflow-x-auto">
        <table className="w-full text-xs border border-gray-200 rounded-lg overflow-hidden">
          <thead>
            <tr className="bg-gray-50">
              <th className="px-3 py-2 text-left font-medium text-gray-600 border-b border-gray-200">
                Population Age Bin
              </th>
              <th className="px-3 py-2 text-left font-medium text-gray-600 border-b border-gray-200">
                CRF Age Range
              </th>
              <th className="px-3 py-2 text-left font-medium text-gray-600 border-b border-gray-200">
                Status
              </th>
            </tr>
          </thead>
          <tbody>
            {AGE_BINS.map((bin, i) => (
              <tr key={bin} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                <td className="px-3 py-1.5 text-gray-700 border-b border-gray-100 font-mono">
                  {bin}
                </td>
                <td className="px-3 py-1.5 text-gray-400 border-b border-gray-100 italic">
                  Configured in Step 5
                </td>
                <td className="px-3 py-1.5 border-b border-gray-100">
                  <span className="inline-block px-1.5 py-0.5 rounded text-[10px] font-medium bg-gray-100 text-gray-500">
                    Pending
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </fieldset>
  )
}

// ── Built-in population loader ────────────────────────────────────

function BuiltinPopulationLoader({ studyArea, years, selectedDatasetId, onSelect, onDataLoaded }) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [loadedData, setLoadedData] = useState(null)

  const country = studyArea?.id || studyArea?.name?.toLowerCase().replace(/\s+/g, '-') || ''
  const year = years?.start || years?.end || new Date().getFullYear()

  // Fetch population data when a dataset is selected
  useEffect(() => {
    if (!selectedDatasetId || !country) return

    setLoading(true)
    setError(null)
    setLoadedData(null)

    fetchPopulation(country, year)
      .then((data) => {
        if (!data) {
          setError(`Built-in data not yet available for ${studyArea?.name || country}. Please use manual entry or upload.`)
          return
        }
        setLoadedData(data)
        const units = data.units || []
        // Sum total population across units
        const total = units.reduce((s, u) => s + (u.total || 0), 0)

        // Aggregate age groups if available
        let ageGroups = null
        const firstWithAges = units.find((u) => u.age_groups)
        if (firstWithAges) {
          const ageTotals = {}
          for (const unit of units) {
            if (!unit.age_groups) continue
            for (const [key, val] of Object.entries(unit.age_groups)) {
              ageTotals[key] = (ageTotals[key] || 0) + (val || 0)
            }
          }
          // Convert absolute counts to percentages
          const totalPop = Object.values(ageTotals).reduce((s, v) => s + v, 0)
          if (totalPop > 0) {
            ageGroups = {}
            for (const [key, val] of Object.entries(ageTotals)) {
              // Convert age_0_4 format to 0–4 display format
              const label = key.replace(/^age_/, '').replace(/_/g, '–')
              ageGroups[label] = Math.round((val / totalPop) * 1000) / 10
            }
          }
        }

        onDataLoaded(total, ageGroups)
      })
      .catch((err) => {
        setError(err.message)
      })
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
          Loading population data…
        </div>
      )}

      {error && (
        <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg text-sm text-amber-700">
          {error}
        </div>
      )}

      {loadedData && !error && (
        <div className="p-3 bg-green-50 border border-green-200 rounded-lg text-sm text-green-700">
          <p className="font-medium">Population data loaded</p>
          <p className="text-xs text-green-600 mt-1">
            {loadedData.units?.length || 0} admin units — total{' '}
            {(loadedData.units || []).reduce((s, u) => s + (u.total || 0), 0).toLocaleString()} people
          </p>
        </div>
      )}
    </div>
  )
}

// ── Main component ─────────────────────────────────────────────────

export default function Step3Population() {
  const { step1, step3, setStep3, setStepValidity } = useAnalysisStore()
  const { populationType, totalPopulation, ageGroups } = step3

  const [activeTab, setActiveTab] = useState(
    populationType === 'file' ? 'upload'
      : populationType === 'dataset' ? 'builtin'
      : 'manual',
  )

  // ── Validation ─────────────────────────────────────────────────

  useEffect(() => {
    const valid =
      (populationType === 'manual' && totalPopulation != null && totalPopulation > 0) ||
      (populationType === 'file' && step3.fileData?.name && !step3.fileData?.error) ||
      (populationType === 'dataset' && step3.datasetId != null)
    setStepValidity(3, valid)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [populationType, totalPopulation, step3.fileData, step3.datasetId])

  // ── Handlers ───────────────────────────────────────────────────

  const handleTabChange = useCallback((tab) => {
    setActiveTab(tab)
    const typeMap = { manual: 'manual', upload: 'file', builtin: 'dataset' }
    setStep3({ populationType: typeMap[tab] || 'manual' })
  }, [setStep3])

  const handleTotalPopulation = useCallback((val) => {
    const num = val === '' ? null : Number(val)
    setStep3({ totalPopulation: num })
  }, [setStep3])

  const handleAgeGroups = useCallback((groups) => {
    setStep3({ ageGroups: groups })
  }, [setStep3])

  const handleFile = useCallback(async (fileData, rawFile) => {
    setStep3({ fileData, populationType: 'file', uploadId: null })
    // Upload raster files to backend
    if (rawFile && !fileData.error) {
      const ext = rawFile.name.split('.').pop().toLowerCase()
      if (['tif', 'tiff'].includes(ext)) {
        try {
          const result = await uploadFile(rawFile, 'population')
          setStep3({
            fileData: { ...fileData, crs: result.crs, metadata: result.metadata_json },
            populationType: 'file',
            uploadId: result.id,
          })
        } catch (err) {
          setStep3({
            fileData: { ...fileData, error: err.message },
            populationType: 'file',
            uploadId: null,
          })
        }
      }
    }
  }, [setStep3])

  const handleClearFile = useCallback(() => {
    setStep3({ fileData: null, uploadId: null })
  }, [setStep3])

  const handleDataset = useCallback((datasetId) => {
    setStep3({ datasetId: datasetId || null, populationType: 'dataset' })
  }, [setStep3])

  // ── Tab definitions ────────────────────────────────────────────

  const tabs = [
    { id: 'manual', label: 'Manual Entry' },
    { id: 'upload', label: 'File Upload' },
    { id: 'builtin', label: 'Built-in Data' },
  ]

  // ── Render ─────────────────────────────────────────────────────

  return (
    <>
      <h1 className="text-2xl font-bold text-gray-900 mb-1">Population Data</h1>
      <p className="text-sm text-gray-500 mb-6">
        Define the exposed population and its age distribution for the health impact calculation.
      </p>

      <div className="space-y-6">
        {/* ── Population Input ───────────────────────────────────── */}
        <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
          <legend className="text-sm font-semibold text-gray-700 px-1">Exposed Population</legend>

          <TabBar tabs={tabs} activeTab={activeTab} onTabChange={handleTabChange} />

          {/* Manual Entry */}
          {activeTab === 'manual' && (
            <div className="space-y-5">
              <div>
                <label htmlFor="total-pop" className="block text-sm text-gray-600 mb-1">
                  Total exposed population
                </label>
                <input
                  id="total-pop"
                  type="number"
                  min="0"
                  step="1"
                  value={totalPopulation ?? ''}
                  onChange={(e) => handleTotalPopulation(e.target.value)}
                  placeholder="e.g. 2700000"
                  className={`w-full max-w-xs rounded-lg border px-3 py-2 text-sm focus:ring-1
                    ${totalPopulation != null && totalPopulation <= 0
                      ? 'border-red-300 focus:border-red-500 focus:ring-red-500'
                      : 'border-gray-300 focus:border-blue-500 focus:ring-blue-500'}`}
                />
                {totalPopulation != null && totalPopulation <= 0 && (
                  <p className="text-xs text-red-600 mt-1">Population must be greater than zero.</p>
                )}
                {totalPopulation != null && totalPopulation > 0 && (
                  <p className="text-xs text-gray-400 mt-1">
                    {Number(totalPopulation).toLocaleString()} people
                  </p>
                )}
              </div>

              <div>
                <p className="text-sm font-medium text-gray-700 mb-2">Age Distribution (%)</p>
                <AgeDistributionEditor ageGroups={ageGroups} onChange={handleAgeGroups} />
              </div>
            </div>
          )}

          {/* File Upload */}
          {activeTab === 'upload' && (
            <CsvUpload
              fileData={step3.fileData}
              onFile={handleFile}
              onClear={handleClearFile}
            />
          )}

          {/* Built-in Data */}
          {activeTab === 'builtin' && (
            <BuiltinPopulationLoader
              studyArea={step1.studyArea}
              years={step1.years}
              selectedDatasetId={step3.datasetId}
              onSelect={handleDataset}
              onDataLoaded={(total, ageGroups) => {
                setStep3({
                  totalPopulation: total,
                  ageGroups: ageGroups || step3.ageGroups,
                  datasetId: step3.datasetId,
                  populationType: 'dataset',
                })
              }}
            />
          )}
        </fieldset>

        {/* ── Crosswalk Table ────────────────────────────────────── */}
        <CrosswalkTable />
      </div>
    </>
  )
}
