import { useEffect, useState, useCallback, useMemo, useRef } from 'react'
import useAnalysisStore from '../../stores/useAnalysisStore'

// ── Constants ──────────────────────────────────────────────────────

const UNIT_MAP = {
  pm25: 'μg/m³',
  no2: 'μg/m³',
  so2: 'μg/m³',
  ozone: 'ppb',
}

const POLLUTANT_LABELS = {
  pm25: 'PM2.5',
  ozone: 'Ozone',
  no2: 'NO₂',
  so2: 'SO₂',
}

const ACCEPTED_FILE_TYPES = '.csv,.nc,.tif,.tiff,.geotiff'
const ACCEPTED_EXTENSIONS = ['csv', 'nc', 'tif', 'tiff', 'geotiff']

const BUILTIN_DATASETS = [
  { id: 'gbd2019', label: 'GBD 2019 — Global PM2.5 Estimates' },
  { id: 'acag_v5', label: 'ACAG V5.GL.03 — Satellite-derived PM2.5' },
  { id: 'who_aap_2024', label: 'WHO Ambient Air Pollution Database 2024' },
  { id: 'epa_aqs', label: 'US EPA AQS Monitor Data' },
  { id: 'cams_reanalysis', label: 'CAMS Global Reanalysis (EAC4)' },
  { id: 'openaq', label: 'OpenAQ Aggregated Monitoring Data' },
]

const BENCHMARKS = [
  { id: 'who_guideline', label: 'WHO Guideline', value: 5 },
  { id: 'who_it1', label: 'WHO IT-1', value: 35 },
  { id: 'who_it2', label: 'WHO IT-2', value: 25 },
  { id: 'who_it3', label: 'WHO IT-3', value: 15 },
  { id: 'who_it4', label: 'WHO IT-4', value: 10 },
  { id: 'us_naaqs', label: 'US NAAQS', value: 9 },
  { id: 'eu_limit', label: 'EU Limit', value: 25 },
]

// ── Tabs component ─────────────────────────────────────────────────

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

// ── File dropzone ──────────────────────────────────────────────────

function FileDropzone({ fileData, onFile, onClear }) {
  const [dragging, setDragging] = useState(false)
  const inputRef = useRef(null)

  const validateFile = (file) => {
    const ext = file.name.split('.').pop().toLowerCase()
    if (!ACCEPTED_EXTENSIONS.includes(ext)) {
      return `Unsupported file type: .${ext}. Accepted: CSV, NetCDF, GeoTIFF.`
    }
    if (file.size > 500 * 1024 * 1024) {
      return 'File exceeds 500 MB limit.'
    }
    return null
  }

  const handleFile = (file) => {
    const error = validateFile(file)
    if (error) {
      onFile({ name: file.name, size: file.size, error })
    } else {
      onFile({ name: file.name, size: file.size, type: file.name.split('.').pop().toLowerCase(), error: null })
    }
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
      <div className="flex items-center justify-between p-3 bg-green-50 border border-green-200 rounded-lg">
        <div className="flex items-center gap-2 text-sm text-green-800">
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
          </svg>
          <span className="font-medium">{fileData.name}</span>
          <span className="text-green-600">({(fileData.size / 1024).toFixed(1)} KB)</span>
        </div>
        <button onClick={onClear} className="text-green-600 hover:text-green-800 text-sm underline">
          Remove
        </button>
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
        <p className="text-xs text-gray-400 mt-1">CSV, NetCDF (.nc), or GeoTIFF</p>
        <input
          ref={inputRef}
          type="file"
          accept={ACCEPTED_FILE_TYPES}
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

// ── Delta preview ──────────────────────────────────────────────────

function DeltaPreview({ baseline, control, unit }) {
  if (baseline == null || control == null) return null

  const delta = baseline - control
  const maxVal = Math.max(baseline, 1)
  const controlPct = Math.max(0, (control / maxVal) * 100)
  const deltaPct = Math.max(0, Math.min(100, (Math.abs(delta) / maxVal) * 100))

  const isReduction = delta > 0
  const barColor = isReduction ? 'bg-green-500' : delta < 0 ? 'bg-red-500' : 'bg-gray-400'
  const textColor = isReduction ? 'text-green-700' : delta < 0 ? 'text-red-700' : 'text-gray-600'

  return (
    <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
      <legend className="text-sm font-semibold text-gray-700 px-1">Delta Preview</legend>

      <div className="space-y-3">
        <div className="grid grid-cols-3 gap-4 text-sm">
          <div>
            <p className="text-gray-500">Baseline</p>
            <p className="font-semibold text-gray-900">{baseline} {unit}</p>
          </div>
          <div>
            <p className="text-gray-500">Control</p>
            <p className="font-semibold text-gray-900">{control} {unit}</p>
          </div>
          <div>
            <p className="text-gray-500">Delta (Δ)</p>
            <p className={`font-semibold ${textColor}`}>
              {delta > 0 ? '−' : delta < 0 ? '+' : ''}{Math.abs(delta).toFixed(2)} {unit}
            </p>
          </div>
        </div>

        <div className="space-y-1.5">
          <div className="flex items-center gap-2 text-xs text-gray-500">
            <span className="w-16">Baseline</span>
            <div className="flex-1 h-4 bg-gray-100 rounded-full overflow-hidden">
              <div className="h-full bg-blue-400 rounded-full" style={{ width: '100%' }} />
            </div>
          </div>
          <div className="flex items-center gap-2 text-xs text-gray-500">
            <span className="w-16">Control</span>
            <div className="flex-1 h-4 bg-gray-100 rounded-full overflow-hidden">
              <div className="h-full bg-blue-300 rounded-full" style={{ width: `${controlPct}%` }} />
            </div>
          </div>
          <div className="flex items-center gap-2 text-xs text-gray-500">
            <span className="w-16">Delta</span>
            <div className="flex-1 h-4 bg-gray-100 rounded-full overflow-hidden">
              <div className={`h-full ${barColor} rounded-full`} style={{ width: `${deltaPct}%` }} />
            </div>
          </div>
        </div>
      </div>
    </fieldset>
  )
}

// ── Main component ─────────────────────────────────────────────────

export default function Step2AirQuality() {
  const { step1, step2, setStep2, setStepValidity } = useAnalysisStore()
  const { baseline, control } = step2
  const pollutant = step1.pollutant

  const unit = UNIT_MAP[pollutant] || 'μg/m³'
  const pollutantLabel = POLLUTANT_LABELS[pollutant] || 'Pollutant'

  // Local UI state
  const [baselineTab, setBaselineTab] = useState(baseline.type || 'manual')
  const [controlOpen, setControlOpen] = useState(control.type !== 'none')
  const [controlTab, setControlTab] = useState(
    control.type === 'none' ? 'manual'
      : control.rollbackPercent != null ? 'rollback'
      : control.benchmarkId != null ? 'benchmark'
      : control.type || 'manual',
  )

  // ── Validation ─────────────────────────────────────────────────

  useEffect(() => {
    const hasBaseline =
      (baseline.type === 'manual' && baseline.value != null && baseline.value !== '') ||
      (baseline.type === 'dataset' && baseline.datasetId != null) ||
      (baseline.type === 'file' && baseline.fileData?.name && !baseline.fileData?.error)
    setStepValidity(2, hasBaseline)
  }, [baseline, setStepValidity])

  // ── Baseline handlers ──────────────────────────────────────────

  const handleBaselineTabChange = useCallback((tab) => {
    setBaselineTab(tab)
    const typeMap = { manual: 'manual', upload: 'file', builtin: 'dataset' }
    setStep2({
      baseline: { ...baseline, type: typeMap[tab] || 'manual' },
    })
  }, [baseline, setStep2])

  const handleBaselineValue = useCallback((val) => {
    const num = val === '' ? null : Number(val)
    setStep2({
      baseline: { ...baseline, value: num, type: 'manual' },
    })
  }, [baseline, setStep2])

  const handleBaselineFile = useCallback((fileData) => {
    setStep2({
      baseline: { ...baseline, fileData, type: 'file' },
    })
  }, [baseline, setStep2])

  const handleClearBaselineFile = useCallback(() => {
    setStep2({
      baseline: { ...baseline, fileData: null },
    })
  }, [baseline, setStep2])

  const handleBaselineDataset = useCallback((datasetId) => {
    setStep2({
      baseline: { ...baseline, datasetId: datasetId || null, type: 'dataset' },
    })
  }, [baseline, setStep2])

  // ── Control handlers ───────────────────────────────────────────

  const handleControlTabChange = useCallback((tab) => {
    setControlTab(tab)
    if (tab === 'rollback') {
      setStep2({
        control: { ...control, type: 'rollback', benchmarkId: null },
      })
    } else if (tab === 'benchmark') {
      setStep2({
        control: { ...control, type: 'benchmark', rollbackPercent: null },
      })
    } else {
      setStep2({
        control: { ...control, type: tab === 'upload' ? 'file' : tab === 'builtin' ? 'dataset' : 'manual' },
      })
    }
  }, [control, setStep2])

  const handleControlValue = useCallback((val) => {
    const num = val === '' ? null : Number(val)
    setStep2({
      control: { ...control, value: num, type: 'manual' },
    })
  }, [control, setStep2])

  const handleControlFile = useCallback((fileData) => {
    setStep2({
      control: { ...control, fileData, type: 'file' },
    })
  }, [control, setStep2])

  const handleClearControlFile = useCallback(() => {
    setStep2({
      control: { ...control, fileData: null },
    })
  }, [control, setStep2])

  const handleControlDataset = useCallback((datasetId) => {
    setStep2({
      control: { ...control, datasetId: datasetId || null, type: 'dataset' },
    })
  }, [control, setStep2])

  const handleRollback = useCallback((pct) => {
    const rollbackPercent = Number(pct)
    const baseVal = baseline.type === 'manual' ? baseline.value : null
    const controlValue = baseVal != null ? +(baseVal * (1 - rollbackPercent / 100)).toFixed(4) : null
    setStep2({
      control: {
        ...control,
        type: 'rollback',
        rollbackPercent,
        value: controlValue,
        benchmarkId: null,
      },
    })
  }, [baseline, control, setStep2])

  const handleBenchmark = useCallback((benchmark) => {
    setStep2({
      control: {
        ...control,
        type: 'benchmark',
        benchmarkId: benchmark.id,
        value: benchmark.value,
        rollbackPercent: null,
      },
    })
  }, [control, setStep2])

  const toggleControl = useCallback(() => {
    if (controlOpen) {
      setStep2({
        control: { type: 'none', value: null, benchmarkId: null, rollbackPercent: null },
      })
    }
    setControlOpen(!controlOpen)
  }, [controlOpen, setStep2])

  // ── Derived values ─────────────────────────────────────────────

  const baselineNumeric = baseline.type === 'manual' ? baseline.value : null
  const controlNumeric = control.value
  const showDelta = baselineNumeric != null && controlNumeric != null

  const rollbackDisplay = useMemo(() => {
    if (control.rollbackPercent == null || baselineNumeric == null) return null
    return (baselineNumeric * (1 - control.rollbackPercent / 100)).toFixed(2)
  }, [control.rollbackPercent, baselineNumeric])

  // ── Tab definitions ────────────────────────────────────────────

  const baselineTabs = [
    { id: 'manual', label: 'Manual Entry' },
    { id: 'upload', label: 'File Upload' },
    { id: 'builtin', label: 'Built-in Data' },
  ]

  const controlTabs = [
    { id: 'manual', label: 'Manual Entry' },
    { id: 'upload', label: 'File Upload' },
    { id: 'builtin', label: 'Built-in Data' },
    { id: 'rollback', label: 'Rollback' },
    { id: 'benchmark', label: 'Benchmark' },
  ]

  // ── Render ─────────────────────────────────────────────────────

  return (
    <>
      <h1 className="text-2xl font-bold text-gray-900 mb-1">Air Quality Data</h1>
      <p className="text-sm text-gray-500 mb-6">
        Specify baseline{controlOpen ? ' and control scenario' : ''} concentrations for{' '}
        <span className="font-medium text-gray-700">{pollutantLabel}</span>.
        {!pollutant && (
          <span className="text-amber-600 ml-1">(No pollutant selected — go back to Step 1)</span>
        )}
      </p>

      <div className="space-y-6">
        {/* ── Baseline Scenario ──────────────────────────────────── */}
        <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
          <legend className="text-sm font-semibold text-gray-700 px-1">Baseline Scenario</legend>

          <TabBar tabs={baselineTabs} activeTab={baselineTab} onTabChange={handleBaselineTabChange} />

          {/* Manual Entry */}
          {baselineTab === 'manual' && (
            <div>
              <label htmlFor="baseline-value" className="block text-sm text-gray-600 mb-1">
                Concentration
              </label>
              <div className="flex items-center gap-2">
                <input
                  id="baseline-value"
                  type="number"
                  min="0"
                  step="any"
                  value={baseline.value ?? ''}
                  onChange={(e) => handleBaselineValue(e.target.value)}
                  placeholder="e.g. 12.5"
                  className="flex-1 rounded-lg border border-gray-300 px-3 py-2 text-sm
                             focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
                />
                <span className="text-sm text-gray-500 whitespace-nowrap">{unit}</span>
              </div>
            </div>
          )}

          {/* File Upload */}
          {baselineTab === 'upload' && (
            <FileDropzone
              fileData={baseline.fileData}
              onFile={handleBaselineFile}
              onClear={handleClearBaselineFile}
            />
          )}

          {/* Built-in Data */}
          {baselineTab === 'builtin' && (
            <div>
              <select
                value={baseline.datasetId || ''}
                onChange={(e) => handleBaselineDataset(e.target.value)}
                className="w-full rounded-lg border border-gray-300 px-3 py-2.5 text-sm
                           focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              >
                <option value="">Select a dataset…</option>
                {BUILTIN_DATASETS.map((d) => (
                  <option key={d.id} value={d.id}>{d.label}</option>
                ))}
              </select>
              {baseline.datasetId && (
                <div className="mt-3 p-3 bg-amber-50 border border-amber-200 rounded-lg text-sm text-amber-700">
                  Data loading not yet implemented. This dataset will be available in a future release.
                </div>
              )}
            </div>
          )}
        </fieldset>

        {/* ── Control Scenario (collapsible) ─────────────────────── */}
        <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200">
          <button
            type="button"
            onClick={toggleControl}
            className="w-full flex items-center justify-between p-5 text-left"
          >
            <legend className="text-sm font-semibold text-gray-700">
              Control Scenario
              <span className="ml-2 text-xs font-normal text-gray-400">(optional)</span>
            </legend>
            <svg
              className={`w-5 h-5 text-gray-400 transition-transform ${controlOpen ? 'rotate-180' : ''}`}
              fill="none" stroke="currentColor" viewBox="0 0 24 24"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </button>

          {controlOpen && (
            <div className="px-5 pb-5 -mt-2">
              <TabBar tabs={controlTabs} activeTab={controlTab} onTabChange={handleControlTabChange} />

              {/* Manual Entry */}
              {controlTab === 'manual' && (
                <div>
                  <label htmlFor="control-value" className="block text-sm text-gray-600 mb-1">
                    Concentration
                  </label>
                  <div className="flex items-center gap-2">
                    <input
                      id="control-value"
                      type="number"
                      min="0"
                      step="any"
                      value={control.value ?? ''}
                      onChange={(e) => handleControlValue(e.target.value)}
                      placeholder="e.g. 8.0"
                      className="flex-1 rounded-lg border border-gray-300 px-3 py-2 text-sm
                                 focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
                    />
                    <span className="text-sm text-gray-500 whitespace-nowrap">{unit}</span>
                  </div>
                </div>
              )}

              {/* File Upload */}
              {controlTab === 'upload' && (
                <FileDropzone
                  fileData={control.fileData}
                  onFile={handleControlFile}
                  onClear={handleClearControlFile}
                />
              )}

              {/* Built-in Data */}
              {controlTab === 'builtin' && (
                <div>
                  <select
                    value={control.datasetId || ''}
                    onChange={(e) => handleControlDataset(e.target.value)}
                    className="w-full rounded-lg border border-gray-300 px-3 py-2.5 text-sm
                               focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
                  >
                    <option value="">Select a dataset…</option>
                    {BUILTIN_DATASETS.map((d) => (
                      <option key={d.id} value={d.id}>{d.label}</option>
                    ))}
                  </select>
                  {control.datasetId && (
                    <div className="mt-3 p-3 bg-amber-50 border border-amber-200 rounded-lg text-sm text-amber-700">
                      Data loading not yet implemented. This dataset will be available in a future release.
                    </div>
                  )}
                </div>
              )}

              {/* Rollback */}
              {controlTab === 'rollback' && (
                <div>
                  <label htmlFor="rollback-slider" className="block text-sm text-gray-600 mb-2">
                    Reduction: <span className="font-semibold text-gray-900">{control.rollbackPercent ?? 0}%</span>
                  </label>
                  <input
                    id="rollback-slider"
                    type="range"
                    min="0"
                    max="100"
                    step="1"
                    value={control.rollbackPercent ?? 0}
                    onChange={(e) => handleRollback(e.target.value)}
                    className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer accent-blue-600"
                  />
                  <div className="flex justify-between text-xs text-gray-400 mt-1">
                    <span>0%</span>
                    <span>50%</span>
                    <span>100%</span>
                  </div>
                  {baselineNumeric != null && control.rollbackPercent != null && (
                    <p className="mt-3 text-sm text-gray-600">
                      Resulting control concentration:{' '}
                      <span className="font-semibold text-gray-900">{rollbackDisplay} {unit}</span>
                    </p>
                  )}
                  {baselineNumeric == null && (
                    <p className="mt-3 text-sm text-amber-600">
                      Enter a baseline manual value to see the resulting concentration.
                    </p>
                  )}
                </div>
              )}

              {/* Benchmark */}
              {controlTab === 'benchmark' && (
                <div>
                  <p className="text-sm text-gray-600 mb-3">Select a standard to set as the control value:</p>
                  <div className="flex flex-wrap gap-2">
                    {BENCHMARKS.map((b) => (
                      <button
                        key={b.id}
                        type="button"
                        onClick={() => handleBenchmark(b)}
                        className={`px-3 py-1.5 text-sm rounded-lg border transition-colors
                          ${control.benchmarkId === b.id
                            ? 'border-blue-500 bg-blue-50 text-blue-700 font-medium'
                            : 'border-gray-300 text-gray-700 hover:border-gray-400 hover:bg-gray-50'}`}
                      >
                        {b.label} ({b.value} {unit})
                      </button>
                    ))}
                  </div>
                  {control.benchmarkId && (
                    <p className="mt-3 text-sm text-gray-600">
                      Control set to:{' '}
                      <span className="font-semibold text-gray-900">
                        {control.value} {unit}
                      </span>
                    </p>
                  )}
                </div>
              )}
            </div>
          )}
        </fieldset>

        {/* ── Delta Preview ──────────────────────────────────────── */}
        {showDelta && <DeltaPreview baseline={baselineNumeric} control={controlNumeric} unit={unit} />}
      </div>
    </>
  )
}
