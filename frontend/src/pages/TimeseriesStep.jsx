import { useEffect, useState, useCallback, useMemo, useRef } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import useTimeseriesStore from '../stores/useTimeseriesStore'
import { computeTimeSeriesHIA } from '../lib/hia-engine'
import crfLibrary from '../data/crf-library.json'

// ── Constants ──────────────────────────────────────────────────────

const POLLUTANT_LABELS = { pm25: 'PM2.5', ozone: 'Ozone', no2: 'NO₂', so2: 'SO₂' }

const STEP_TITLES = {
  1: 'Study Area & Pollutant',
  2: 'Daily Air Quality Data',
  3: 'Population',
  4: 'Health Data',
  5: 'Short-Term CRFs',
  6: 'Analysis Options',
  7: 'Review & Run',
}

// ── CSV parser ─────────────────────────────────────────────────────

function parseCsvTimeSeries(text) {
  const lines = text.split(/\r?\n/).filter((l) => l.trim())
  if (lines.length < 2) return { error: 'File is empty or has no data rows.' }

  const headers = lines[0].split(',').map((h) => h.trim().toLowerCase())
  const dateIdx = headers.findIndex((h) => h === 'date')
  const concIdx = headers.findIndex((h) => ['concentration', 'conc', 'value', 'pm25', 'ozone'].includes(h))

  if (dateIdx === -1 || concIdx === -1) {
    return { error: 'CSV must have "date" and "concentration" columns.' }
  }

  const data = []
  for (let i = 1; i < lines.length; i++) {
    const vals = lines[i].split(',').map((v) => v.trim())
    const date = vals[dateIdx]
    const concentration = parseFloat(vals[concIdx])
    if (date && !isNaN(concentration)) {
      data.push({ date, concentration })
    }
  }

  if (data.length === 0) return { error: 'No valid data rows found.' }
  return { data }
}

// ── Step 1: Study area ─────────────────────────────────────────────

function Step1({ store }) {
  const { step1, setStep1, setStepValidity } = store

  useEffect(() => {
    setStepValidity(1, Boolean(step1.pollutant))
  }, [step1.pollutant, setStepValidity])

  return (
    <div className="space-y-6">
      <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
        <legend className="text-sm font-semibold text-gray-700 px-1">Analysis Name</legend>
        <input
          type="text"
          value={step1.analysisName || ''}
          onChange={(e) => setStep1({ analysisName: e.target.value })}
          placeholder="e.g. Delhi PM2.5 Short-Term 2023"
          className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
        />
      </fieldset>

      <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
        <legend className="text-sm font-semibold text-gray-700 px-1">Pollutant</legend>
        <div className="flex flex-wrap gap-3">
          {Object.entries(POLLUTANT_LABELS).map(([key, label]) => (
            <button
              key={key}
              onClick={() => setStep1({ pollutant: key })}
              className={`px-4 py-2 rounded-lg border text-sm font-medium transition-colors
                ${step1.pollutant === key
                  ? 'border-blue-500 bg-blue-50 text-blue-700'
                  : 'border-gray-300 text-gray-700 hover:border-gray-400'}`}
            >
              {label}
            </button>
          ))}
        </div>
      </fieldset>

      <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
        <legend className="text-sm font-semibold text-gray-700 px-1">Study Area (optional)</legend>
        <input
          type="text"
          value={step1.studyArea?.name || ''}
          onChange={(e) => setStep1({ studyArea: { ...step1.studyArea, name: e.target.value, id: e.target.value.toLowerCase().replace(/\s+/g, '-') } })}
          placeholder="e.g. Delhi, Mumbai, Beijing"
          className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
        />
      </fieldset>
    </div>
  )
}

// ── Step 2: Time-series air quality ────────────────────────────────

function Step2({ store }) {
  const { step2, setStep2, setStepValidity } = store
  const baselineInputRef = useRef(null)
  const controlInputRef = useRef(null)

  useEffect(() => {
    setStepValidity(2, step2.baseline.csvData && step2.baseline.csvData.length > 0)
  }, [step2.baseline.csvData, setStepValidity])

  const handleBaselineCsv = useCallback((e) => {
    const file = e.target.files?.[0]
    if (!file) return
    const reader = new FileReader()
    reader.onload = (ev) => {
      const result = parseCsvTimeSeries(ev.target.result)
      if (result.error) {
        setStep2({ baseline: { ...step2.baseline, csvData: null, fileName: file.name, error: result.error } })
      } else {
        setStep2({ baseline: { type: 'csv', csvData: result.data, fileName: file.name, error: null } })
      }
    }
    reader.readAsText(file)
  }, [step2.baseline, setStep2])

  const handleControlCsv = useCallback((e) => {
    const file = e.target.files?.[0]
    if (!file) return
    const reader = new FileReader()
    reader.onload = (ev) => {
      const result = parseCsvTimeSeries(ev.target.result)
      if (result.error) {
        setStep2({ control: { ...step2.control, type: 'csv', csvData: null, fileName: file.name, error: result.error } })
      } else {
        setStep2({ control: { type: 'csv', csvData: result.data, fileName: file.name, error: null } })
      }
    }
    reader.readAsText(file)
  }, [step2.control, setStep2])

  const chartData = useMemo(() => {
    if (!step2.baseline.csvData) return []
    return step2.baseline.csvData.map((d, i) => ({
      date: d.date,
      baseline: d.concentration,
      control: step2.control.type === 'csv' && step2.control.csvData
        ? step2.control.csvData[i]?.concentration ?? step2.control.value
        : step2.control.value,
    }))
  }, [step2.baseline.csvData, step2.control])

  return (
    <div className="space-y-6">
      {/* Baseline time series */}
      <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
        <legend className="text-sm font-semibold text-gray-700 px-1">Baseline Daily Concentrations</legend>
        <p className="text-xs text-gray-500 mb-3">Upload a CSV with columns: date, concentration</p>

        {step2.baseline.csvData ? (
          <div className="space-y-3">
            <div className="flex items-center justify-between p-3 bg-green-50 border border-green-200 rounded-lg">
              <span className="text-sm text-green-800 font-medium">{step2.baseline.fileName} — {step2.baseline.csvData.length} days</span>
              <button onClick={() => setStep2({ baseline: { type: 'csv', csvData: null, fileName: null } })} className="text-green-600 hover:text-green-800 text-sm underline">Remove</button>
            </div>

            <div className="h-48">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                  <XAxis dataKey="date" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
                  <YAxis tick={{ fontSize: 10 }} />
                  <Tooltip contentStyle={{ fontSize: 12 }} />
                  <Line type="monotone" dataKey="baseline" stroke="#3b82f6" dot={false} strokeWidth={1.5} name="Baseline" />
                  {chartData[0]?.control != null && (
                    <Line type="monotone" dataKey="control" stroke="#14b8a6" dot={false} strokeWidth={1.5} strokeDasharray="4 2" name="Control" />
                  )}
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        ) : (
          <div>
            <button
              onClick={() => baselineInputRef.current?.click()}
              className="border-2 border-dashed border-gray-300 rounded-lg p-8 w-full text-center hover:border-gray-400 cursor-pointer"
            >
              <p className="text-sm text-gray-600">Click to upload CSV</p>
              <p className="text-xs text-gray-400 mt-1">Columns: date, concentration</p>
            </button>
            <input ref={baselineInputRef} type="file" accept=".csv" className="hidden" onChange={handleBaselineCsv} />
            {step2.baseline.error && <p className="mt-2 text-sm text-red-600">{step2.baseline.error}</p>}
          </div>
        )}
      </fieldset>

      {/* Control scenario */}
      <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
        <legend className="text-sm font-semibold text-gray-700 px-1">Control Scenario</legend>
        <div className="flex gap-4 mb-3">
          <label className="flex items-center gap-2 text-sm cursor-pointer">
            <input type="radio" checked={step2.control.type === 'constant'} onChange={() => setStep2({ control: { ...step2.control, type: 'constant' } })} className="text-blue-600" />
            Constant value
          </label>
          <label className="flex items-center gap-2 text-sm cursor-pointer">
            <input type="radio" checked={step2.control.type === 'csv'} onChange={() => setStep2({ control: { ...step2.control, type: 'csv' } })} className="text-blue-600" />
            Daily CSV
          </label>
        </div>

        {step2.control.type === 'constant' ? (
          <div className="flex items-center gap-2">
            <input
              type="number" min="0" step="any"
              value={step2.control.value ?? ''}
              onChange={(e) => setStep2({ control: { ...step2.control, value: e.target.value === '' ? null : Number(e.target.value) } })}
              placeholder="e.g. 5.0"
              className="w-40 rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
            />
            <span className="text-sm text-gray-500">μg/m³</span>
          </div>
        ) : (
          <div>
            {step2.control.csvData ? (
              <div className="flex items-center justify-between p-3 bg-green-50 border border-green-200 rounded-lg">
                <span className="text-sm text-green-800 font-medium">{step2.control.fileName} — {step2.control.csvData.length} days</span>
                <button onClick={() => setStep2({ control: { type: 'csv', csvData: null, fileName: null } })} className="text-green-600 hover:text-green-800 text-sm underline">Remove</button>
              </div>
            ) : (
              <div>
                <button onClick={() => controlInputRef.current?.click()} className="border-2 border-dashed border-gray-300 rounded-lg p-4 w-full text-center hover:border-gray-400 cursor-pointer">
                  <p className="text-sm text-gray-600">Upload control CSV</p>
                </button>
                <input ref={controlInputRef} type="file" accept=".csv" className="hidden" onChange={handleControlCsv} />
                {step2.control.error && <p className="mt-2 text-sm text-red-600">{step2.control.error}</p>}
              </div>
            )}
          </div>
        )}
      </fieldset>
    </div>
  )
}

// ── Step 3: Population ─────────────────────────────────────────────

function Step3({ store }) {
  const { step3, setStep3, setStepValidity } = store

  useEffect(() => {
    setStepValidity(3, step3.totalPopulation != null && step3.totalPopulation > 0)
  }, [step3.totalPopulation, setStepValidity])

  return (
    <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
      <legend className="text-sm font-semibold text-gray-700 px-1">Exposed Population</legend>
      <input
        type="number" min="0" step="1"
        value={step3.totalPopulation ?? ''}
        onChange={(e) => setStep3({ totalPopulation: e.target.value === '' ? null : Number(e.target.value) })}
        placeholder="e.g. 20000000"
        className="w-full max-w-xs rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
      />
      {step3.totalPopulation > 0 && (
        <p className="text-xs text-gray-400 mt-1">{Number(step3.totalPopulation).toLocaleString()} people</p>
      )}
    </fieldset>
  )
}

// ── Step 4: Incidence rate ─────────────────────────────────────────

function Step4({ store }) {
  const { step4, setStep4, setStepValidity } = store

  useEffect(() => {
    setStepValidity(4, step4.baselineIncidence != null && step4.baselineIncidence > 0)
  }, [step4.baselineIncidence, setStepValidity])

  return (
    <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
      <legend className="text-sm font-semibold text-gray-700 px-1">Baseline Incidence Rate</legend>
      <p className="text-xs text-gray-500 mb-3">Annual all-cause mortality rate (per person per year)</p>
      <div className="flex items-center gap-2">
        <input
          type="number" min="0" max="1" step="0.0001"
          value={step4.baselineIncidence ?? ''}
          onChange={(e) => setStep4({ baselineIncidence: e.target.value === '' ? null : Number(e.target.value) })}
          placeholder="e.g. 0.008"
          className="w-40 rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
        />
        <span className="text-sm text-gray-500">per person/yr</span>
      </div>
    </fieldset>
  )
}

// ── Step 5: Short-term CRFs ────────────────────────────────────────

function Step5({ store }) {
  const { step1, step5, setStep5, setStepValidity } = store

  const shortTermCRFs = useMemo(() =>
    crfLibrary.filter((c) => c.shortTerm === true && c.pollutant === step1.pollutant),
    [step1.pollutant],
  )

  useEffect(() => {
    setStepValidity(5, step5.selectedCRFs.length > 0)
  }, [step5.selectedCRFs, setStepValidity])

  const toggle = (id) => {
    const selected = step5.selectedCRFs.includes(id)
      ? step5.selectedCRFs.filter((c) => c !== id)
      : [...step5.selectedCRFs, id]
    setStep5({ selectedCRFs: selected })
  }

  return (
    <div className="space-y-4">
      <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg text-sm text-blue-700">
        Showing <strong>{shortTermCRFs.length}</strong> short-term CRFs for {POLLUTANT_LABELS[step1.pollutant] || 'the selected pollutant'}.
      </div>

      {shortTermCRFs.length === 0 && (
        <p className="text-sm text-gray-400 text-center py-8">No short-term CRFs available for this pollutant.</p>
      )}

      <div className="space-y-2">
        {shortTermCRFs.map((crf) => (
          <label
            key={crf.id}
            className={`flex items-start gap-3 p-4 rounded-xl border cursor-pointer transition-colors
              ${step5.selectedCRFs.includes(crf.id)
                ? 'border-blue-400 bg-blue-50'
                : 'border-gray-200 bg-white hover:border-gray-300'}`}
          >
            <input
              type="checkbox"
              checked={step5.selectedCRFs.includes(crf.id)}
              onChange={() => toggle(crf.id)}
              className="mt-0.5 text-blue-600"
            />
            <div className="flex-1 min-w-0">
              <p className="text-sm font-medium text-gray-900">{crf.endpoint}</p>
              <p className="text-xs text-gray-500 mt-0.5">{crf.source} — Age: {crf.ageRange} — β: {crf.beta}</p>
            </div>
            <span className="text-xs px-2 py-0.5 rounded-full bg-gray-100 text-gray-600 font-medium shrink-0">
              {crf.framework.toUpperCase()}
            </span>
          </label>
        ))}
      </div>
    </div>
  )
}

// ── Step 6: Options ────────────────────────────────────────────────

function Step6({ store }) {
  const { step6, setStep6, setStepValidity } = store

  useEffect(() => { setStepValidity(6, true) }, [setStepValidity])

  return (
    <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
      <legend className="text-sm font-semibold text-gray-700 px-1">Monte Carlo Iterations</legend>
      <select
        value={step6.monteCarloIterations}
        onChange={(e) => setStep6({ monteCarloIterations: Number(e.target.value) })}
        className="rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
      >
        {[100, 500, 1000, 5000].map((n) => <option key={n} value={n}>{n.toLocaleString()}</option>)}
      </select>
      <p className="text-xs text-gray-400 mt-1">Higher values increase precision but take longer.</p>
    </fieldset>
  )
}

// ── Step 7: Review & Run ───────────────────────────────────────────

function Step7({ store, navigate }) {
  const { step1, step2, step3, step4, step5, step6, setResults } = store
  const [running, setRunning] = useState(false)
  const [error, setError] = useState(null)

  const selectedCRFDetails = useMemo(() =>
    step5.selectedCRFs.map((id) => crfLibrary.find((c) => c.id === id)).filter(Boolean),
    [step5.selectedCRFs],
  )

  const handleRun = useCallback(async () => {
    setRunning(true)
    setError(null)
    try {
      const config = {
        baselineTimeSeries: step2.baseline.csvData,
        controlConcentration: step2.control.type === 'csv' ? step2.control.csvData : (step2.control.value ?? 0),
        baselineIncidence: step4.baselineIncidence,
        population: step3.totalPopulation,
        selectedCRFs: selectedCRFDetails,
        monteCarloIterations: step6.monteCarloIterations,
      }
      const results = await Promise.resolve(computeTimeSeriesHIA(config))
      setResults(results)
      navigate('/timeseries/results')
    } catch (err) {
      setError(err.message || 'Analysis failed.')
    } finally {
      setRunning(false)
    }
  }, [step2, step3, step4, step6, selectedCRFDetails, setResults, navigate])

  const Row = ({ label, value }) => (
    <div className="flex justify-between text-sm">
      <span className="text-gray-500">{label}</span>
      <span className="font-medium text-gray-900">{value || '—'}</span>
    </div>
  )

  return (
    <div className="space-y-6">
      <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5 space-y-2">
        <legend className="text-sm font-semibold text-gray-700 px-1">Summary</legend>
        <Row label="Pollutant" value={POLLUTANT_LABELS[step1.pollutant]} />
        <Row label="Days" value={step2.baseline.csvData?.length} />
        <Row label="Control" value={step2.control.type === 'constant' ? `${step2.control.value} μg/m³` : 'Daily CSV'} />
        <Row label="Population" value={step3.totalPopulation?.toLocaleString()} />
        <Row label="Incidence rate" value={step4.baselineIncidence} />
        <Row label="CRFs" value={`${step5.selectedCRFs.length} selected`} />
        <Row label="MC iterations" value={step6.monteCarloIterations.toLocaleString()} />
      </fieldset>

      {error && (
        <div className="p-4 bg-red-50 border border-red-200 rounded-xl text-sm text-red-700">{error}</div>
      )}

      <button
        onClick={handleRun}
        disabled={running}
        className="w-full px-6 py-3 rounded-lg bg-green-600 text-white font-semibold hover:bg-green-700 transition-colors disabled:bg-gray-300 disabled:cursor-not-allowed flex items-center justify-center gap-2"
      >
        {running ? (
          <>
            <svg className="animate-spin h-5 w-5" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
            Running…
          </>
        ) : 'RUN TIME-SERIES ANALYSIS'}
      </button>
    </div>
  )
}

// ── Step dispatcher ────────────────────────────────────────────────

const STEP_COMPONENTS = { 1: Step1, 2: Step2, 3: Step3, 4: Step4, 5: Step5, 6: Step6, 7: Step7 }

// ── Main component ─────────────────────────────────────────────────

export default function TimeseriesStep() {
  const { step } = useParams()
  const navigate = useNavigate()
  const currentStep = Number(step) || 1
  const store = useTimeseriesStore()
  const { totalSteps, stepValidity, markStepCompleted, setCurrentStep } = store

  useEffect(() => { setCurrentStep(currentStep) }, [currentStep, setCurrentStep])

  const isStepValid = stepValidity[currentStep] ?? false
  const StepComponent = STEP_COMPONENTS[currentStep]

  const goBack = () => currentStep > 1 ? navigate(`/timeseries/${currentStep - 1}`) : navigate('/')
  const goNext = () => {
    markStepCompleted(currentStep)
    if (currentStep < totalSteps) navigate(`/timeseries/${currentStep + 1}`)
    else navigate('/timeseries/results')
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Progress bar */}
      <nav className="bg-white border-b border-gray-200 px-4 py-3">
        <div className="max-w-4xl mx-auto flex items-center gap-2">
          {Array.from({ length: totalSteps }, (_, i) => i + 1).map((n) => (
            <div key={n} className="flex items-center gap-2">
              {n > 1 && <div className={`w-6 h-0.5 ${n <= currentStep ? 'bg-green-400' : 'bg-gray-200'}`} />}
              <button
                onClick={() => navigate(`/timeseries/${n}`)}
                className={`w-8 h-8 rounded-full text-xs font-bold flex items-center justify-center transition-colors
                  ${n === currentStep ? 'bg-green-600 text-white' : n < currentStep ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-400'}`}
              >
                {n}
              </button>
            </div>
          ))}
          <span className="ml-4 text-sm text-gray-500 hidden sm:inline">{STEP_TITLES[currentStep]}</span>
        </div>
      </nav>

      {/* Content */}
      <main className="max-w-4xl mx-auto p-6 lg:p-8">
        <h1 className="text-2xl font-bold text-gray-900 mb-6">{STEP_TITLES[currentStep]}</h1>
        {StepComponent && <StepComponent store={store} navigate={navigate} />}
      </main>

      {/* Navigation */}
      {currentStep < 7 && (
        <div className="fixed bottom-0 inset-x-0 bg-white border-t border-gray-200 px-6 py-4">
          <div className="max-w-4xl mx-auto flex justify-between">
            <button onClick={goBack} className="px-6 py-2.5 rounded-lg border border-gray-300 text-gray-700 hover:bg-gray-100 text-sm font-medium">
              Back
            </button>
            <button
              onClick={goNext}
              disabled={!isStepValid}
              className={`px-6 py-2.5 rounded-lg text-sm font-medium ${isStepValid ? 'bg-green-600 text-white hover:bg-green-700' : 'bg-gray-300 text-white cursor-not-allowed'}`}
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
