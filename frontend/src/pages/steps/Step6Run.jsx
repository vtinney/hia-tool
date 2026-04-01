import { useEffect, useState, useCallback, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import useAnalysisStore from '../../stores/useAnalysisStore'
import { computeHIA } from '../../lib/hia-engine'
import crfLibrary from '../../data/crf-library.json'

// ── Constants ──────────────────────────────────────────────────────

const POLLUTANT_LABELS = {
  pm25: 'PM\u2082.\u2085',
  ozone: 'Ozone',
  no2: 'NO\u2082',
  so2: 'SO\u2082',
}

const FRAMEWORK_LABELS = {
  epa: 'EPA Standard',
  gbd: 'GBD 2023 MR-BRT',
  gemm: 'GEMM',
  fusion: 'Fusion',
  hrapie: 'HRAPIE',
}

const POOLING_OPTIONS = [
  { value: 'fixed', label: 'Fixed effects' },
  { value: 'random', label: 'Random effects' },
  { value: 'separate', label: 'Run separately' },
]

const MC_OPTIONS = [100, 500, 1000, 5000]

const STEP_ROUTES = {
  'Study Area': 1,
  'Air Quality': 2,
  'Population': 3,
  'Health Data': 4,
  'CRFs': 5,
}

// ── Summary section wrapper ────────────────────────────────────────

function SummarySection({ title, stepNum, children }) {
  const navigate = useNavigate()
  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">{title}</h3>
        <button
          onClick={() => navigate(`/analysis/${stepNum}`)}
          className="px-3 py-1 text-xs font-medium text-blue-600 hover:text-blue-700 border border-blue-200 hover:border-blue-300 rounded-lg transition-colors"
        >
          Edit
        </button>
      </div>
      <div className="space-y-1 text-sm text-gray-600">{children}</div>
    </div>
  )
}

function SummaryRow({ label, value }) {
  return (
    <div className="flex justify-between">
      <span className="text-gray-500">{label}</span>
      <span className="font-medium text-gray-900">{value || '—'}</span>
    </div>
  )
}

// ── Save Template Modal ────────────────────────────────────────────

function SaveTemplateModal({ open, onClose, onSave, saving }) {
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')

  if (!open) return null

  const handleSave = () => {
    if (!name.trim()) return
    onSave({ name: name.trim(), description: description.trim() })
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-xl shadow-xl p-6 w-full max-w-md mx-4">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Save as Template</h3>
        <div className="space-y-3">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Template Name</label>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. PM2.5 US Standard Analysis"
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Description (optional)</label>
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              rows={3}
              placeholder="Brief description of this configuration..."
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none resize-none"
            />
          </div>
        </div>
        <div className="flex justify-end gap-3 mt-5">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm font-medium text-gray-700 hover:text-gray-800 border border-gray-300 rounded-lg transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={!name.trim() || saving}
            className="px-4 py-2 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors disabled:bg-gray-300 disabled:cursor-not-allowed"
          >
            {saving ? 'Saving...' : 'Save Template'}
          </button>
        </div>
      </div>
    </div>
  )
}

// ── Main component ─────────────────────────────────────────────────

export default function Step6Run() {
  const navigate = useNavigate()
  const {
    step1, step2, step3, step4, step5, step6,
    setStep6, setStepValidity, setResults, exportConfig,
  } = useAnalysisStore()

  const [running, setRunning] = useState(false)
  const [error, setError] = useState(null)
  const [templateModal, setTemplateModal] = useState(false)
  const [savingTemplate, setSavingTemplate] = useState(false)
  const [templateSaved, setTemplateSaved] = useState(false)

  // Always valid — analysis options have defaults
  useEffect(() => {
    setStepValidity(6, true)
  }, [setStepValidity])

  // ── Derived summaries ───────────────────────────────────────────

  const crfLookup = useMemo(() => {
    const map = {}
    for (const crf of crfLibrary) map[crf.id] = crf
    return map
  }, [])

  const selectedCRFDetails = useMemo(() => {
    return step5.selectedCRFs.map((id) => crfLookup[id]).filter(Boolean)
  }, [step5.selectedCRFs, crfLookup])

  const deltaValue = useMemo(() => {
    const b = step2.baseline?.value
    const c = step2.control?.value
    if (b != null && c != null) return b - c
    return null
  }, [step2.baseline?.value, step2.control?.value])

  const ageDistSummary = useMemo(() => {
    if (!step3.ageGroups) return 'Not specified'
    const entries = Object.entries(step3.ageGroups).filter(([, v]) => v > 0)
    if (entries.length === 0) return 'Not specified'
    return `${entries.length} age groups defined`
  }, [step3.ageGroups])

  const incidenceSummary = useMemo(() => {
    if (!step4.rates) return 'Using CRF defaults'
    const count = Object.values(step4.rates).filter((v) => v != null && v > 0).length
    return count > 0 ? `${count} custom rate${count > 1 ? 's' : ''}` : 'Using CRF defaults'
  }, [step4.rates])

  const yearsLabel = useMemo(() => {
    if (!step1.years) return '—'
    const { start, end } = step1.years
    return start === end ? String(start) : `${start}–${end}`
  }, [step1.years])

  // ── Handlers ────────────────────────────────────────────────────

  const handlePoolingChange = useCallback((value) => {
    setStep6({ poolingMethod: value })
  }, [setStep6])

  const handleMCChange = useCallback((e) => {
    setStep6({ monteCarloIterations: Number(e.target.value) })
  }, [setStep6])

  const handleSaveTemplate = useCallback(async ({ name, description }) => {
    setSavingTemplate(true)
    try {
      const config = exportConfig()
      const res = await fetch('/api/templates', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, description, config }),
      })
      if (!res.ok) throw new Error(`Server error: ${res.status}`)
      setTemplateModal(false)
      setTemplateSaved(true)
      setTimeout(() => setTemplateSaved(false), 3000)
    } catch (err) {
      alert(`Failed to save template: ${err.message}`)
    } finally {
      setSavingTemplate(false)
    }
  }, [exportConfig])

  const handleRunAnalysis = useCallback(async () => {
    setRunning(true)
    setError(null)

    try {
      // Build config for the HIA engine
      const config = {
        pollutant: step1.pollutant,
        baselineConcentration: step2.baseline?.value,
        controlConcentration: step2.control?.value ?? step2.baseline?.value,
        totalPopulation: step3.totalPopulation,
        ageGroups: step3.ageGroups,
        selectedCRFs: step5.selectedCRFs,
        incidenceRates: step4.rates,
        poolingMethod: step6.poolingMethod,
        monteCarloIterations: step6.monteCarloIterations,
      }

      // Run client-side engine (single-value analysis)
      const results = await Promise.resolve(computeHIA(config))
      setResults(results)
      navigate('/analysis/results')
    } catch (err) {
      setError(err.message || 'Analysis failed. Please check your inputs.')
    } finally {
      setRunning(false)
    }
  }, [step1, step2, step3, step4, step5, step6, setResults, navigate])

  // ── Render ──────────────────────────────────────────────────────

  return (
    <>
      <h1 className="text-2xl font-bold text-gray-900 mb-6">Review &amp; Run Analysis</h1>

      {/* Summary sections */}
      <div className="grid gap-4 md:grid-cols-2 mb-6">
        <SummarySection title="Study Area" stepNum={1}>
          <SummaryRow label="Country" value={step1.studyArea?.name} />
          <SummaryRow label="Pollutant" value={POLLUTANT_LABELS[step1.pollutant]} />
          <SummaryRow label="Year(s)" value={yearsLabel} />
        </SummarySection>

        <SummarySection title="Air Quality" stepNum={2}>
          <SummaryRow label="Baseline" value={step2.baseline?.value != null ? `${step2.baseline.value} µg/m³` : null} />
          <SummaryRow label="Control" value={step2.control?.value != null ? `${step2.control.value} µg/m³` : null} />
          <SummaryRow label="Delta (ΔC)" value={deltaValue != null ? `${deltaValue.toFixed(2)} µg/m³` : null} />
        </SummarySection>

        <SummarySection title="Population" stepNum={3}>
          <SummaryRow label="Total" value={step3.totalPopulation?.toLocaleString()} />
          <SummaryRow label="Age groups" value={ageDistSummary} />
        </SummarySection>

        <SummarySection title="Health Data" stepNum={4}>
          <SummaryRow label="Incidence rates" value={incidenceSummary} />
        </SummarySection>
      </div>

      {/* CRFs section — full width */}
      <div className="mb-6">
        <SummarySection title="Concentration-Response Functions" stepNum={5}>
          {selectedCRFDetails.length === 0 ? (
            <p className="text-gray-400 italic">No CRFs selected</p>
          ) : (
            <div className="space-y-2">
              {selectedCRFDetails.map((crf) => (
                <div key={crf.id} className="flex items-center justify-between py-1.5 border-b border-gray-100 last:border-0">
                  <div>
                    <span className="font-medium text-gray-900">{crf.endpoint}</span>
                    <span className="text-gray-400 mx-2">·</span>
                    <span className="text-gray-500">{crf.source}</span>
                  </div>
                  <span className="text-xs px-2 py-0.5 rounded-full bg-gray-100 text-gray-600 font-medium">
                    {FRAMEWORK_LABELS[crf.framework] || crf.framework}
                  </span>
                </div>
              ))}
            </div>
          )}
        </SummarySection>
      </div>

      {/* Analysis options */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5 mb-6">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-4">Analysis Options</h3>

        <div className="grid gap-6 md:grid-cols-2">
          {/* Pooling method */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Pooling Method</label>
            <div className="space-y-2">
              {POOLING_OPTIONS.map((opt) => (
                <label key={opt.value} className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="radio"
                    name="poolingMethod"
                    value={opt.value}
                    checked={step6.poolingMethod === opt.value}
                    onChange={() => handlePoolingChange(opt.value)}
                    className="text-blue-600 focus:ring-blue-500"
                  />
                  <span className="text-sm text-gray-700">{opt.label}</span>
                </label>
              ))}
            </div>
          </div>

          {/* Monte Carlo iterations */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Monte Carlo Iterations</label>
            <select
              value={step6.monteCarloIterations}
              onChange={handleMCChange}
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none"
            >
              {MC_OPTIONS.map((n) => (
                <option key={n} value={n}>{n.toLocaleString()}</option>
              ))}
            </select>
            <p className="text-xs text-gray-400 mt-1">Higher values increase precision but take longer</p>
          </div>
        </div>
      </div>

      {/* Error display */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-xl p-4 mb-6">
          <p className="text-sm text-red-700">{error}</p>
        </div>
      )}

      {/* Action buttons */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <button
            onClick={() => setTemplateModal(true)}
            className="px-4 py-2 text-sm font-medium text-gray-700 hover:text-gray-800 border border-gray-300 hover:border-gray-400 rounded-lg transition-colors"
          >
            Save as Template
          </button>
          {templateSaved && (
            <span className="text-sm text-green-600 font-medium">Template saved!</span>
          )}
        </div>

        <button
          onClick={handleRunAnalysis}
          disabled={running || selectedCRFDetails.length === 0}
          className="px-6 py-2.5 text-sm font-semibold text-white bg-green-600 hover:bg-green-700 rounded-lg transition-colors disabled:bg-gray-300 disabled:cursor-not-allowed flex items-center gap-2"
        >
          {running ? (
            <>
              <svg className="animate-spin h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
              Running...
            </>
          ) : (
            'RUN ANALYSIS'
          )}
        </button>
      </div>

      <SaveTemplateModal
        open={templateModal}
        onClose={() => setTemplateModal(false)}
        onSave={handleSaveTemplate}
        saving={savingTemplate}
      />
    </>
  )
}
