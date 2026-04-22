import { useEffect, useState, useCallback, useMemo } from 'react'
import useAnalysisStore from '../../stores/useAnalysisStore'
import crfLibrary from '../../data/crf-library.json'

// ── Constants ──────────────────────────────────────────────────────

const FRAMEWORKS = [
  { id: 'epa', label: 'EPA Standard' },
  { id: 'gbd', label: 'GBD 2023 MR-BRT' },
  { id: 'gemm', label: 'GEMM' },
  { id: 'fusion', label: 'Fusion' },
  { id: 'hrapie', label: 'HRAPIE' },
]

const POLLUTANT_LABELS = {
  pm25: 'PM2.5',
  ozone: 'Ozone',
  no2: 'NO₂',
}

const FUNCTIONAL_FORMS = [
  { id: 'log-linear', label: 'Log-linear' },
  { id: 'linear', label: 'Linear' },
  { id: 'mr-brt', label: 'MR-BRT (spline)' },
  { id: 'gemm-nlt', label: 'GEMM (no lower threshold)' },
  { id: 'power', label: 'Power' },
]

const EMPTY_CUSTOM = {
  endpoint: '',
  beta: '',
  se: '',
  functionalForm: 'log-linear',
  ageMin: '',
  ageMax: '',
}

// ── Framework tab bar ──────────────────────────────────────────────

function FrameworkTabs({ frameworks, activeId, onChange, crfCounts }) {
  return (
    <div className="flex border-b border-gray-200 mb-4 overflow-x-auto" role="tablist">
      {frameworks.map((fw) => {
        const count = crfCounts[fw.id] || 0
        return (
          <button
            key={fw.id}
            role="tab"
            aria-selected={activeId === fw.id}
            onClick={() => onChange(fw.id)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors -mb-px whitespace-nowrap
              ${activeId === fw.id
                ? 'border-blue-500 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'}
              ${count === 0 ? 'opacity-50' : ''}`}
          >
            {fw.label}
            {count > 0 && (
              <span className="ml-1.5 text-xs text-gray-400">({count})</span>
            )}
          </button>
        )
      })}
    </div>
  )
}

// ── GBD tooltip ────────────────────────────────────────────────────

function GbdTooltip({ show }) {
  if (!show) return null
  return (
    <div className="absolute z-50 bottom-full left-1/2 -translate-x-1/2 mb-2 w-64 px-3 py-2
                    bg-gray-800 text-white text-xs rounded-lg shadow-lg leading-relaxed pointer-events-none">
      <p className="font-medium mb-1">MR-BRT Risk Curve</p>
      <div className="h-16 bg-gray-700 rounded flex items-center justify-center text-gray-400 text-[10px]">
        Risk curve visualization will be added when GBD data is loaded
      </div>
      <div className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-gray-800" />
    </div>
  )
}

// ── CRF table row ──────────────────────────────────────────────────

function CRFRow({ crf, selected, onToggle, isGbd }) {
  const [hovered, setHovered] = useState(false)

  const effectEstimate = useMemo(() => {
    const rr = Math.exp(crf.beta * 10).toFixed(3)
    const rrLow = Math.exp(crf.betaLow * 10).toFixed(3)
    const rrHigh = Math.exp(crf.betaHigh * 10).toFixed(3)
    return `${rr} (${rrLow}–${rrHigh})`
  }, [crf.beta, crf.betaLow, crf.betaHigh])

  return (
    <tr
      className={`transition-colors cursor-pointer
        ${selected ? 'bg-blue-50' : hovered ? 'bg-gray-50' : 'bg-white'}`}
      onClick={onToggle}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      <td className="px-3 py-2.5 border-b border-gray-100 w-10">
        <input
          type="checkbox"
          checked={selected}
          onChange={onToggle}
          onClick={(e) => e.stopPropagation()}
          className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
        />
      </td>
      <td className="px-3 py-2.5 border-b border-gray-100 text-sm text-gray-700 relative">
        {crf.source}
        {isGbd && <GbdTooltip show={hovered} />}
      </td>
      <td className="px-3 py-2.5 border-b border-gray-100 text-sm font-medium text-gray-900">
        {crf.endpoint}
      </td>
      <td className="px-3 py-2.5 border-b border-gray-100 text-sm text-gray-600 font-mono text-right">
        {effectEstimate}
      </td>
      <td className="px-3 py-2.5 border-b border-gray-100 text-sm text-gray-600 font-mono text-center">
        {crf.ageRange}
      </td>
    </tr>
  )
}

// ── CRF table ──────────────────────────────────────────────────────

function CRFTable({ crfs, selectedIds, onToggle, isGbd }) {
  if (crfs.length === 0) {
    return (
      <p className="text-sm text-gray-400 text-center py-8">
        No CRFs available for this pollutant under this framework.
      </p>
    )
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm border border-gray-200 rounded-lg overflow-hidden">
        <thead>
          <tr className="bg-gray-50">
            <th className="px-3 py-2 border-b border-gray-200 w-10" />
            <th className="px-3 py-2 text-left font-medium text-gray-600 border-b border-gray-200">Study</th>
            <th className="px-3 py-2 text-left font-medium text-gray-600 border-b border-gray-200">Endpoint</th>
            <th className="px-3 py-2 text-right font-medium text-gray-600 border-b border-gray-200">
              RR per 10 units (95% CI)
            </th>
            <th className="px-3 py-2 text-center font-medium text-gray-600 border-b border-gray-200">Age Range</th>
          </tr>
        </thead>
        <tbody>
          {crfs.map((crf) => (
            <CRFRow
              key={crf.id}
              crf={crf}
              selected={selectedIds.has(crf.id)}
              onToggle={() => onToggle(crf.id)}
              isGbd={isGbd}
            />
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ── Selection summary ──────────────────────────────────────────────

function SelectionSummary({ selectedCRFs, allCrfs, customCRFs, onRemove, onRemoveCustom }) {
  const selectedLibrary = allCrfs.filter((c) => selectedCRFs.includes(c.id))

  if (selectedLibrary.length === 0 && customCRFs.length === 0) {
    return (
      <div className="p-4 bg-gray-50 border border-gray-200 rounded-lg text-center">
        <p className="text-sm text-gray-400">No CRFs selected yet. Check boxes above to add them.</p>
      </div>
    )
  }

  return (
    <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
      <legend className="text-sm font-semibold text-gray-700 px-1">
        Selected CRFs ({selectedLibrary.length + customCRFs.length})
      </legend>
      <div className="space-y-1.5">
        {selectedLibrary.map((crf) => (
          <div key={crf.id} className="flex items-center justify-between py-1.5 px-3 rounded bg-blue-50 text-sm">
            <div className="flex items-center gap-3 min-w-0">
              <span className="inline-block px-1.5 py-0.5 rounded text-[10px] font-medium bg-blue-100 text-blue-700 uppercase shrink-0">
                {crf.framework}
              </span>
              <span className="text-gray-900 font-medium truncate">{crf.endpoint}</span>
              <span className="text-gray-500 truncate hidden sm:inline">{crf.source}</span>
            </div>
            <button
              onClick={() => onRemove(crf.id)}
              className="text-gray-400 hover:text-red-500 ml-2 shrink-0"
              aria-label={`Remove ${crf.endpoint}`}
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
        ))}
        {customCRFs.map((crf, i) => (
          <div key={`custom-${i}`} className="flex items-center justify-between py-1.5 px-3 rounded bg-green-50 text-sm">
            <div className="flex items-center gap-3 min-w-0">
              <span className="inline-block px-1.5 py-0.5 rounded text-[10px] font-medium bg-green-100 text-green-700 uppercase shrink-0">
                Custom
              </span>
              <span className="text-gray-900 font-medium truncate">{crf.endpoint}</span>
              <span className="text-gray-500 truncate hidden sm:inline">
                {crf.functionalForm} &middot; ages {crf.ageMin}–{crf.ageMax}
              </span>
            </div>
            <button
              onClick={() => onRemoveCustom(i)}
              className="text-gray-400 hover:text-red-500 ml-2 shrink-0"
              aria-label={`Remove custom ${crf.endpoint}`}
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
        ))}
      </div>
    </fieldset>
  )
}

// ── Custom CRF form ────────────────────────────────────────────────

function CustomCRFForm({ onAdd }) {
  const [open, setOpen] = useState(false)
  const [form, setForm] = useState({ ...EMPTY_CUSTOM })
  const [error, setError] = useState(null)

  const handleChange = (field, value) => {
    setForm((f) => ({ ...f, [field]: value }))
    setError(null)
  }

  const handleAdd = () => {
    if (!form.endpoint.trim()) {
      setError('Endpoint name is required.')
      return
    }
    if (form.beta === '' || isNaN(Number(form.beta))) {
      setError('Beta (or log RR) is required and must be a number.')
      return
    }
    if (form.se === '' || isNaN(Number(form.se)) || Number(form.se) <= 0) {
      setError('Standard error is required and must be positive.')
      return
    }
    if (form.ageMin === '' || form.ageMax === '') {
      setError('Age min and max are required.')
      return
    }
    if (Number(form.ageMin) >= Number(form.ageMax)) {
      setError('Age min must be less than age max.')
      return
    }

    onAdd({
      endpoint: form.endpoint.trim(),
      beta: Number(form.beta),
      se: Number(form.se),
      functionalForm: form.functionalForm,
      ageMin: Number(form.ageMin),
      ageMax: Number(form.ageMax),
    })
    setForm({ ...EMPTY_CUSTOM })
    setError(null)
  }

  return (
    <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200">
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between p-5 text-left"
      >
        <legend className="text-sm font-semibold text-gray-700">
          Custom CRF
          <span className="ml-2 text-xs font-normal text-gray-400">(add your own)</span>
        </legend>
        <svg
          className={`w-5 h-5 text-gray-400 transition-transform ${open ? 'rotate-180' : ''}`}
          fill="none" stroke="currentColor" viewBox="0 0 24 24"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {open && (
        <div className="px-5 pb-5 -mt-2 space-y-4">
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            {/* Endpoint name */}
            <div className="sm:col-span-2">
              <label className="block text-sm text-gray-600 mb-1">Endpoint name</label>
              <input
                type="text"
                value={form.endpoint}
                onChange={(e) => handleChange('endpoint', e.target.value)}
                placeholder="e.g. Cardiovascular mortality"
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm
                           focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              />
            </div>

            {/* Beta */}
            <div>
              <label className="block text-sm text-gray-600 mb-1">
                Beta (log RR per unit)
              </label>
              <input
                type="number"
                step="any"
                value={form.beta}
                onChange={(e) => handleChange('beta', e.target.value)}
                placeholder="e.g. 0.006"
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm
                           focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              />
            </div>

            {/* Standard error */}
            <div>
              <label className="block text-sm text-gray-600 mb-1">Standard error</label>
              <input
                type="number"
                step="any"
                min="0"
                value={form.se}
                onChange={(e) => handleChange('se', e.target.value)}
                placeholder="e.g. 0.001"
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm
                           focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              />
            </div>

            {/* Functional form */}
            <div>
              <label className="block text-sm text-gray-600 mb-1">Functional form</label>
              <select
                value={form.functionalForm}
                onChange={(e) => handleChange('functionalForm', e.target.value)}
                className="w-full rounded-lg border border-gray-300 px-3 py-2.5 text-sm
                           focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              >
                {FUNCTIONAL_FORMS.map((ff) => (
                  <option key={ff.id} value={ff.id}>{ff.label}</option>
                ))}
              </select>
            </div>

            {/* Age range */}
            <div className="flex gap-3">
              <div className="flex-1">
                <label className="block text-sm text-gray-600 mb-1">Age min</label>
                <input
                  type="number"
                  min="0"
                  max="120"
                  value={form.ageMin}
                  onChange={(e) => handleChange('ageMin', e.target.value)}
                  placeholder="0"
                  className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm
                             focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
                />
              </div>
              <div className="flex-1">
                <label className="block text-sm text-gray-600 mb-1">Age max</label>
                <input
                  type="number"
                  min="0"
                  max="120"
                  value={form.ageMax}
                  onChange={(e) => handleChange('ageMax', e.target.value)}
                  placeholder="99"
                  className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm
                             focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
                />
              </div>
            </div>
          </div>

          {error && <p className="text-sm text-red-600">{error}</p>}

          <button
            type="button"
            onClick={handleAdd}
            className="px-4 py-2 text-sm font-medium rounded-lg bg-blue-600 text-white
                       hover:bg-blue-700 transition-colors"
          >
            Add Custom CRF
          </button>
        </div>
      )}
    </fieldset>
  )
}

// ── Main component ─────────────────────────────────────────────────

export default function Step5CRFs() {
  const { step1, step4, step5, setStep5, setStepValidity } = useAnalysisStore()
  const pollutant = step1.pollutant
  const pollutantLabel = POLLUTANT_LABELS[pollutant] || 'Pollutant'

  const selectedCRFs = step5.selectedCRFs || []
  const customCRFs = step5.customCRFs || []
  const selectedSet = useMemo(() => new Set(selectedCRFs), [selectedCRFs])

  const selectedEndpoints = step4?.selectedEndpoints || []
  const selectedEndpointsSet = useMemo(
    () => new Set(selectedEndpoints),
    [selectedEndpoints],
  )

  const [activeFramework, setActiveFramework] = useState('epa')

  // Filter CRFs by pollutant and by the endpoints the user picked in
  // Step 4, grouped by framework.
  const crfsByFramework = useMemo(() => {
    const grouped = {}
    FRAMEWORKS.forEach((fw) => { grouped[fw.id] = [] })
    crfLibrary
      .filter((c) => c.pollutant === pollutant && selectedEndpointsSet.has(c.endpoint))
      .forEach((c) => {
        if (grouped[c.framework]) grouped[c.framework].push(c)
      })
    return grouped
  }, [pollutant, selectedEndpointsSet])

  // Count CRFs per framework (for tab badges)
  const crfCounts = useMemo(() => {
    const counts = {}
    FRAMEWORKS.forEach((fw) => { counts[fw.id] = crfsByFramework[fw.id]?.length || 0 })
    return counts
  }, [crfsByFramework])

  // All CRFs for current pollutant + selected endpoints (flat)
  const allPollutantCrfs = useMemo(
    () => crfLibrary.filter(
      (c) => c.pollutant === pollutant && selectedEndpointsSet.has(c.endpoint),
    ),
    [pollutant, selectedEndpointsSet],
  )

  // ── Validation ─────────────────────────────────────────────────

  useEffect(() => {
    setStepValidity(5, selectedCRFs.length > 0 || customCRFs.length > 0)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedCRFs, customCRFs])

  // ── Handlers ───────────────────────────────────────────────────

  const handleToggle = useCallback((crfId) => {
    const next = selectedCRFs.includes(crfId)
      ? selectedCRFs.filter((id) => id !== crfId)
      : [...selectedCRFs, crfId]
    setStep5({ selectedCRFs: next })
  }, [selectedCRFs, setStep5])

  const handleRemove = useCallback((crfId) => {
    setStep5({ selectedCRFs: selectedCRFs.filter((id) => id !== crfId) })
  }, [selectedCRFs, setStep5])

  const handleSelectAll = useCallback(() => {
    const frameworkCrfs = crfsByFramework[activeFramework] || []
    const frameworkIds = frameworkCrfs.map((c) => c.id)
    const allSelected = frameworkIds.every((id) => selectedSet.has(id))

    if (allSelected) {
      // Deselect all in this framework
      setStep5({ selectedCRFs: selectedCRFs.filter((id) => !frameworkIds.includes(id)) })
    } else {
      // Select all in this framework
      const merged = [...new Set([...selectedCRFs, ...frameworkIds])]
      setStep5({ selectedCRFs: merged })
    }
  }, [activeFramework, crfsByFramework, selectedCRFs, selectedSet, setStep5])

  const handleAddCustom = useCallback((crf) => {
    setStep5({ customCRFs: [...customCRFs, crf] })
  }, [customCRFs, setStep5])

  const handleRemoveCustom = useCallback((index) => {
    setStep5({ customCRFs: customCRFs.filter((_, i) => i !== index) })
  }, [customCRFs, setStep5])

  // ── Render ─────────────────────────────────────────────────────

  if (!pollutant) {
    return (
      <>
        <h1 className="text-2xl font-bold text-gray-900 mb-1">Concentration-Response Functions</h1>
        <div className="mt-6 p-6 bg-amber-50 border border-amber-200 rounded-xl text-center">
          <p className="text-amber-700">
            Please select a pollutant in Step 1 before selecting CRFs.
          </p>
        </div>
      </>
    )
  }

  const currentFrameworkCrfs = crfsByFramework[activeFramework] || []
  const allCurrentSelected = currentFrameworkCrfs.length > 0 &&
    currentFrameworkCrfs.every((c) => selectedSet.has(c.id))

  if (selectedEndpoints.length === 0) {
    return (
      <>
        <h1 className="text-2xl font-bold text-gray-900 mb-1">Concentration-Response Functions</h1>
        <div className="mt-6 p-6 bg-amber-50 border border-amber-200 rounded-xl text-center">
          <p className="text-amber-700">
            No health endpoints have been selected yet. Go back to Step 4 and pick
            which outcomes you want to analyze — only those endpoints' CRFs will
            appear here.
          </p>
        </div>
      </>
    )
  }

  return (
    <>
      <h1 className="text-2xl font-bold text-gray-900 mb-1">Concentration-Response Functions</h1>
      <p className="text-sm text-gray-500 mb-6">
        Select one or more CRFs for{' '}
        <span className="font-medium text-gray-700">{pollutantLabel}</span>.
        You can mix CRFs from different frameworks and add custom functions.
      </p>

      <div className="space-y-6">
        {/* ── How CRFs are evaluated ─────────────────────────────── */}
        <div className="p-4 bg-gray-50 border border-gray-200 rounded-xl text-xs text-gray-600 space-y-2 leading-relaxed">
          <p className="font-medium text-gray-800">How each CRF is evaluated at run time</p>
          <ul className="list-disc pl-5 space-y-1">
            <li>
              <span className="font-medium text-gray-700">Log-linear (EPA / HRAPIE)</span> — PAF = 1 − exp(−β · ΔC).
              Flat in β, linear in ΔC. Same slope at every concentration.
            </li>
            <li>
              <span className="font-medium text-gray-700">MR-BRT spline (GBD 2023)</span> — RR is interpolated
              at both c_baseline and c_control from IHME's tabulated spline
              (~2,500 knots, 0–2,500 μg/m³). PAF = (RR(c_base) − RR(c_ctrl)) / RR(c_base).
              The slope flattens at high exposures — concentration-specific.
            </li>
            <li>
              <span className="font-medium text-gray-700">GEMM</span> — closed-form HR(z) = exp(θ · z / (1 + exp(−(z−μ)/τ))) with
              z = max(0, C − 2.4). Also concentration-specific.
            </li>
            <li>
              <span className="font-medium text-gray-700">Fusion-CanCHEC</span> — log-RR is
              read from the Weichenthal et al. (2022) hybrid table (eSCHIF below
              9.8 μg/m³, Fusion above), 1,200 knots over 0–120 μg/m³, then
              PAF = (RR(c_base) − RR(c_ctrl)) / RR(c_base). All-cause mortality is
              wired; CVD and lung-cancer Fusion CRFs fall back to log-linear until
              endpoint-specific parameters are published (see
              <span className="font-mono"> docs/outstanding_work.md</span>).
            </li>
          </ul>
        </div>

        {/* ── Framework tabs + CRF table ─────────────────────────── */}
        <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
          <legend className="text-sm font-semibold text-gray-700 px-1">CRF Library</legend>

          <FrameworkTabs
            frameworks={FRAMEWORKS}
            activeId={activeFramework}
            onChange={setActiveFramework}
            crfCounts={crfCounts}
          />

          {/* Select-all toggle */}
          {currentFrameworkCrfs.length > 0 && (
            <div className="flex items-center justify-between mb-3">
              <label className="flex items-center gap-2 text-sm text-gray-600 cursor-pointer">
                <input
                  type="checkbox"
                  checked={allCurrentSelected}
                  onChange={handleSelectAll}
                  className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                />
                Select all in {FRAMEWORKS.find((f) => f.id === activeFramework)?.label}
              </label>
              <span className="text-xs text-gray-400">
                {currentFrameworkCrfs.filter((c) => selectedSet.has(c.id)).length}/{currentFrameworkCrfs.length} selected
              </span>
            </div>
          )}

          <CRFTable
            crfs={currentFrameworkCrfs}
            selectedIds={selectedSet}
            onToggle={handleToggle}
            isGbd={activeFramework === 'gbd'}
          />
        </fieldset>

        {/* ── Selection summary ──────────────────────────────────── */}
        <SelectionSummary
          selectedCRFs={selectedCRFs}
          allCrfs={allPollutantCrfs}
          customCRFs={customCRFs}
          onRemove={handleRemove}
          onRemoveCustom={handleRemoveCustom}
        />

        {/* ── Custom CRF ────────────────────────────────────────── */}
        <CustomCRFForm onAdd={handleAddCustom} />
      </div>
    </>
  )
}
