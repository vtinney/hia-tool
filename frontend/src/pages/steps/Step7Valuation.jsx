import { useEffect, useCallback, useMemo } from 'react'
import useAnalysisStore from '../../stores/useAnalysisStore'
import gniData from '../../data/world-bank-gni.json'

// ── Constants ──────────────────────────────────────────────────────

const EPA_VSL_2024 = 11_800_000
const US_ISO = 'USA'
const US_GNI = gniData[US_ISO].gniPcPpp

const ELASTICITY_OPTIONS = [
  { value: 0.8, label: '0.8 (lower bound)' },
  { value: 1.0, label: '1.0 (standard)' },
  { value: 1.2, label: '1.2 (upper bound)' },
]

const DOLLAR_YEARS = [2020, 2021, 2022, 2023, 2024, 2025]
const DEFLATORS = gniData._meta.gdpDeflators

// ── Helpers ────────────────────────────────────────────────────────

function formatCurrency(value, currencyCode = 'USD') {
  if (value == null || isNaN(value)) return '—'
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: currencyCode,
    maximumFractionDigits: 0,
  }).format(value)
}

function parseCurrencyInput(raw) {
  const cleaned = raw.replace(/[^0-9]/g, '')
  return cleaned === '' ? null : Number(cleaned)
}

function formatInputCurrency(value) {
  if (value == null) return ''
  return Number(value).toLocaleString('en-US')
}

// ── Component ──────────────────────────────────────────────────────

export default function Step7Valuation() {
  const {
    step1, step7, setStep7, setStepValidity,
  } = useAnalysisStore()

  // Step 7 is always valid — it's optional
  useEffect(() => {
    setStepValidity(7, true)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // ── Country data lookup ─────────────────────────────────────────

  const studyCountryIso = step1.studyArea?.id || ''
  const isUS = studyCountryIso === US_ISO
  const countryGni = gniData[studyCountryIso]

  const showBenefitTransfer = step7.runValuation && !isUS && countryGni

  // ── Derived: transferred VSL ────────────────────────────────────

  const transferredVsl = useMemo(() => {
    if (!showBenefitTransfer || !countryGni) return null
    const ratio = countryGni.gniPcPpp / US_GNI
    const e = step7.incomeElasticity ?? 1.0
    return EPA_VSL_2024 * Math.pow(ratio, e)
  }, [showBenefitTransfer, countryGni, step7.incomeElasticity])

  // Sync transferred VSL to store
  useEffect(() => {
    if (transferredVsl !== step7.transferredVsl) {
      setStep7({ transferredVsl })
    }
  }, [transferredVsl, step7.transferredVsl, setStep7])

  // ── Derived: deflator-adjusted VSL ──────────────────────────────

  const activeVsl = showBenefitTransfer ? transferredVsl : step7.vsl
  const baseDeflator = DEFLATORS['2024']
  const targetDeflator = DEFLATORS[String(step7.dollarYear)]

  const adjustedVsl = useMemo(() => {
    if (activeVsl == null || !baseDeflator || !targetDeflator) return null
    return activeVsl * (targetDeflator / baseDeflator)
  }, [activeVsl, baseDeflator, targetDeflator])

  // ── Derived: local currency conversion ──────────────────────────

  const localCurrencyVsl = useMemo(() => {
    if (!countryGni || adjustedVsl == null) return null
    return adjustedVsl * countryGni.pppRate
  }, [countryGni, adjustedVsl])

  // ── Currency options ────────────────────────────────────────────

  const currencyOptions = useMemo(() => {
    const opts = [{ value: 'USD', label: 'USD — US Dollar' }]
    if (countryGni && countryGni.currencyCode !== 'USD') {
      opts.push({
        value: countryGni.currencyCode,
        label: `${countryGni.currencyCode} — ${countryGni.currencyName}`,
      })
    }
    return opts
  }, [countryGni])

  // ── Handlers ────────────────────────────────────────────────────

  const handleToggle = useCallback(() => {
    setStep7({ runValuation: !step7.runValuation })
  }, [step7.runValuation, setStep7])

  const handleVslChange = useCallback((e) => {
    setStep7({ vsl: parseCurrencyInput(e.target.value) })
  }, [setStep7])

  const handleElasticityChange = useCallback((e) => {
    setStep7({ incomeElasticity: Number(e.target.value) })
  }, [setStep7])

  const handleCurrencyChange = useCallback((e) => {
    setStep7({ currency: e.target.value })
  }, [setStep7])

  const handleDollarYearChange = useCallback((e) => {
    setStep7({ dollarYear: Number(e.target.value) })
  }, [setStep7])

  // ── Render ──────────────────────────────────────────────────────

  return (
    <>
      <h1 className="text-2xl font-bold text-gray-900 mb-6">Economic Valuation</h1>

      {/* Toggle */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5 mb-6">
        <label className="flex items-center gap-3 cursor-pointer">
          <button
            role="switch"
            aria-checked={step7.runValuation}
            onClick={handleToggle}
            className={`relative inline-flex h-6 w-11 shrink-0 rounded-full transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 ${
              step7.runValuation ? 'bg-blue-600' : 'bg-gray-200'
            }`}
          >
            <span
              className={`inline-block h-5 w-5 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out mt-0.5 ${
                step7.runValuation ? 'translate-x-[22px]' : 'translate-x-0.5'
              }`}
            />
          </button>
          <div>
            <span className="text-sm font-semibold text-gray-900">Include economic valuation?</span>
            <span className="ml-2 text-xs font-medium text-amber-600 bg-amber-50 px-2 py-0.5 rounded-full">Optional</span>
          </div>
        </label>

        {!step7.runValuation && (
          <p className="mt-3 text-sm text-gray-500">
            Economic valuation is not required. You can proceed directly to results, which will report
            health outcomes (attributable cases) without monetary values.
          </p>
        )}
      </div>

      {step7.runValuation && (
        <>
          {/* VSL Input */}
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5 mb-6">
            <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-1">
              Value of a Statistical Life (VSL)
            </h3>
            <p className="text-xs text-gray-400 mb-4">
              The EPA 2024 recommended central estimate is $11,800,000 in 2024 dollars.
            </p>
            <div className="max-w-xs">
              <label className="block text-sm font-medium text-gray-700 mb-1">VSL (USD, 2024$)</label>
              <div className="relative">
                <span className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 text-sm">$</span>
                <input
                  type="text"
                  inputMode="numeric"
                  value={formatInputCurrency(step7.vsl)}
                  onChange={handleVslChange}
                  className="w-full pl-7 pr-3 py-2 rounded-lg border border-gray-300 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none"
                />
              </div>
              {step7.vsl !== EPA_VSL_2024 && (
                <button
                  onClick={() => setStep7({ vsl: EPA_VSL_2024 })}
                  className="mt-1 text-xs text-blue-600 hover:text-blue-700"
                >
                  Reset to EPA default
                </button>
              )}
            </div>
          </div>

          {/* OECD Benefit Transfer */}
          {showBenefitTransfer && (
            <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5 mb-6">
              <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-1">
                OECD Benefit Transfer
              </h3>
              <p className="text-sm text-gray-500 mb-4">
                Because your study area ({step1.studyArea?.name}) is outside the United States, the VSL
                is adjusted using the OECD benefit-transfer method. This scales the U.S. VSL by the
                ratio of national incomes, raised to an income elasticity, to reflect differences in
                willingness to pay across countries.
              </p>

              {/* Formula */}
              <div className="bg-gray-50 rounded-lg p-4 mb-4 font-mono text-sm text-gray-700">
                <div className="mb-1">
                  VSL<sub>country</sub> = VSL<sub>US</sub> &times; (GNI<sub>country</sub> / GNI<sub>US</sub>)<sup>e</sup>
                </div>
                <div className="text-xs text-gray-400 mt-2 font-sans space-y-0.5">
                  <div>GNI<sub>US</sub> = ${US_GNI.toLocaleString()} (PPP, 2022)</div>
                  <div>GNI<sub>{step1.studyArea?.name}</sub> = ${countryGni.gniPcPpp.toLocaleString()} (PPP, 2022)</div>
                </div>
              </div>

              {/* Income elasticity */}
              <div className="max-w-xs mb-4">
                <label className="block text-sm font-medium text-gray-700 mb-1">Income Elasticity (e)</label>
                <select
                  value={step7.incomeElasticity ?? 1.0}
                  onChange={handleElasticityChange}
                  className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none"
                >
                  {ELASTICITY_OPTIONS.map((opt) => (
                    <option key={opt.value} value={opt.value}>{opt.label}</option>
                  ))}
                </select>
                <p className="text-xs text-gray-400 mt-1">
                  OECD recommends 1.0 as the central estimate. Lower values assume less-than-proportional scaling.
                </p>
              </div>

              {/* Transferred result */}
              <div className="bg-blue-50 rounded-lg p-4 border border-blue-100">
                <div className="flex justify-between items-baseline">
                  <span className="text-sm font-medium text-blue-800">Transferred VSL for {step1.studyArea?.name}</span>
                  <span className="text-lg font-bold text-blue-900">
                    {formatCurrency(transferredVsl)}
                  </span>
                </div>
                <p className="text-xs text-blue-600 mt-1">
                  = {formatCurrency(step7.vsl)} &times; ({countryGni.gniPcPpp.toLocaleString()} / {US_GNI.toLocaleString()})<sup>{step7.incomeElasticity ?? 1.0}</sup>
                </p>
              </div>
            </div>
          )}

          {/* Currency & Dollar Year */}
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5 mb-6">
            <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-4">
              Currency &amp; Dollar Year
            </h3>
            <div className="grid gap-6 md:grid-cols-2">
              {/* Currency */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Currency</label>
                <select
                  value={step7.currency}
                  onChange={handleCurrencyChange}
                  className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none"
                >
                  {currencyOptions.map((opt) => (
                    <option key={opt.value} value={opt.value}>{opt.label}</option>
                  ))}
                </select>
              </div>

              {/* Dollar year */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Dollar Year</label>
                <select
                  value={step7.dollarYear}
                  onChange={handleDollarYearChange}
                  className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none"
                >
                  {DOLLAR_YEARS.map((yr) => (
                    <option key={yr} value={yr}>{yr}</option>
                  ))}
                </select>
                <p className="text-xs text-gray-400 mt-1">GDP deflator applied relative to 2024 base year</p>
              </div>
            </div>

            {/* Adjusted VSL summary */}
            {adjustedVsl != null && (
              <div className="mt-4 pt-4 border-t border-gray-100">
                <div className="flex justify-between items-baseline text-sm">
                  <span className="text-gray-500">VSL in {step7.dollarYear} dollars</span>
                  <span className="font-semibold text-gray-900">{formatCurrency(adjustedVsl)}</span>
                </div>
                {step7.currency !== 'USD' && localCurrencyVsl != null && (
                  <div className="flex justify-between items-baseline text-sm mt-1">
                    <span className="text-gray-500">VSL in {step7.currency} (PPP)</span>
                    <span className="font-semibold text-gray-900">
                      {formatCurrency(localCurrencyVsl, step7.currency)}
                    </span>
                  </div>
                )}
              </div>
            )}
          </div>
        </>
      )}
    </>
  )
}
