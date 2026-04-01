import { useState, useRef, useCallback } from 'react'
import { Link } from 'react-router-dom'
import Papa from 'papaparse'
import { jsPDF } from 'jspdf'
import html2canvas from 'html2canvas'
import useAnalysisStore from '../stores/useAnalysisStore'
import ResultsTable from '../components/ResultsTable'

// ── Formatting helpers ──────────────────────────────────────────

function fmtNumber(n, decimals = 0) {
  if (n == null) return '—'
  return Number(n).toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  })
}

function fmtPercent(n) {
  if (n == null) return '—'
  return `${(Number(n) * 100).toFixed(1)}%`
}

function fmtCurrency(n) {
  if (n == null) return '—'
  if (Math.abs(n) >= 1e9) return `$${(n / 1e9).toFixed(2)}B`
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(1)}M`
  return `$${fmtNumber(n)}`
}

function fmtRate(n) {
  if (n == null) return '—'
  return fmtNumber(n, 1)
}

function slugify(name) {
  return (name || 'hia-analysis')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/(^-|-$)/g, '')
}

function triggerDownload(blob, filename) {
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.click()
  URL.revokeObjectURL(url)
}

// ── Summary Card ────────────────────────────────────────────────

function SummaryCard({ label, value, ci, subtitle, bgClass }) {
  return (
    <div className={`rounded-2xl p-6 shadow-sm ${bgClass}`}>
      <p className="text-sm font-medium text-gray-500 mb-1">{label}</p>
      <p className="text-3xl font-bold text-gray-900 leading-tight">{value}</p>
      {ci && (
        <p className="text-sm text-gray-500 mt-1">
          95% CI: {ci}
        </p>
      )}
      {subtitle && (
        <p className="text-xs text-gray-400 mt-2">{subtitle}</p>
      )}
    </div>
  )
}

// ── Tab: Map placeholder ────────────────────────────────────────

function MapTab() {
  return (
    <div className="flex items-center justify-center h-80 rounded-xl border-2 border-dashed border-gray-200 bg-gray-50">
      <div className="text-center">
        <svg className="mx-auto h-12 w-12 text-gray-300 mb-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 20l-5.447-2.724A1 1 0 013 16.382V5.618a1 1 0 011.447-.894L9 7m0 13l6-3m-6 3V7m6 10l5.447 2.724A1 1 0 0021 18.382V7.618a1 1 0 00-.553-.894L15 4m0 13V4m0 0L9 7" />
        </svg>
        <p className="text-gray-400 font-medium">Spatial map available for gridded analyses</p>
        <p className="text-xs text-gray-300 mt-1">Run a gridded analysis to generate spatial results</p>
      </div>
    </div>
  )
}

// ── Tab: Results Table ──────────────────────────────────────────

function TableTab({ results, hasValuation }) {
  const rows = results?.detail ?? []
  const hasSpatialUnits = rows.some((r) => r.spatialUnit != null)

  return (
    <ResultsTable
      rows={rows}
      hasValuation={hasValuation}
      hasSpatialUnits={hasSpatialUnits}
    />
  )
}

// ── Tab: Trend placeholder ──────────────────────────────────────

function TrendTab() {
  return (
    <div className="flex items-center justify-center h-80 rounded-xl border-2 border-dashed border-gray-200 bg-gray-50">
      <div className="text-center">
        <svg className="mx-auto h-12 w-12 text-gray-300 mb-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z" />
        </svg>
        <p className="text-gray-400 font-medium">Multi-year trend chart</p>
        <p className="text-xs text-gray-300 mt-1">Available when analyzing multiple years</p>
      </div>
    </div>
  )
}

// ── Save Template Modal ─────────────────────────────────────────

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

// ── Export button card ───────────────────────────────────────────

function ExportButton({ icon, label, description, onClick, disabled, busy, colorClass = 'text-blue-500', hoverClass = 'hover:border-blue-300 hover:bg-blue-50' }) {
  return (
    <button
      onClick={onClick}
      disabled={disabled || busy}
      className={`flex flex-col items-center gap-2 p-6 rounded-xl border border-gray-200 ${hoverClass} transition-colors disabled:opacity-40 disabled:cursor-not-allowed`}
    >
      {busy ? (
        <svg className="animate-spin h-8 w-8 text-gray-400" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
        </svg>
      ) : (
        <svg className={`h-8 w-8 ${colorClass}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
          {icon}
        </svg>
      )}
      <span className="font-medium text-gray-700">{busy ? 'Generating...' : label}</span>
      <span className="text-xs text-gray-400">{description}</span>
    </button>
  )
}

// ── Tab: Export ──────────────────────────────────────────────────

const CSV_COLUMNS = [
  { key: 'crfStudy',            header: 'CRF Study' },
  { key: 'framework',           header: 'Framework' },
  { key: 'endpoint',            header: 'Endpoint' },
  { key: 'spatialUnit',         header: 'Spatial Unit' },
  { key: 'attributableCases',   header: 'Attributable Cases (mean)' },
  { key: 'lower95',             header: 'Lower 95% CI' },
  { key: 'upper95',             header: 'Upper 95% CI' },
  { key: 'attributableFraction', header: 'Attributable Fraction' },
  { key: 'ratePer100k',         header: 'Rate per 100,000' },
  { key: 'economicValue',       header: 'Economic Value' },
]

function ExportTab({ results, analysisName, hasValuation, summaryRef, tableRef, step1, step6, step7, exportConfig, onOpenTemplateModal }) {
  const [pdfBusy, setPdfBusy] = useState(false)

  const slug = slugify(analysisName)
  const rows = results?.detail ?? []
  const hasSpatialUnits = rows.some((r) => r.spatialUnit != null)

  // ── 1. Download CSV (Papaparse) ─────────────────────────────

  const handleDownloadCSV = useCallback(() => {
    if (rows.length === 0) return

    const visibleCols = CSV_COLUMNS.filter((col) => {
      if (col.key === 'economicValue' && !hasValuation) return false
      if (col.key === 'spatialUnit' && !hasSpatialUnits) return false
      return true
    })

    const data = rows.map((row) => {
      const obj = {}
      for (const col of visibleCols) {
        obj[col.header] = row[col.key] ?? ''
      }
      return obj
    })

    const csv = Papa.unparse(data)
    triggerDownload(
      new Blob([csv], { type: 'text/csv;charset=utf-8' }),
      `${slug}-results.csv`,
    )
  }, [rows, hasValuation, hasSpatialUnits, slug])

  // ── 2. Download PDF Report ──────────────────────────────────

  const handleDownloadPDF = useCallback(async () => {
    setPdfBusy(true)
    try {
      const pdf = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' })
      const pageW = pdf.internal.pageSize.getWidth()
      const pageH = pdf.internal.pageSize.getHeight()
      const margin = 20

      // — Title page —
      pdf.setFontSize(28)
      pdf.setTextColor(15, 23, 42) // slate-900
      pdf.text(analysisName || 'HIA Analysis Report', margin, 50)

      pdf.setFontSize(12)
      pdf.setTextColor(100, 116, 139) // slate-500
      pdf.text(`Generated: ${new Date().toLocaleDateString()}`, margin, 65)

      // Key parameters
      let y = 90
      pdf.setFontSize(14)
      pdf.setTextColor(15, 23, 42)
      pdf.text('Analysis Parameters', margin, y)
      y += 10

      pdf.setFontSize(10)
      pdf.setTextColor(71, 85, 105) // slate-600
      const params = [
        ['Study Area', step1?.studyArea?.name || '—'],
        ['Pollutant', step1?.pollutant || '—'],
        ['Years', step1?.years ? (step1.years.start === step1.years.end ? String(step1.years.start) : `${step1.years.start}–${step1.years.end}`) : '—'],
        ['Pooling Method', step6?.poolingMethod || '—'],
        ['Monte Carlo Iterations', String(step6?.monteCarloIterations ?? '—')],
      ]
      if (hasValuation) {
        params.push(
          ['VSL', `$${(step7?.vsl ?? 0).toLocaleString()}`],
          ['Currency / Year', `${step7?.currency ?? '—'} ${step7?.dollarYear ?? '—'}`],
        )
      }
      for (const [label, value] of params) {
        pdf.setFont(undefined, 'bold')
        pdf.text(`${label}:`, margin, y)
        pdf.setFont(undefined, 'normal')
        pdf.text(value, margin + 55, y)
        y += 7
      }

      // — Summary cards capture —
      if (summaryRef.current) {
        pdf.addPage()
        pdf.setFontSize(16)
        pdf.setTextColor(15, 23, 42)
        pdf.text('Summary', margin, 25)

        const summaryCanvas = await html2canvas(summaryRef.current, {
          scale: 2,
          useCORS: true,
          backgroundColor: '#f8fafc',
        })
        const summaryImg = summaryCanvas.toDataURL('image/png')
        const imgW = pageW - margin * 2
        const imgH = (summaryCanvas.height / summaryCanvas.width) * imgW
        pdf.addImage(summaryImg, 'PNG', margin, 35, imgW, Math.min(imgH, pageH - 55))
      }

      // — Table capture —
      if (tableRef.current) {
        pdf.addPage()
        pdf.setFontSize(16)
        pdf.setTextColor(15, 23, 42)
        pdf.text('Detailed Results by CRF', margin, 25)

        const tableCanvas = await html2canvas(tableRef.current, {
          scale: 2,
          useCORS: true,
          backgroundColor: '#ffffff',
        })
        const tableImg = tableCanvas.toDataURL('image/png')
        const imgW = pageW - margin * 2
        const imgH = (tableCanvas.height / tableCanvas.width) * imgW

        // If the table image is taller than one page, scale to fit width and let it clip
        // A production version would paginate; for now, fit what we can.
        const maxImgH = pageH - 45
        const finalH = Math.min(imgH, maxImgH)
        pdf.addImage(tableImg, 'PNG', margin, 35, imgW, finalH)

        if (imgH > maxImgH) {
          pdf.setFontSize(9)
          pdf.setTextColor(148, 163, 184)
          pdf.text('Table truncated — download CSV for full data.', margin, pageH - 10)
        }
      }

      pdf.save(`${slug}-report.pdf`)
    } catch (err) {
      console.error('PDF generation failed:', err)
      alert('Failed to generate PDF. Please try again.')
    } finally {
      setPdfBusy(false)
    }
  }, [analysisName, hasValuation, step1, step6, step7, summaryRef, tableRef, slug])

  // ── 3. Download JSON Config ─────────────────────────────────

  const handleDownloadConfig = useCallback(() => {
    const config = exportConfig()
    const payload = {
      _format: 'hia-analysis-config',
      _version: 1,
      _exportedAt: new Date().toISOString(),
      analysisName: analysisName || null,
      ...config,
    }
    triggerDownload(
      new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' }),
      `${slug}-config.json`,
    )
  }, [exportConfig, analysisName, slug])

  // ── Render ──────────────────────────────────────────────────

  return (
    <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-4">
      <ExportButton
        label="Download CSV"
        description="Results table via Papaparse"
        disabled={rows.length === 0}
        colorClass="text-teal-500"
        hoverClass="hover:border-teal-300 hover:bg-teal-50"
        onClick={handleDownloadCSV}
        icon={<path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />}
      />

      <ExportButton
        label="Download PDF Report"
        description="Summary cards, parameters & table"
        busy={pdfBusy}
        colorClass="text-blue-500"
        hoverClass="hover:border-blue-300 hover:bg-blue-50"
        onClick={handleDownloadPDF}
        icon={<path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z" />}
      />

      <ExportButton
        label="Download JSON Config"
        description="Reproducibility file"
        colorClass="text-blue-400"
        hoverClass="hover:border-blue-300 hover:bg-blue-50"
        onClick={handleDownloadConfig}
        icon={<path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" />}
      />

      <ExportButton
        label="Save as Template"
        description="Reuse this configuration"
        colorClass="text-teal-600"
        hoverClass="hover:border-teal-300 hover:bg-teal-50"
        onClick={onOpenTemplateModal}
        icon={<path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M5 5a2 2 0 012-2h10a2 2 0 012 2v16l-7-3.5L5 21V5z" />}
      />
    </div>
  )
}

// ── Tabs ────────────────────────────────────────────────────────

const TABS = [
  { key: 'map', label: 'Map' },
  { key: 'table', label: 'Table' },
  { key: 'trend', label: 'Trend' },
  { key: 'export', label: 'Export' },
]

// ── Main Page ───────────────────────────────────────────────────

export default function Results() {
  const { results, step1, step6, step7, exportConfig } = useAnalysisStore()
  const [activeTab, setActiveTab] = useState('table')
  const [templateModal, setTemplateModal] = useState(false)
  const [savingTemplate, setSavingTemplate] = useState(false)
  const [templateSaved, setTemplateSaved] = useState(false)

  const summaryRef = useRef(null)
  const tableRef = useRef(null)

  const summary = results?.summary ?? {}
  const hasValuation = step7?.runValuation && summary.economicValue != null
  const analysisName = results?.meta?.analysisName || step1?.analysisName || ''

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

  return (
    <div className="min-h-screen bg-gradient-to-b from-slate-50 to-white">
      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-10">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="text-3xl font-bold text-slate-900">Analysis Results</h1>
            {analysisName && (
              <p className="text-slate-500 mt-1">{analysisName}</p>
            )}
          </div>
          <div className="flex gap-3">
            {templateSaved && (
              <span className="flex items-center gap-1 text-sm text-teal-600 font-medium animate-pulse">
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
                Template saved
              </span>
            )}
            <Link
              to="/analysis/7"
              className="px-5 py-2 rounded-lg border border-gray-300 text-gray-700 hover:bg-gray-100 transition-colors text-sm font-medium"
            >
              Back to Wizard
            </Link>
            <Link
              to="/"
              className="px-5 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 transition-colors text-sm font-medium"
            >
              New Analysis
            </Link>
          </div>
        </div>

        {/* No results state */}
        {!results ? (
          <div className="bg-white rounded-2xl shadow-sm p-16 text-center">
            <svg className="mx-auto h-16 w-16 text-gray-200 mb-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
            </svg>
            <p className="text-gray-400 text-lg font-medium">No results yet</p>
            <p className="text-gray-300 text-sm mt-1">Complete the analysis wizard to see results here.</p>
            <Link
              to="/analysis/1"
              className="inline-block mt-6 px-6 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 transition-colors text-sm font-medium"
            >
              Start Analysis
            </Link>
          </div>
        ) : (
          <>
            {/* Summary Cards */}
            <div
              ref={summaryRef}
              className={`grid gap-4 mb-8 ${hasValuation ? 'sm:grid-cols-2 lg:grid-cols-4' : 'sm:grid-cols-3'}`}
            >
              <SummaryCard
                label="Total Attributable Deaths"
                value={fmtNumber(summary.totalDeaths?.mean)}
                ci={
                  summary.totalDeaths
                    ? `${fmtNumber(summary.totalDeaths.lower95)} – ${fmtNumber(summary.totalDeaths.upper95)}`
                    : null
                }
                bgClass="bg-blue-50"
              />
              <SummaryCard
                label="Attributable Fraction"
                value={fmtPercent(summary.attributableFraction)}
                subtitle="Share of deaths attributable to exposure"
                bgClass="bg-teal-50"
              />
              <SummaryCard
                label="Attributable Rate"
                value={fmtRate(summary.attributableRate)}
                subtitle="Per 100,000 population"
                bgClass="bg-blue-50"
              />
              {hasValuation && (
                <SummaryCard
                  label="Economic Value"
                  value={fmtCurrency(summary.economicValue)}
                  subtitle={`VSL-based valuation (${step7.currency} ${step7.dollarYear})`}
                  bgClass="bg-teal-50"
                />
              )}
            </div>

            {/* Tabs */}
            <div className="bg-white rounded-2xl shadow-sm overflow-hidden">
              <div className="border-b border-gray-200">
                <nav className="flex -mb-px">
                  {TABS.map((tab) => (
                    <button
                      key={tab.key}
                      onClick={() => setActiveTab(tab.key)}
                      className={`px-6 py-3 text-sm font-medium border-b-2 transition-colors ${
                        activeTab === tab.key
                          ? 'border-blue-600 text-blue-600'
                          : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                      }`}
                    >
                      {tab.label}
                    </button>
                  ))}
                </nav>
              </div>

              <div className="p-6">
                {activeTab === 'map' && <MapTab />}
                {activeTab === 'table' && (
                  <div ref={tableRef}>
                    <TableTab results={results} hasValuation={hasValuation} />
                  </div>
                )}
                {activeTab === 'trend' && <TrendTab />}
                {activeTab === 'export' && (
                  <ExportTab
                    results={results}
                    analysisName={analysisName}
                    hasValuation={hasValuation}
                    summaryRef={summaryRef}
                    tableRef={tableRef}
                    step1={step1}
                    step6={step6}
                    step7={step7}
                    exportConfig={exportConfig}
                    onOpenTemplateModal={() => setTemplateModal(true)}
                  />
                )}
              </div>
            </div>
          </>
        )}
      </div>

      {/* Save Template Modal */}
      <SaveTemplateModal
        open={templateModal}
        onClose={() => setTemplateModal(false)}
        onSave={handleSaveTemplate}
        saving={savingTemplate}
      />
    </div>
  )
}
