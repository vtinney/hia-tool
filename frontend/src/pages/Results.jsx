import { useState, useRef, useCallback, useMemo, useEffect } from 'react'
import { Link } from 'react-router-dom'
import Papa from 'papaparse'
import { jsPDF } from 'jspdf'
import html2canvas from 'html2canvas'
import useAnalysisStore from '../stores/useAnalysisStore'
import ResultsTable from '../components/ResultsTable'
import CompareAnotherYearCard from '../components/CompareAnotherYearCard'
import AdditionalRunSummary from '../components/AdditionalRunSummary'
import { runAnalysisForYear } from '../lib/api'

// ── Formatting helpers ─────────────────────────────────────────
function fmtNumber(n, decimals = 0) {
  if (n == null) return '—'
  return Number(n).toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  })
}
function fmtCompact(n) {
  if (n == null) return '—'
  if (Math.abs(n) >= 1e9) return `${(n / 1e9).toFixed(2)}B`
  if (Math.abs(n) >= 1e6) return `${(n / 1e6).toFixed(2)}M`
  if (Math.abs(n) >= 1e3) return `${(n / 1e3).toFixed(1)}k`
  return fmtNumber(n)
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
function slugify(name) {
  return (name || 'hia-analysis').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '')
}
function triggerDownload(blob, filename) {
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.click()
  URL.revokeObjectURL(url)
}

// ── Inline icons ──────────────────────────────────────────────
function ChevronLeft(props) {
  return <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" {...props}><path d="M10 3.5L5.5 8l4.5 4.5" /></svg>
}
function ArrowRight(props) {
  return <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" {...props}><path d="M3.5 8h9M9 4.5l3.5 3.5L9 11.5" /></svg>
}
function CheckIcon(props) {
  return <svg viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" {...props}><path d="M2.5 6.5L5 9l4.5-5.5" /></svg>
}

// ── Count-up hook (used by hero number) ─────────────────────
function useCountUp(target, duration = 1100) {
  const [value, setValue] = useState(0)
  useEffect(() => {
    if (target == null || isNaN(target)) return
    if (window.matchMedia?.('(prefers-reduced-motion: reduce)').matches) {
      setValue(target)
      return
    }
    let raf
    const start = performance.now()
    const tick = (now) => {
      const t = Math.min(1, (now - start) / duration)
      // Strong ease-out (cubic-bezier(0.23, 1, 0.32, 1)) approximation
      const eased = 1 - Math.pow(1 - t, 3)
      setValue(target * eased)
      if (t < 1) raf = requestAnimationFrame(tick)
    }
    raf = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(raf)
  }, [target, duration])
  return value
}

// ── Hero: massive number + CI bar ──────────────────────────
function HeroNumber({ totalDeaths, detailRows = [], isSpatial, zoneCount }) {
  // When the pooled total isn't available (the default now that the
  // pooling UI is removed), fall back to the highest-impact CRF so the
  // hero is still a real number, not a placeholder sentence.
  const isPooled = totalDeaths != null
  const headlineCRF = useMemo(() => {
    if (isPooled || !detailRows?.length) return null
    return [...detailRows].sort(
      (a, b) => (Number(b.attributableCases) || 0) - (Number(a.attributableCases) || 0),
    )[0]
  }, [isPooled, detailRows])

  const mean = isPooled
    ? totalDeaths.mean
    : (headlineCRF ? Number(headlineCRF.attributableCases) || 0 : 0)
  const lower = isPooled
    ? totalDeaths.lower95
    : (headlineCRF?.lower95 ?? null)
  const upper = isPooled
    ? totalDeaths.upper95
    : (headlineCRF?.upper95 ?? null)
  const animatedMean = useCountUp(mean)
  const hasNumber = isPooled || headlineCRF != null
  const showCI = lower != null && upper != null && Number.isFinite(lower) && Number.isFinite(upper)

  // Empty state: no detail rows at all (run failed or hasn't completed).
  if (!hasNumber) {
    return (
      <div className="surface p-8 lg:p-10">
        <div className="flex items-baseline justify-between mb-6">
          <p className="eyebrow">Headline result</p>
          <p className="font-mono text-[10px] tracking-[0.12em] uppercase text-zinc-400">
            No results
          </p>
        </div>
        <p className="text-[14px] text-zinc-500 max-w-prose">
          No CRF returned a numeric result for this run.
        </p>
      </div>
    )
  }

  // CI bar geometry. The bracket spans the full width (with a small
  // margin) so the visual is dominated by the CI rather than by an
  // arbitrary axis. The point-estimate marker sits visually centered
  // between the two brackets — the precise numeric values are shown
  // in the labels underneath, so the bar's job is to communicate the
  // *width* of the interval, not the absolute scale.
  const lowerPct = 6
  const upperPct = 94
  const widthPct = upperPct - lowerPct
  const meanPct = (lowerPct + upperPct) / 2

  return (
    <div className="surface p-8 lg:p-10">
      <div className="flex items-baseline justify-between mb-6">
        <p className="eyebrow">Headline result</p>
        <p className="font-mono text-[10px] tracking-[0.12em] uppercase text-zinc-400">
          {isPooled
            ? 'Attributable deaths · 95% CI · analytical'
            : 'Per-CRF · top endpoint · 95% CI · analytical'}
        </p>
      </div>

      <div className="flex flex-col lg:flex-row lg:items-end gap-8 lg:gap-12">
        {/* Massive mean */}
        <div className="lg:flex-1">
          <p className="font-mono font-medium text-ink leading-[0.85] tracking-tightest tabular-nums text-[88px] md:text-[112px] lg:text-[128px]">
            {fmtNumber(Math.round(animatedMean))}
          </p>
          <p className="mt-7 text-[15px] leading-relaxed text-zinc-600">
            attributable cases
            {!isPooled && headlineCRF && (
              <>
                <span className="text-zinc-400"> · </span>
                <span className="font-medium text-zinc-800">{headlineCRF.endpoint}</span>
                {headlineCRF.crfStudy && (
                  <span className="text-zinc-500"> ({headlineCRF.crfStudy})</span>
                )}
              </>
            )}
            {isSpatial && zoneCount ? (
              <span className="text-zinc-400"> · across {zoneCount.toLocaleString()} zones</span>
            ) : null}
          </p>
          {!isPooled && detailRows.length > 1 && (
            <p className="mt-2 text-[12px] text-zinc-400">
              {detailRows.length} CRFs analyzed — see breakdown below
            </p>
          )}
        </div>

        {/* CI bracket bar — only when bounds are present */}
        {showCI && (
          <div className="lg:flex-1 lg:pb-3">
            <div className="relative h-12">
              {/* baseline */}
              <div className="absolute inset-x-0 top-1/2 -translate-y-px h-px bg-zinc-200" />

              {/* CI band */}
              <div
                className="absolute top-1/2 -translate-y-1/2 h-2 rounded-full hia-bar-grow"
                style={{
                  left: `${lowerPct}%`,
                  width: `${widthPct}%`,
                  background:
                    'linear-gradient(90deg, rgba(21,88,82,0.18), rgba(21,88,82,0.42), rgba(21,88,82,0.18))',
                  boxShadow: 'inset 0 0 0 1px rgba(21,88,82,0.25)',
                }}
              />

              {/* Lower bracket */}
              <div className="absolute top-1/2 -translate-y-1/2 flex flex-col items-center" style={{ left: `${lowerPct}%` }}>
                <div className="h-5 w-px bg-accent-700" />
              </div>

              {/* Upper bracket */}
              <div className="absolute top-1/2 -translate-y-1/2 flex flex-col items-center" style={{ left: `${upperPct}%` }}>
                <div className="h-5 w-px bg-accent-700" />
              </div>

              {/* Mean marker */}
              <div className="absolute top-1/2 -translate-y-1/2 -translate-x-1/2" style={{ left: `${meanPct}%` }}>
                <div className="h-7 w-[3px] bg-ink rounded-full" />
              </div>
            </div>

            {/* CI labels */}
            <div className="mt-3 flex items-baseline justify-between font-mono text-[11px] tabular-nums">
              <div>
                <span className="text-zinc-400 uppercase tracking-[0.12em] mr-2 text-[9.5px]">Lower</span>
                <span className="text-zinc-600">{fmtNumber(lower)}</span>
              </div>
              <div className="text-center">
                <span className="text-zinc-400 uppercase tracking-[0.12em] mr-2 text-[9.5px]">Point estimate</span>
                <span className="text-ink">{fmtNumber(mean)}</span>
              </div>
              <div className="text-right">
                <span className="text-zinc-400 uppercase tracking-[0.12em] mr-2 text-[9.5px]">Upper</span>
                <span className="text-zinc-600">{fmtNumber(upper)}</span>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

// ── Secondary stats row ───────────────────────────────────
function SecondaryStat({ label, value, sub }) {
  return (
    <div className="border-t border-zinc-200/80 pt-5">
      <p className="font-mono text-[10px] uppercase tracking-[0.14em] text-zinc-400 mb-2">{label}</p>
      <p className="font-mono text-[28px] tracking-tight text-ink tabular-nums leading-none">{value}</p>
      {sub && <p className="mt-2 text-[12px] text-zinc-500 leading-snug">{sub}</p>}
    </div>
  )
}

// ── Endpoint breakdown chart (hand-rolled SVG) ─────────────
// Horizontal bars sorted by point-estimate attributable cases.
// Each bar's length is proportional to the point estimate alone —
// the count to the right is the absolute number, not a percentage
// of any total. (CI bounds are not displayed here; they live in the
// detail table below for analysts who need them.)
function EndpointBreakdown({ rows = [] }) {
  const data = useMemo(() => {
    // One bar per endpoint, using the first row seen for that endpoint.
    // When a user selects multiple CRFs for the same endpoint (e.g.
    // Turner + GBD for all-cause mortality), summing would double-count
    // the same attributable burden.
    const seen = new Map()
    for (const r of rows) {
      const k = r.endpoint || 'Unknown'
      if (seen.has(k)) continue
      seen.set(k, { endpoint: k, mean: Number(r.attributableCases) || 0 })
    }
    return Array.from(seen.values())
      .filter((d) => d.mean > 0)
      .sort((a, b) => b.mean - a.mean)
      .slice(0, 8)
  }, [rows])

  if (data.length === 0) return null

  const maxMean = Math.max(...data.map((d) => d.mean)) || 1

  return (
    <div className="surface p-7 lg:p-8">
      <div className="flex items-baseline justify-between mb-6">
        <p className="eyebrow">By endpoint</p>
        <p className="font-mono text-[10px] tracking-[0.12em] uppercase text-zinc-400 tabular-nums">
          Top {data.length} · attributable cases
        </p>
      </div>

      <ul className="space-y-5">
        {data.map((d, i) => {
          const widthPct = (d.mean / maxMean) * 100
          return (
            <li
              key={d.endpoint}
              className="hia-rise"
              style={{ '--i': i + 1 }}
            >
              <div className="flex items-baseline justify-between mb-2">
                <span className="text-[13px] text-ink truncate pr-3">{d.endpoint}</span>
                <span className="font-mono text-[13px] text-ink tabular-nums shrink-0">
                  {fmtNumber(d.mean)}
                </span>
              </div>
              <div className="relative h-2.5">
                <div className="absolute inset-y-0 left-0 right-0 bg-zinc-100 rounded-full" />
                <div
                  className="absolute top-0 bottom-0 bg-accent-700 rounded-full hia-bar-grow"
                  style={{ left: 0, width: `${widthPct}%` }}
                />
              </div>
            </li>
          )
        })}
      </ul>
    </div>
  )
}

// ── Map placeholder (zone summary) ─────────────────────────
function MapTab({ zones }) {
  if (!zones || zones.length === 0) {
    return (
      <div className="border border-dashed border-zinc-200 rounded-2xl py-20 text-center">
        <p className="font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-400">
          Spatial map available for gridded analyses
        </p>
        <p className="mt-2 text-[13px] text-zinc-500">
          Run a gridded analysis to generate spatial results.
        </p>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="border border-dashed border-zinc-200 rounded-2xl py-16 text-center">
        <p className="font-mono text-[11px] uppercase tracking-[0.14em] text-accent-700">
          Choropleth rendering coming soon
        </p>
        <p className="mt-2 text-[13px] text-zinc-500 tabular-nums">
          {zones.length} zones with spatial results available
        </p>
      </div>

      <div className="overflow-x-auto border-y border-zinc-200/80">
        <table className="w-full text-[13px]">
          <thead>
            <tr className="border-b border-zinc-200/80">
              {['Zone', 'Population', 'Baseline conc.', 'Control conc.', 'Attr. cases', '95% CI'].map((h, i) => (
                <th key={h} className={`px-3 py-3 font-mono text-[10px] uppercase tracking-[0.12em] text-zinc-500 ${i === 0 ? 'text-left' : 'text-right'}`}>
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-100">
            {zones.map((zone) => {
              const totalCases = zone.results?.reduce((s, r) => s + (r.attributableCases?.mean || 0), 0) || 0
              const totalLower = zone.results?.reduce((s, r) => s + (r.attributableCases?.lower95 || 0), 0) || 0
              const totalUpper = zone.results?.reduce((s, r) => s + (r.attributableCases?.upper95 || 0), 0) || 0
              return (
                <tr key={zone.zoneId} className="hover:bg-zinc-50/60 transition-colors">
                  <td className="px-3 py-2.5 text-ink">{zone.zoneName || zone.zoneId}</td>
                  <td className="px-3 py-2.5 text-right font-mono tabular-nums text-zinc-700">{fmtNumber(zone.population)}</td>
                  <td className="px-3 py-2.5 text-right font-mono tabular-nums text-zinc-700">{fmtNumber(zone.baselineConcentration, 1)}</td>
                  <td className="px-3 py-2.5 text-right font-mono tabular-nums text-zinc-700">{fmtNumber(zone.controlConcentration, 1)}</td>
                  <td className="px-3 py-2.5 text-right font-mono tabular-nums text-ink">{fmtNumber(totalCases)}</td>
                  <td className="px-3 py-2.5 text-right font-mono tabular-nums text-zinc-500 text-[11.5px]">
                    {fmtNumber(totalLower)} – {fmtNumber(totalUpper)}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function TableTab({ results, hasValuation }) {
  const rows = results?.detail ?? []
  const hasSpatialUnits = rows.some((r) => r.spatialUnit != null)
  return <ResultsTable rows={rows} hasValuation={hasValuation} hasSpatialUnits={hasSpatialUnits} />
}

function TrendTab() {
  return (
    <div className="border border-dashed border-zinc-200 rounded-2xl py-20 text-center">
      <p className="font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-400">
        Multi-year trend chart
      </p>
      <p className="mt-2 text-[13px] text-zinc-500">
        Available when analyzing multiple years.
      </p>
    </div>
  )
}

// ── Save Template Modal ─────────────────────────────────────
function SaveTemplateModal({ open, onClose, onSave, saving }) {
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')

  if (!open) return null

  const handleSave = () => {
    if (!name.trim()) return
    onSave({ name: name.trim(), description: description.trim() })
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-ink/40 backdrop-blur-sm p-4">
      <div className="surface w-full max-w-md p-7">
        <p className="eyebrow mb-2">Reusable</p>
        <h3 className="text-[20px] font-medium tracking-tight text-ink mb-5">Save as template</h3>
        <div className="space-y-4">
          <div>
            <label className="block font-mono text-[10px] uppercase tracking-[0.12em] text-zinc-500 mb-1.5">Template name</label>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. PM₂.₅ US standard analysis"
              className="input"
            />
          </div>
          <div>
            <label className="block font-mono text-[10px] uppercase tracking-[0.12em] text-zinc-500 mb-1.5">Description (optional)</label>
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              rows={3}
              placeholder="Brief description of this configuration…"
              className="input resize-none"
            />
          </div>
        </div>
        <div className="flex justify-end gap-3 mt-6">
          <button onClick={onClose} className="btn-ghost">Cancel</button>
          <button onClick={handleSave} disabled={!name.trim() || saving} className="btn-accent">
            {saving ? 'Saving…' : 'Save template'}
          </button>
        </div>
      </div>
    </div>
  )
}

// ── Export tile ─────────────────────────────────────────────
function ExportTile({ kicker, label, description, onClick, disabled, busy }) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled || busy}
      className="group text-left p-6 border border-zinc-200/80 rounded-2xl bg-white hover:border-accent-300 transition-colors duration-200 ease-out disabled:opacity-40 disabled:cursor-not-allowed"
    >
      <p className="font-mono text-[10px] uppercase tracking-[0.14em] text-zinc-400 mb-3">
        {busy ? 'Generating…' : kicker}
      </p>
      <p className="text-[15px] font-medium text-ink mb-1.5">{label}</p>
      <p className="text-[12.5px] text-zinc-500 leading-relaxed">{description}</p>
      <span className="mt-4 inline-flex items-center gap-1 text-[12px] text-accent-700 group-hover:gap-1.5 transition-all duration-200 ease-out">
        Download
        <ArrowRight className="w-3 h-3" />
      </span>
    </button>
  )
}

const CSV_COLUMNS = [
  { key: 'crfStudy',             header: 'CRF Study' },
  { key: 'framework',            header: 'Framework' },
  { key: 'endpoint',             header: 'Endpoint' },
  { key: 'spatialUnit',          header: 'Spatial Unit' },
  { key: 'attributableCases',    header: 'Attributable Cases (mean)' },
  { key: 'lower95',              header: 'Lower 95% CI' },
  { key: 'upper95',              header: 'Upper 95% CI' },
  { key: 'attributableFraction', header: 'Attributable Fraction' },
  { key: 'ratePer100k',          header: 'Rate per 100,000' },
  { key: 'economicValue',        header: 'Economic Value' },
]

function ExportTab({ results, analysisName, hasValuation, summaryRef, tableRef, step1, step2, step6, step7, exportConfig, onOpenTemplateModal }) {
  const [pdfBusy, setPdfBusy] = useState(false)
  const slug = slugify(analysisName)
  const rows = results?.detail ?? []
  const hasSpatialUnits = rows.some((r) => r.spatialUnit != null)

  const handleDownloadCSV = useCallback(() => {
    if (rows.length === 0) return
    const visibleCols = CSV_COLUMNS.filter((col) => {
      if (col.key === 'economicValue' && !hasValuation) return false
      if (col.key === 'spatialUnit' && !hasSpatialUnits) return false
      return true
    })
    const data = rows.map((row) => {
      const obj = {}
      for (const col of visibleCols) obj[col.header] = row[col.key] ?? ''
      return obj
    })
    const csv = Papa.unparse(data)
    triggerDownload(new Blob([csv], { type: 'text/csv;charset=utf-8' }), `${slug}-results.csv`)
  }, [rows, hasValuation, hasSpatialUnits, slug])

  const handleDownloadPDF = useCallback(async () => {
    setPdfBusy(true)
    try {
      const pdf = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' })
      const pageW = pdf.internal.pageSize.getWidth()
      const pageH = pdf.internal.pageSize.getHeight()
      const margin = 20

      pdf.setFontSize(28)
      pdf.setTextColor(15, 23, 42)
      pdf.text(analysisName || 'HIA Analysis Report', margin, 50)
      pdf.setFontSize(12)
      pdf.setTextColor(100, 116, 139)
      pdf.text(`Generated: ${new Date().toLocaleDateString()}`, margin, 65)

      let y = 90
      pdf.setFontSize(14)
      pdf.setTextColor(15, 23, 42)
      pdf.text('Analysis Parameters', margin, y)
      y += 10
      pdf.setFontSize(10)
      pdf.setTextColor(71, 85, 105)
      const params = [
        ['Study Area', step1?.studyArea?.name || '—'],
        ['Pollutant', step1?.pollutant || '—'],
        ['Year', step2?.baseline?.year ?? '—'],
        ['Uncertainty Method', 'Analytical 95% CI'],
      ]
      if (hasValuation) {
        params.push(
          ['VSL', `$${(step7?.vsl ?? 0).toLocaleString()}`],
          ['Currency / Year', `${step7?.currency ?? '—'} ${step7?.dollarYear ?? '—'}`],
        )
      }
      for (const [label, value] of params) {
        pdf.setFont(undefined, 'bold'); pdf.text(`${label}:`, margin, y)
        pdf.setFont(undefined, 'normal'); pdf.text(value, margin + 55, y)
        y += 7
      }

      if (summaryRef.current) {
        pdf.addPage()
        pdf.setFontSize(16); pdf.setTextColor(15, 23, 42)
        pdf.text('Summary', margin, 25)
        const summaryCanvas = await html2canvas(summaryRef.current, { scale: 2, useCORS: true, backgroundColor: '#fafaf9' })
        const summaryImg = summaryCanvas.toDataURL('image/png')
        const imgW = pageW - margin * 2
        const imgH = (summaryCanvas.height / summaryCanvas.width) * imgW
        pdf.addImage(summaryImg, 'PNG', margin, 35, imgW, Math.min(imgH, pageH - 55))
      }

      if (tableRef.current) {
        pdf.addPage()
        pdf.setFontSize(16); pdf.setTextColor(15, 23, 42)
        pdf.text('Detailed Results by CRF', margin, 25)
        const tableCanvas = await html2canvas(tableRef.current, { scale: 2, useCORS: true, backgroundColor: '#ffffff' })
        const tableImg = tableCanvas.toDataURL('image/png')
        const imgW = pageW - margin * 2
        const imgH = (tableCanvas.height / tableCanvas.width) * imgW
        const maxImgH = pageH - 45
        const finalH = Math.min(imgH, maxImgH)
        pdf.addImage(tableImg, 'PNG', margin, 35, imgW, finalH)
        if (imgH > maxImgH) {
          pdf.setFontSize(9); pdf.setTextColor(148, 163, 184)
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

  return (
    <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-4">
      <ExportTile kicker="CSV" label="Results table" description="Tidy CSV via Papaparse — every CRF, every endpoint, every CI." disabled={rows.length === 0} onClick={handleDownloadCSV} />
      <ExportTile kicker="PDF" label="Full report" description="Title page, parameters, summary cards, and the detail table." busy={pdfBusy} onClick={handleDownloadPDF} />
      <ExportTile kicker="JSON" label="Reproducibility config" description="The exact inputs that produced these numbers, in one file." onClick={handleDownloadConfig} />
      <ExportTile kicker="Save" label="Reusable template" description="Save this configuration to start a new analysis from it." onClick={onOpenTemplateModal} />
    </div>
  )
}

const TABS = [
  { key: 'table',  label: 'Detail' },
  { key: 'map',    label: 'Map' },
  { key: 'trend',  label: 'Trend' },
  { key: 'export', label: 'Export' },
]

// ── Main Page ──────────────────────────────────────────────
export default function Results() {
  const {
    results, step1, step2, step6, step7, exportConfig,
    additionalRuns, appendAdditionalRun,
  } = useAnalysisStore()
  const [activeTab, setActiveTab] = useState('table')
  const [templateModal, setTemplateModal] = useState(false)
  const [savingTemplate, setSavingTemplate] = useState(false)
  const [templateSaved, setTemplateSaved] = useState(false)
  const [running, setRunning] = useState(false)
  const [runError, setRunError] = useState(null)

  const summaryRef = useRef(null)
  const tableRef = useRef(null)

  const isSpatial = Boolean(results?.zones)
  const summary = isSpatial ? (results?.aggregate ?? {}) : (results?.summary ?? {})
  const totalDeaths = isSpatial ? results?.totalDeaths : summary.totalDeaths
  const hasValuation = step7?.runValuation && summary.economicValue != null
  const analysisName = results?.meta?.analysisName || step1?.analysisName || ''
  const detailRows = results?.detail ?? []

  // Years already consumed (primary + stacked additional runs) — excluded
  // from the "Compare another year" picker to prevent duplicate runs.
  const primaryYear = step2?.baseline?.year ?? results?.meta?.year ?? null
  const usedYears = useMemo(
    () => [
      ...(primaryYear != null ? [primaryYear] : []),
      ...additionalRuns.map((r) => r.year),
    ],
    [primaryYear, additionalRuns],
  )

  // Broad 1990..current-year fallback. A follow-up can narrow this to
  // the years the primary run's datasets actually cover by querying
  // /api/data/datasets.
  const allowedYears = useMemo(() => {
    const years = []
    const maxYear = new Date().getFullYear()
    for (let y = maxYear; y >= 1990; y--) years.push(y)
    return years
  }, [])

  const handleRunAnotherYear = useCallback(async (year) => {
    setRunning(true)
    setRunError(null)
    try {
      const cfg = exportConfig()
      const res = await runAnalysisForYear(cfg, year)
      appendAdditionalRun({
        runId: `run-${Date.now()}`,
        year,
        results: res,
      })
    } catch (err) {
      setRunError(err.message || 'Run failed')
    } finally {
      setRunning(false)
    }
  }, [exportConfig, appendAdditionalRun])

  const handleRemoveRun = useCallback((runId) => {
    const kept = additionalRuns.filter((r) => r.runId !== runId)
    useAnalysisStore.setState({ additionalRuns: kept })
  }, [additionalRuns])

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
    <div className="min-h-[100dvh] bg-paper">
      {/* Header */}
      <header className="border-b border-zinc-200/80">
        <div className="max-w-[1280px] mx-auto px-6 lg:px-10 py-6 flex items-start justify-between gap-6">
          <div>
            <p className="eyebrow mb-2">Run complete</p>
            <h1 className="text-ink">Analysis results</h1>
            {analysisName && (
              <p className="mt-2 text-[14px] text-zinc-500 max-w-prose">{analysisName}</p>
            )}
          </div>
          <div className="flex items-center gap-3 shrink-0">
            {templateSaved && (
              <span className="flex items-center gap-1.5 font-mono text-[10px] uppercase tracking-[0.14em] text-accent-700">
                <CheckIcon className="w-3 h-3" />
                Template saved
              </span>
            )}
            <Link to="/analysis/6" className="btn-ghost">
              <ChevronLeft className="w-3.5 h-3.5" />
              Back to analysis inputs
            </Link>
            <Link to="/" className="btn-ghost">
              Home
            </Link>
            <Link to="/" className="btn-primary">
              New analysis
            </Link>
          </div>
        </div>
      </header>

      <div className="max-w-[1280px] mx-auto px-6 lg:px-10 py-12">
        {!results ? (
          // ── Empty state ──────────────────────────────────
          <div className="surface p-16 text-center">
            <p className="font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-400">
              No results yet
            </p>
            <p className="mt-3 text-[15px] text-zinc-600">
              No analysis has been run yet.
            </p>
            <Link to="/" className="btn-accent inline-flex mt-6">
              Home
              <ArrowRight className="w-3.5 h-3.5" />
            </Link>
          </div>
        ) : (
          <>
            {/* ── Hero summary ─────────────────────────────── */}
            <div ref={summaryRef} className="space-y-6 mb-12">
              <HeroNumber
                totalDeaths={totalDeaths}
                detailRows={detailRows}
                isSpatial={isSpatial}
                zoneCount={results?.zones?.length}
              />

              <div className={`grid gap-x-10 gap-y-8 ${hasValuation ? 'sm:grid-cols-3' : 'sm:grid-cols-2'}`}>
                <SecondaryStat
                  label="Attributable fraction"
                  value={fmtPercent(summary.attributableFraction)}
                  sub="Share of deaths attributable to exposure"
                />
                <SecondaryStat
                  label="Rate per 100k"
                  value={summary.attributableRate != null ? fmtNumber(summary.attributableRate, 1) : '—'}
                  sub="Per 100,000 population"
                />
                {hasValuation && (
                  <SecondaryStat
                    label="Economic value"
                    value={fmtCurrency(summary.economicValue)}
                    sub={`VSL-based valuation · ${step7.currency} ${step7.dollarYear}`}
                  />
                )}
              </div>
            </div>

            {/* ── Endpoint breakdown ───────────────────────── */}
            {detailRows.length > 0 && (
              <div className="mb-12">
                <EndpointBreakdown rows={detailRows} />
              </div>
            )}

            {/* ── Tabs ─────────────────────────────────────── */}
            <div>
              <div className="flex items-center justify-between border-b border-zinc-200/80">
                <nav className="flex items-center -mb-px">
                  {TABS.map((tab) => {
                    const active = activeTab === tab.key
                    return (
                      <button
                        key={tab.key}
                        type="button"
                        onClick={() => setActiveTab(tab.key)}
                        className={`relative px-4 py-3 text-[13px] font-medium tracking-tight
                                    transition-colors duration-150 ease-out
                                    ${active ? 'text-ink' : 'text-zinc-500 hover:text-ink'}`}
                      >
                        {tab.label}
                        {active && (
                          <span className="absolute inset-x-3 -bottom-px h-px bg-ink" />
                        )}
                      </button>
                    )
                  })}
                </nav>
                <p className="font-mono text-[10px] uppercase tracking-[0.14em] text-zinc-400 hidden sm:block">
                  {detailRows.length} {detailRows.length === 1 ? 'row' : 'rows'}
                </p>
              </div>

              <div className="pt-8">
                {activeTab === 'map' && <MapTab zones={results?.zones} />}
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
                    step2={step2}
                    step6={step6}
                    step7={step7}
                    exportConfig={exportConfig}
                    onOpenTemplateModal={() => setTemplateModal(true)}
                  />
                )}
              </div>
            </div>

            {/* ── Multi-year comparison ──────────────────────── */}
            <div className="mt-12 space-y-5">
              {additionalRuns.length > 0 && (
                <div className="space-y-3">
                  <p className="text-xs uppercase tracking-widest text-gray-500">
                    Additional years
                  </p>
                  {additionalRuns.map((run) => (
                    <AdditionalRunSummary
                      key={run.runId}
                      run={run}
                      onRemove={handleRemoveRun}
                    />
                  ))}
                </div>
              )}

              <CompareAnotherYearCard
                allowedYears={allowedYears}
                excludeYears={usedYears}
                additionalRunCount={additionalRuns.length}
                onRun={handleRunAnotherYear}
                running={running}
              />

              {runError && (
                <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg text-sm text-amber-700">
                  {runError}
                </div>
              )}
            </div>
          </>
        )}
      </div>

      <SaveTemplateModal
        open={templateModal}
        onClose={() => setTemplateModal(false)}
        onSave={handleSaveTemplate}
        saving={savingTemplate}
      />
    </div>
  )
}
