import { useState, useMemo } from 'react'

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

// ── Column definitions ──────────────────────────────────────────

const COLUMNS = [
  { key: 'crfStudy',           label: 'CRF Study',           format: String },
  { key: 'framework',          label: 'Framework',           format: String },
  { key: 'endpoint',           label: 'Endpoint',            format: String },
  { key: 'attributableCases',  label: 'Attr. Cases (mean)',  format: (v) => fmtNumber(v, 1), numeric: true },
  { key: 'lower95',            label: 'Lower 95% CI',        format: (v) => fmtNumber(v, 1), numeric: true },
  { key: 'upper95',            label: 'Upper 95% CI',        format: (v) => fmtNumber(v, 1), numeric: true },
  { key: 'attributableFraction', label: 'Attr. Fraction',    format: fmtPercent,              numeric: true },
  { key: 'ratePer100k',        label: 'Rate per 100k',       format: (v) => fmtNumber(v, 1), numeric: true },
  { key: 'economicValue',      label: 'Economic Value',      format: fmtCurrency,             numeric: true },
]

// ── Sort indicator arrow ────────────────────────────────────────

function SortArrow({ direction }) {
  if (!direction) {
    return (
      <svg className="w-3 h-3 text-zinc-300 ml-1 inline-block" viewBox="0 0 14 14" fill="currentColor" aria-hidden="true">
        <path d="M7 2l3.5 4h-7L7 2zM7 12L3.5 8h7L7 12z" />
      </svg>
    )
  }
  return (
    <svg className="w-3 h-3 text-accent-700 ml-1 inline-block" viewBox="0 0 14 14" fill="currentColor" aria-hidden="true">
      {direction === 'asc'
        ? <path d="M7 2l3.5 4h-7L7 2z" />
        : <path d="M7 12L3.5 8h7L7 12z" />}
    </svg>
  )
}

// ── Main component ──────────────────────────────────────────────

export default function ResultsTable({ rows = [], hasValuation = false, hasSpatialUnits = false }) {
  const [sortKey, setSortKey] = useState(null)
  const [sortDir, setSortDir] = useState('asc')
  const [endpointFilter, setEndpointFilter] = useState('')
  const [frameworkFilter, setFrameworkFilter] = useState('')

  // Derive the set of unique frameworks for the dropdown
  const frameworks = useMemo(
    () => [...new Set(rows.map((r) => r.framework).filter(Boolean))].sort(),
    [rows],
  )

  // Build visible column list (conditionally include spatial_unit and economicValue)
  const visibleColumns = useMemo(() => {
    let cols = COLUMNS
    if (!hasValuation) cols = cols.filter((c) => c.key !== 'economicValue')
    if (hasSpatialUnits) {
      cols = [
        { key: 'spatialUnit', label: 'Spatial Unit', format: String },
        ...cols,
      ]
    }
    return cols
  }, [hasValuation, hasSpatialUnits])

  // Filter
  const filtered = useMemo(() => {
    let data = rows
    if (endpointFilter) {
      const q = endpointFilter.toLowerCase()
      data = data.filter((r) => (r.endpoint ?? '').toLowerCase().includes(q))
    }
    if (frameworkFilter) {
      data = data.filter((r) => r.framework === frameworkFilter)
    }
    return data
  }, [rows, endpointFilter, frameworkFilter])

  // Sort
  const sorted = useMemo(() => {
    if (!sortKey) return filtered
    const col = visibleColumns.find((c) => c.key === sortKey)
    const numeric = col?.numeric
    return [...filtered].sort((a, b) => {
      let va = a[sortKey]
      let vb = b[sortKey]
      if (va == null && vb == null) return 0
      if (va == null) return 1
      if (vb == null) return -1
      if (numeric) {
        va = Number(va)
        vb = Number(vb)
      } else {
        va = String(va).toLowerCase()
        vb = String(vb).toLowerCase()
      }
      if (va < vb) return sortDir === 'asc' ? -1 : 1
      if (va > vb) return sortDir === 'asc' ? 1 : -1
      return 0
    })
  }, [filtered, sortKey, sortDir, visibleColumns])

  function handleSort(key) {
    if (sortKey === key) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortKey(key)
      setSortDir('asc')
    }
  }

  // ── Empty state ───────────────────────────────────────────────

  if (rows.length === 0) {
    return (
      <div className="py-16 text-center">
        <p className="font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-400">
          No detailed results available
        </p>
      </div>
    )
  }

  // ── Render ────────────────────────────────────────────────────

  return (
    <div>
      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3 mb-5">
        <div className="relative">
          <svg
            className="absolute left-3 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-zinc-400 pointer-events-none"
            fill="none" viewBox="0 0 24 24" stroke="currentColor" aria-hidden="true"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          <input
            type="text"
            placeholder="Filter by endpoint…"
            value={endpointFilter}
            onChange={(e) => setEndpointFilter(e.target.value)}
            className="input pl-9 w-60"
          />
        </div>

        <select
          value={frameworkFilter}
          onChange={(e) => setFrameworkFilter(e.target.value)}
          className="input w-auto"
        >
          <option value="">All frameworks</option>
          {frameworks.map((fw) => (
            <option key={fw} value={fw}>{fw}</option>
          ))}
        </select>

        {(endpointFilter || frameworkFilter) && (
          <button
            type="button"
            onClick={() => { setEndpointFilter(''); setFrameworkFilter('') }}
            className="btn-link text-zinc-500"
          >
            Clear filters
          </button>
        )}

        <span className="ml-auto font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-400 tabular-nums">
          {sorted.length} / {rows.length} {rows.length === 1 ? 'row' : 'rows'}
        </span>
      </div>

      {/* Table — borders, not boxes */}
      <div className="overflow-x-auto border-y border-zinc-200/80">
        <table className="min-w-full text-[13px]">
          <thead>
            <tr className="border-b border-zinc-200/80">
              {visibleColumns.map((col) => (
                <th
                  key={col.key}
                  onClick={() => handleSort(col.key)}
                  className={`px-4 py-3 font-mono text-[10px] uppercase tracking-[0.12em] text-zinc-500 whitespace-nowrap cursor-pointer select-none
                              hover:text-ink transition-colors duration-150 ease-out
                              ${col.numeric ? 'text-right' : 'text-left'}`}
                >
                  {col.label}
                  <SortArrow direction={sortKey === col.key ? sortDir : null} />
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-100">
            {sorted.length === 0 ? (
              <tr>
                <td colSpan={visibleColumns.length} className="px-4 py-10 text-center">
                  <span className="font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-400">
                    No rows match the current filters
                  </span>
                </td>
              </tr>
            ) : (
              sorted.map((row, i) => (
                <tr key={i} className="hover:bg-zinc-50/60 transition-colors duration-150 ease-out">
                  {visibleColumns.map((col) => (
                    <td
                      key={col.key}
                      className={`px-4 py-3 whitespace-nowrap ${
                        col.numeric
                          ? 'text-right font-mono text-[12.5px] tabular-nums text-ink'
                          : 'text-zinc-700'
                      }`}
                    >
                      {row[col.key] != null ? col.format(row[col.key]) : <span className="text-zinc-300">—</span>}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
