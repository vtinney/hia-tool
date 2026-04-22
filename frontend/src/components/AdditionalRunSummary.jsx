function fmtNumber(n) {
  if (n == null) return '—'
  return Number(n).toLocaleString(undefined, { maximumFractionDigits: 0 })
}

export default function AdditionalRunSummary({ run, onRemove }) {
  // Results shape varies by pooling mode:
  //   - spatial with pooled total:    results.totalDeaths
  //   - non-spatial / aggregate:      results.summary.totalDeaths
  //   - pooling: none                 totalDeaths is absent
  const totalDeaths =
    run.results?.totalDeaths ?? run.results?.summary?.totalDeaths ?? null
  const mean = totalDeaths?.mean ?? null
  const lower = totalDeaths?.lower95 ?? null
  const upper = totalDeaths?.upper95 ?? null

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5 flex items-center justify-between gap-4">
      <div className="min-w-0">
        <p className="text-xs uppercase tracking-widest text-gray-500 mb-1">
          Year {run.year}
        </p>
        <p className="font-mono font-medium text-gray-900 leading-none tabular-nums text-2xl">
          {fmtNumber(mean)}
        </p>
        <p className="mt-1 text-xs text-gray-500">
          attributable deaths
          {lower != null && upper != null && (
            <span className="text-gray-400">
              {' · 95% CI '}
              {fmtNumber(lower)}–{fmtNumber(upper)}
            </span>
          )}
        </p>
      </div>
      <button
        type="button"
        onClick={() => onRemove(run.runId)}
        className="text-gray-400 hover:text-gray-700 text-sm shrink-0"
        aria-label={`Remove ${run.year} run`}
      >
        Remove
      </button>
    </div>
  )
}
