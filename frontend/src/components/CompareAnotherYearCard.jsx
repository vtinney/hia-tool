import { useState, useMemo } from 'react'
import YearField from './YearField'

const MAX_TOTAL_RUNS = 10
const CONFIRMATION_AFTER_N_ADDITIONAL = 2

export default function CompareAnotherYearCard({
  allowedYears,
  excludeYears = [],
  additionalRunCount,
  onRun,
  running,
}) {
  const [year, setYear] = useState(null)
  const [confirmOpen, setConfirmOpen] = useState(false)

  const totalRuns = additionalRunCount + 1
  const atCap = totalRuns >= MAX_TOTAL_RUNS
  const needsConfirmation = additionalRunCount >= CONFIRMATION_AFTER_N_ADDITIONAL

  const options = useMemo(
    () => (allowedYears || []).filter((y) => !excludeYears.includes(y)),
    [allowedYears, excludeYears],
  )

  const handleRun = () => {
    if (!year || running || atCap) return
    if (needsConfirmation) {
      setConfirmOpen(true)
      return
    }
    onRun(year)
    setYear(null)
  }

  const confirmAndRun = () => {
    setConfirmOpen(false)
    onRun(year)
    setYear(null)
  }

  if (atCap) {
    return (
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
        <p className="text-xs uppercase tracking-widest text-gray-500 mb-1">
          Multi-year comparison
        </p>
        <p className="text-sm text-gray-500">
          Reached the {MAX_TOTAL_RUNS}-run limit for this analysis. Start a new
          analysis to compare more years.
        </p>
      </div>
    )
  }

  return (
    <>
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
        <p className="text-xs uppercase tracking-widest text-gray-500 mb-1">
          Compare another year
        </p>
        <p className="text-sm text-gray-500 mb-4">
          Run the same analysis for a different year. Results stack below the primary.
        </p>

        <div className="flex items-end gap-3">
          <div className="flex-1">
            <YearField
              id="compare-year"
              label="Year"
              value={year}
              baselineYear={null}
              allowedYears={options}
              onChange={setYear}
            />
          </div>
          <button
            type="button"
            onClick={handleRun}
            disabled={!year || running}
            className="px-4 py-2.5 rounded-lg text-sm font-medium transition-colors
                       bg-blue-600 text-white hover:bg-blue-700
                       disabled:opacity-40 disabled:cursor-not-allowed"
          >
            {running ? 'Running…' : 'Run'}
          </button>
        </div>

        {additionalRunCount >= 1 && (
          <p className="mt-3 text-xs text-gray-400">
            {additionalRunCount + 1} of {MAX_TOTAL_RUNS} runs used.
          </p>
        )}
      </div>

      {confirmOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-gray-900/40 backdrop-blur-sm p-4">
          <div className="bg-white rounded-xl shadow-lg w-full max-w-md p-6">
            <p className="text-xs uppercase tracking-widest text-gray-500 mb-2">
              Confirm
            </p>
            <h3 className="text-lg font-medium text-gray-900 mb-3">Another year?</h3>
            <p className="text-sm text-gray-600 mb-5">
              Running multi-year trends is only useful for policy comparisons or
              robustness checks. Most analyses don't need this. Continue?
            </p>
            <div className="flex justify-end gap-3">
              <button
                onClick={() => setConfirmOpen(false)}
                className="px-4 py-2 rounded-lg border border-gray-300 text-sm text-gray-700
                           hover:bg-gray-50 transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={confirmAndRun}
                className="px-4 py-2 rounded-lg bg-blue-600 text-white text-sm
                           hover:bg-blue-700 transition-colors"
              >
                Continue
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  )
}
