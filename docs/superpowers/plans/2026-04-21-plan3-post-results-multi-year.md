# Plan 3: Post-results multi-year runs

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Prerequisite:** Plans 1 and 2 must be merged. This plan assumes `step2.baseline.year`, `step2.control.year`, `step3.year`, and `step4.year` exist in the store and each data step uses them.

**Goal:** Let users who've just completed one run trigger additional runs for different years, stacked below the primary result. Cap at 10 total runs. A confirmation appears after the second additional run (so the third and onwards are deliberate).

**Architecture:** The primary `results` object stays single (preserves the existing hero/chart/export components). A new `additionalRuns` array in the store holds `{runId, year, results}` entries appended by a "Compare another year" card on the Results page. Each card reuses a compact summary component. Cloning an analysis for a new year means spreading the current analysis config with every `.year` field replaced; the existing compute endpoint handles the rest.

**Tech stack:** React / Zustand / Vite / Vitest (frontend only — no backend changes).

---

## Task 1: Store — add `additionalRuns` and actions

**Files:**
- Modify: `frontend/src/stores/useAnalysisStore.js`

- [ ] **Step 1: Extend state and actions**

In `frontend/src/stores/useAnalysisStore.js`:

1. Add `additionalRuns: []` to `initialState()`:

```js
function initialState() {
  return {
    currentStep: 1,
    totalSteps: 7,
    completedSteps: [],
    stepValidity: defaultStepValidity(),
    step1: { ...DEFAULT_STEP1 },
    step2: { ...DEFAULT_STEP2, baseline: { ...DEFAULT_STEP2.baseline }, control: { ...DEFAULT_STEP2.control } },
    step3: { ...DEFAULT_STEP3 },
    step4: { ...DEFAULT_STEP4 },
    step5: { ...DEFAULT_STEP5, selectedCRFs: [] },
    step6: { ...DEFAULT_STEP6 },
    step7: { ...DEFAULT_STEP7 },
    results: null,
    additionalRuns: [],
  }
}
```

2. Add actions before the `// ── Reset to defaults ──` block:

```js
      appendAdditionalRun: (run) =>
        set((state) => ({
          additionalRuns: [...state.additionalRuns, run],
        })),

      clearAdditionalRuns: () => set({ additionalRuns: [] }),
```

3. Extend `reset` to also clear `additionalRuns` — it already calls `initialState()`, so nothing to change.

4. Extend `setResults` to clear additional runs when a fresh primary result arrives:

```js
      setResults: (results) => set({ results, additionalRuns: [] }),
```

5. Bump `version: 6 → 7` and extend the migration:

```js
      version: 7,
      migrate: (persisted, version) => {
        if (version < 7) return initialState()
        return persisted
      },
```

6. Add `additionalRuns` to `partialize`:

```js
      partialize: (state) => ({
        currentStep: state.currentStep,
        completedSteps: state.completedSteps,
        stepValidity: state.stepValidity,
        step1: state.step1,
        step2: state.step2,
        step3: state.step3,
        step4: state.step4,
        step5: state.step5,
        step6: state.step6,
        step7: state.step7,
        additionalRuns: state.additionalRuns,
      }),
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/stores/useAnalysisStore.js
git commit -m "feat(store): additionalRuns list + appendAdditionalRun action (v7)"
```

---

## Task 2: API — helper to run an analysis for a different year

**Files:**
- Modify: `frontend/src/lib/api.js`
- Test: `frontend/src/lib/__tests__/api.runForYear.test.js` (create)

Thin helper that takes the current analysis config and a new year, clones it with every `.year` replaced, and POSTs to `/api/compute/spatial`. The helper does not mutate the store — the caller decides what to do with the response.

- [ ] **Step 1: Write the failing test**

Create `frontend/src/lib/__tests__/api.runForYear.test.js`:

```js
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { cloneConfigWithYear } from '../api'

describe('cloneConfigWithYear', () => {
  it('replaces every year field and leaves everything else alone', () => {
    const base = {
      step1: { pollutant: 'pm25', studyArea: { id: 'MEX' } },
      step2: {
        baseline: { type: 'dataset', datasetId: 'who', year: 2018, value: 15.2 },
        control: { type: 'none', year: null },
      },
      step3: { populationType: 'manual', totalPopulation: 1e6, year: 2018 },
      step4: { incidenceType: 'manual', year: 2018 },
      step5: { selectedCRFs: [] },
      step6: { poolingMethod: 'separate' },
      step7: { runValuation: false },
    }

    const cloned = cloneConfigWithYear(base, 2016)

    expect(cloned.step2.baseline.year).toBe(2016)
    expect(cloned.step2.control.year).toBe(2016)
    expect(cloned.step3.year).toBe(2016)
    expect(cloned.step4.year).toBe(2016)

    // Untouched fields
    expect(cloned.step1.pollutant).toBe('pm25')
    expect(cloned.step2.baseline.datasetId).toBe('who')
    expect(cloned.step3.totalPopulation).toBe(1e6)
    expect(cloned.step5).toEqual({ selectedCRFs: [] })

    // Should not have mutated input
    expect(base.step2.baseline.year).toBe(2018)
  })

  it('leaves null control.year null when source config had it null', () => {
    const base = {
      step1: {},
      step2: {
        baseline: { year: 2018 },
        control: { year: null },
      },
      step3: { year: 2018 },
      step4: { year: 2018 },
    }
    const cloned = cloneConfigWithYear(base, 2020)
    expect(cloned.step2.control.year).toBe(null)
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd frontend && npm test -- src/lib/__tests__/api.runForYear.test.js
```

Expected: FAIL.

- [ ] **Step 3: Implement `cloneConfigWithYear` and `runAnalysisForYear` in `api.js`**

Add to the bottom of `frontend/src/lib/api.js`:

```js
/**
 * Return a deep copy of an analysis config with every year field
 * replaced by `year`. Control.year is only updated when the source
 * config had a non-null value (preserves the "no control scenario"
 * case). Does not mutate the input.
 */
export function cloneConfigWithYear(config, year) {
  const cloned = JSON.parse(JSON.stringify(config))
  if (cloned.step2?.baseline) cloned.step2.baseline.year = year
  if (cloned.step2?.control && cloned.step2.control.year != null) {
    cloned.step2.control.year = year
  }
  if (cloned.step3) cloned.step3.year = year
  if (cloned.step4) cloned.step4.year = year
  return cloned
}

/**
 * Run the analysis backend with the current config re-keyed to a new
 * year. Returns the raw compute response (same shape as the first run).
 */
export async function runAnalysisForYear(config, year) {
  const req = cloneConfigWithYear(config, year)
  return runSpatialCompute(req)
}
```

- [ ] **Step 4: Run test**

```bash
cd frontend && npm test -- src/lib/__tests__/api.runForYear.test.js
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/lib/api.js frontend/src/lib/__tests__/api.runForYear.test.js
git commit -m "feat(api): cloneConfigWithYear + runAnalysisForYear helpers"
```

---

## Task 3: `CompareAnotherYearCard` component

**Files:**
- Create: `frontend/src/components/CompareAnotherYearCard.jsx`

Self-contained card shown on the Results page. Owns:
- a year `<select>` (year options can be constrained to the original datasets' years — passed via prop),
- a "Run" button,
- the confirmation dialog for the third run onward,
- a disabled state at the hard cap (10 total runs).

Emits `onRun(year)` — the parent handles the actual API call + store update.

- [ ] **Step 1: Implement the component**

Create `frontend/src/components/CompareAnotherYearCard.jsx`:

```jsx
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
      <div className="surface p-6">
        <p className="eyebrow mb-1">Multi-year comparison</p>
        <p className="text-sm text-zinc-500">
          Reached the 10-run limit for this analysis. Start a new analysis to compare more years.
        </p>
      </div>
    )
  }

  return (
    <>
      <div className="surface p-6">
        <p className="eyebrow mb-1">Compare another year</p>
        <p className="text-sm text-zinc-500 mb-4">
          Run the same analysis for a different year. Results are stacked below the primary.
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
            className="btn-accent disabled:opacity-40 disabled:cursor-not-allowed"
          >
            {running ? 'Running…' : 'Run'}
          </button>
        </div>

        {additionalRunCount >= 1 && (
          <p className="mt-3 text-[11px] text-zinc-400">
            {additionalRunCount + 1} of {MAX_TOTAL_RUNS} runs used.
          </p>
        )}
      </div>

      {confirmOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-ink/40 backdrop-blur-sm p-4">
          <div className="surface w-full max-w-md p-7">
            <p className="eyebrow mb-2">Confirm</p>
            <h3 className="text-[18px] font-medium tracking-tight text-ink mb-3">
              Another year?
            </h3>
            <p className="text-sm text-zinc-600 mb-5">
              Running multi-year trends is only useful for policy comparisons or robustness checks.
              Most analyses don't need this. Continue?
            </p>
            <div className="flex justify-end gap-3">
              <button onClick={() => setConfirmOpen(false)} className="btn-ghost">Cancel</button>
              <button onClick={confirmAndRun} className="btn-accent">Continue</button>
            </div>
          </div>
        </div>
      )}
    </>
  )
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/CompareAnotherYearCard.jsx
git commit -m "feat(components): CompareAnotherYearCard with cap + confirmation"
```

---

## Task 4: `AdditionalRunSummary` — compact card for each extra run

**Files:**
- Create: `frontend/src/components/AdditionalRunSummary.jsx`

Compact card showing a single extra run's year, total attributable deaths (with 95% CI), and a "×" to remove it.

- [ ] **Step 1: Implement the component**

Create `frontend/src/components/AdditionalRunSummary.jsx`:

```jsx
function fmtNumber(n) {
  if (n == null) return '—'
  return Number(n).toLocaleString(undefined, { maximumFractionDigits: 0 })
}

export default function AdditionalRunSummary({ run, onRemove }) {
  const totalDeaths = run.results?.totalDeaths ?? run.results?.summary?.totalDeaths ?? null
  const mean = totalDeaths?.mean ?? null
  const lower = totalDeaths?.lower95 ?? null
  const upper = totalDeaths?.upper95 ?? null

  return (
    <div className="surface p-6 flex items-center justify-between gap-4">
      <div>
        <p className="eyebrow mb-1">Year {run.year}</p>
        <p className="font-mono font-medium text-ink leading-none tabular-nums text-[28px]">
          {fmtNumber(mean)}
        </p>
        <p className="mt-1 text-[12px] text-zinc-500">
          attributable deaths
          {lower != null && upper != null && (
            <span className="text-zinc-400"> · 95% CI {fmtNumber(lower)}–{fmtNumber(upper)}</span>
          )}
        </p>
      </div>
      <button
        type="button"
        onClick={() => onRemove(run.runId)}
        className="text-zinc-400 hover:text-zinc-600 text-sm"
        aria-label={`Remove ${run.year} run`}
      >
        Remove
      </button>
    </div>
  )
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/AdditionalRunSummary.jsx
git commit -m "feat(components): AdditionalRunSummary card for stacked multi-year results"
```

---

## Task 5: Wire the cards into `Results.jsx`

**Files:**
- Modify: `frontend/src/pages/Results.jsx`

- [ ] **Step 1: Import the new pieces and add state**

At the top of `Results.jsx`:

```js
import CompareAnotherYearCard from '../components/CompareAnotherYearCard'
import AdditionalRunSummary from '../components/AdditionalRunSummary'
import { runAnalysisForYear } from '../lib/api'
```

In the `Results` component (line 583), extend the store destructure:

```js
  const {
    results, step1, step2, step6, step7, exportConfig,
    additionalRuns, appendAdditionalRun,
  } = useAnalysisStore()

  const [running, setRunning] = useState(false)
  const [runError, setRunError] = useState(null)
```

- [ ] **Step 2: Derive allowed years for the compare card**

Just after existing derivations (around line 593-598), compute the year list. If the baseline used a dataset, use that dataset's years; otherwise fall back to a broad 1990–current-year range.

```js
  const primaryYear = step2?.baseline?.year ?? results?.meta?.year ?? null
  const usedYears = useMemo(
    () => [
      ...(primaryYear != null ? [primaryYear] : []),
      ...additionalRuns.map((r) => r.year),
    ],
    [primaryYear, additionalRuns],
  )

  // Allowed years: mirror the primary run's dataset availability if we
  // have it; otherwise default to a broad 1990-current range. Follow-up
  // work can narrow this by querying the scanner output.
  const allowedYears = useMemo(() => {
    const years = []
    const maxYear = new Date().getFullYear()
    for (let y = maxYear; y >= 1990; y--) years.push(y)
    return years
  }, [])
```

- [ ] **Step 3: Implement the run handler**

Just below the state declarations:

```js
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
```

- [ ] **Step 4: Render the cards just above the final closing `</>` block inside the `else` branch of `!results`**

Find the location just after the existing tabs block (around line 755) and before the closing `</>`. Insert:

```jsx
            {/* ── Multi-year comparison ──────────────────────── */}
            <div className="mt-12 space-y-5">
              {additionalRuns.length > 0 && (
                <div className="space-y-3">
                  <p className="eyebrow">Additional years</p>
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
```

- [ ] **Step 5: Manual smoke test**

With Plans 1 & 2 merged, walk Mexico → PM2.5 → WHO AAP 2018 → complete analysis. On the Results page:
1. Verify the "Compare another year" card appears below the tabs.
2. Pick 2015 (2018 is excluded since it's the primary year). Click Run. An "Additional years — Year 2015" card appears with its own number.
3. Pick 2017, click Run. No confirmation yet (1 additional run completed, 2 still allowed without prompt).
4. Pick 2016, click Run. Confirmation modal appears ("Another year?"). Click Continue. A third extra run appears.
5. Verify Remove button removes an individual run.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/pages/Results.jsx
git commit -m "feat(results): post-run Compare-another-year flow with stacked summaries"
```

---

## Task 6: Regression sweep

- [ ] **Step 1: Frontend tests**

```bash
cd frontend && npm test
```

Expected: PASS. (`CompareAnotherYearCard` is covered via manual smoke since it's a UI orchestration; the `cloneConfigWithYear` helper carries the unit-level assurance.)

- [ ] **Step 2: Backend tests**

```bash
venv/Scripts/python.exe -m pytest backend/tests/ -v
```

Expected: PASS (no backend changes in this plan).

- [ ] **Step 3: End-to-end smoke with Plan 1 + Plan 2 + Plan 3 merged**

Walk full scenario end to end — Mexico PM2.5 WHO AAP 2018 → results → Compare 2015 → Compare 2017 → Compare 2016 (confirmation) → Remove 2017 → Export PDF. Confirm no errors in console.

---

## Spec alignment check

| Spec item | Task |
|-----------|------|
| D6: "Compare another year" card on results | Task 3, 5 |
| D6: Shifts all four year fields on clone | Task 2 |
| D6: Cap at 10 total runs | Task 3 |
| D6: Confirmation after 2 additional | Task 3 |
| D6: Stacked results | Task 4, 5 |
| Data model: `additionalRuns` list | Task 1 |

Out-of-scope (deferred per spec Non-goals):
- Trend chart visualization across multi-run years.
- PDF export including additional runs (stays single-run for this round).
- Smoothing via range averaging.
