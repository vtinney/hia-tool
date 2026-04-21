# Plan 2: Year cascade to Steps 3/4 + upload year fields

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Prerequisite:** Plan 1 must be merged. This plan builds on `step2.baseline.year`, the Step 2 dataset-before-year flow, and the `countries_covered` scanner output.

**Goal:** Propagate Step 2's baseline year to Steps 3 (population) and 4 (incidence) as an editable default, show a "differs from baseline year" badge when they diverge, and add a required year input to every file-upload flow.

**Architecture:** `step3.year` and `step4.year` are added to the store with a prefill rule: when null, they inherit from `step2.baseline.year` at render time (no write-through). User edits persist to the step's own field. Uploads add a `<input type="number">` year field required before the step is valid.

**Tech stack:** React / Zustand / Vite / Vitest (frontend only — no backend changes).

---

## Task 1: Store — add year fields to Steps 3 and 4

**Files:**
- Modify: `frontend/src/stores/useAnalysisStore.js`

- [ ] **Step 1: Add year fields to defaults and bump store version**

In `frontend/src/stores/useAnalysisStore.js`, update `DEFAULT_STEP3` and `DEFAULT_STEP4`:

```js
const DEFAULT_STEP3 = {
  populationType: 'manual',
  totalPopulation: null,
  ageGroups: null,
  uploadId: null,
  year: null,
}

const DEFAULT_STEP4 = {
  incidenceType: 'manual',
  rates: null,
  year: null,
}
```

Bump `version: 5 → 6` and extend the migration:

```js
      version: 6,
      // ...
      migrate: (persisted, version) => {
        // v5 and older had a different step shape. v6 added year to
        // step3 and step4. Always reset — simpler than partial upgrade.
        if (version < 6) return initialState()
        return persisted
      },
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/stores/useAnalysisStore.js
git commit -m "feat(store): add step3.year and step4.year fields (v6)"
```

---

## Task 2: `YearField` reusable component

**Files:**
- Create: `frontend/src/components/YearField.jsx`
- Test: `frontend/src/components/__tests__/YearField.test.jsx`

Small component that renders a year `<select>` with optional constraint to a list of allowed years, and a "Differs from baseline year (N)" badge when the chosen year is not null and does not match the baseline.

- [ ] **Step 1: Write the failing test**

Create `frontend/src/components/__tests__/YearField.test.jsx`:

```jsx
import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import YearField from '../YearField'

describe('YearField', () => {
  it('renders the current value in the select', () => {
    render(
      <YearField label="Year" value={2018} baselineYear={2018} onChange={() => {}} />,
    )
    expect(screen.getByRole('combobox')).toHaveValue('2018')
  })

  it('shows "differs from baseline" badge when year does not match baseline', () => {
    render(
      <YearField label="Year" value={2020} baselineYear={2018} onChange={() => {}} />,
    )
    expect(screen.getByText(/differs from baseline year/i)).toBeInTheDocument()
  })

  it('does not show badge when value equals baseline', () => {
    render(
      <YearField label="Year" value={2018} baselineYear={2018} onChange={() => {}} />,
    )
    expect(screen.queryByText(/differs from baseline year/i)).not.toBeInTheDocument()
  })

  it('does not show badge when baselineYear is null', () => {
    render(
      <YearField label="Year" value={2018} baselineYear={null} onChange={() => {}} />,
    )
    expect(screen.queryByText(/differs from baseline year/i)).not.toBeInTheDocument()
  })

  it('emits numeric value via onChange', () => {
    let captured = null
    render(
      <YearField
        label="Year"
        value={2018}
        baselineYear={2018}
        onChange={(v) => { captured = v }}
      />,
    )
    fireEvent.change(screen.getByRole('combobox'), { target: { value: '2019' } })
    expect(captured).toBe(2019)
  })

  it('restricts options to allowedYears when provided', () => {
    render(
      <YearField
        label="Year"
        value={null}
        baselineYear={null}
        allowedYears={[2015, 2016]}
        onChange={() => {}}
      />,
    )
    const options = screen.getAllByRole('option').map((o) => o.textContent)
    expect(options).toEqual(expect.arrayContaining(['2015', '2016']))
    expect(options).not.toContain('2017')
  })
})
```

Ensure testing-library is available — check `frontend/package.json`. If `@testing-library/react` is absent, add it before running the test:

```bash
cd frontend && npm install --save-dev @testing-library/react @testing-library/jest-dom
```

If installed, import `@testing-library/jest-dom` in a setup file. Check `frontend/vite.config.js` for a `test.setupFiles` entry. If missing, create `frontend/src/setupTests.js`:

```js
import '@testing-library/jest-dom'
```

And add `test: { setupFiles: ['./src/setupTests.js'], environment: 'jsdom' }` to `frontend/vite.config.js` under the default export if not already present.

- [ ] **Step 2: Run test to verify it fails**

```bash
cd frontend && npm test -- src/components/__tests__/YearField.test.jsx
```

Expected: FAIL with "Cannot find module '../YearField'".

- [ ] **Step 3: Implement YearField**

Create `frontend/src/components/YearField.jsx`:

```jsx
import { useMemo } from 'react'

const DEFAULT_MIN = 1990
const DEFAULT_MAX = new Date().getFullYear()

export default function YearField({
  label,
  value,
  baselineYear,
  allowedYears,
  onChange,
  id,
  required = false,
}) {
  const options = useMemo(() => {
    if (allowedYears && allowedYears.length > 0) return [...allowedYears].sort((a, b) => b - a)
    const years = []
    for (let y = DEFAULT_MAX; y >= DEFAULT_MIN; y--) years.push(y)
    return years
  }, [allowedYears])

  const showDiffers = baselineYear != null && value != null && value !== baselineYear

  return (
    <div>
      {label && (
        <label htmlFor={id} className="block text-xs text-gray-500 mb-1">
          {label}{required && <span className="text-red-500 ml-0.5">*</span>}
        </label>
      )}
      <select
        id={id}
        value={value ?? ''}
        onChange={(e) => onChange(e.target.value ? Number(e.target.value) : null)}
        className="w-full rounded-lg border border-gray-300 px-3 py-2.5 text-sm
                   focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
      >
        <option value="">Select a year…</option>
        {options.map((y) => (
          <option key={y} value={y}>{y}</option>
        ))}
      </select>
      {showDiffers && (
        <p className="mt-1 text-xs text-amber-600">
          Differs from baseline year ({baselineYear}).
        </p>
      )}
    </div>
  )
}
```

- [ ] **Step 4: Run tests**

```bash
cd frontend && npm test -- src/components/__tests__/YearField.test.jsx
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/YearField.jsx frontend/src/components/__tests__/YearField.test.jsx frontend/package.json frontend/package-lock.json frontend/vite.config.js frontend/src/setupTests.js
git commit -m "feat(components): YearField with baseline-diff badge and allowed-year constraint"
```

---

## Task 3: Step 3 — prefill year from Step 2, show diff badge, require for upload

**Files:**
- Modify: `frontend/src/pages/steps/Step3Population.jsx`

- [ ] **Step 1: Import YearField and replace inline year derivation in `BuiltinPopulationLoader`**

At the top of `Step3Population.jsx`, add the import (around line 3):

```js
import YearField from '../../components/YearField'
```

Delete the `BUILTIN_DATASETS` constant (lines 34-39) — it's a placeholder that never matches real backend entries. The population loader will fall through to the backend's real dataset list in a follow-up; for now it simply takes a year and calls `fetchPopulation`.

Rewrite `BuiltinPopulationLoader` (lines 364-463) to accept a `year` prop instead of the `years` object and drop the dropdown:

```jsx
function BuiltinPopulationLoader({ studyArea, year, onDataLoaded }) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [loadedData, setLoadedData] = useState(null)

  const country = studyArea?.id || ''

  useEffect(() => {
    if (!country || !year) return

    setLoading(true)
    setError(null)
    setLoadedData(null)

    fetchPopulation(country, year)
      .then((data) => {
        if (!data) {
          setError(`No built-in population data for ${studyArea?.name || country} in ${year}.`)
          return
        }
        setLoadedData(data)
        const units = data.units || []
        const total = units.reduce((s, u) => s + (u.total || 0), 0)

        let ageGroups = null
        const firstWithAges = units.find((u) => u.age_groups)
        if (firstWithAges) {
          const ageTotals = {}
          for (const unit of units) {
            if (!unit.age_groups) continue
            for (const [key, val] of Object.entries(unit.age_groups)) {
              ageTotals[key] = (ageTotals[key] || 0) + (val || 0)
            }
          }
          const totalPop = Object.values(ageTotals).reduce((s, v) => s + v, 0)
          if (totalPop > 0) {
            ageGroups = {}
            for (const [key, val] of Object.entries(ageTotals)) {
              const label = key.replace(/^age_/, '').replace(/_/g, '–')
              ageGroups[label] = Math.round((val / totalPop) * 1000) / 10
            }
          }
        }

        onDataLoaded(total, ageGroups)
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false))
  }, [country, year]) // eslint-disable-line react-hooks/exhaustive-deps

  if (!year) {
    return (
      <div className="p-3 bg-gray-50 border border-gray-200 rounded-lg text-sm text-gray-500">
        Set a year above to load population data.
      </div>
    )
  }

  return (
    <div className="space-y-3">
      {loading && (
        <div className="flex items-center gap-2 p-3 bg-blue-50 border border-blue-200 rounded-lg text-sm text-blue-700">
          <svg className="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
          Loading population data…
        </div>
      )}
      {error && (
        <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg text-sm text-amber-700">
          {error}
        </div>
      )}
      {loadedData && !error && (
        <div className="p-3 bg-green-50 border border-green-200 rounded-lg text-sm text-green-700">
          <p className="font-medium">Population data loaded</p>
          <p className="text-xs text-green-600 mt-1">
            {loadedData.units?.length || 0} admin units — total{' '}
            {(loadedData.units || []).reduce((s, u) => s + (u.total || 0), 0).toLocaleString()} people
          </p>
        </div>
      )}
    </div>
  )
}
```

- [ ] **Step 2: Add year picker + prefill logic in `Step3Population`**

Replace the main component (starting at line 467) so:
- Destructure `step2` from the store: `const { step1, step2, step3, setStep3, setStepValidity } = useAnalysisStore()`
- Compute `baselineYear = step2?.baseline?.year ?? null`
- Compute `effectiveYear = step3.year ?? baselineYear`
- Render a `YearField` at the top of the "Exposed Population" fieldset, above the tabs:

```jsx
        <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
          <legend className="text-sm font-semibold text-gray-700 px-1">Exposed Population</legend>

          <div className="mb-4">
            <YearField
              id="step3-year"
              label="Year"
              value={effectiveYear}
              baselineYear={baselineYear}
              required
              onChange={(y) => setStep3({ year: y })}
            />
          </div>

          <TabBar tabs={tabs} activeTab={activeTab} onTabChange={handleTabChange} />

          {/* ...existing tab content... */}
```

- Update validation to require `effectiveYear` when populationType is `file` or `dataset`:

```jsx
  useEffect(() => {
    const hasYear = effectiveYear != null
    const valid =
      (populationType === 'manual' && totalPopulation != null && totalPopulation > 0) ||
      (populationType === 'file' && step3.fileData?.name && !step3.fileData?.error && hasYear) ||
      (populationType === 'dataset' && hasYear)
    setStepValidity(3, valid)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [populationType, totalPopulation, step3.fileData, effectiveYear])
```

- Update the `BuiltinPopulationLoader` invocation (inside the `builtin` tab branch) to pass `year={effectiveYear}` and drop `selectedDatasetId` / `onSelect` props (which no longer exist):

```jsx
          {activeTab === 'builtin' && (
            <BuiltinPopulationLoader
              studyArea={step1.studyArea}
              year={effectiveYear}
              onDataLoaded={(total, ageGroups) => {
                setStep3({
                  totalPopulation: total,
                  ageGroups: ageGroups || step3.ageGroups,
                  populationType: 'dataset',
                })
              }}
            />
          )}
```

- [ ] **Step 3: Manual smoke test**

With Plan 1 merged and dev servers running, walk Mexico → PM2.5 → Step 2 Baseline → WHO AAP → 2018. Advance to Step 3. Verify:
- Year picker prefilled with 2018 (no explicit save yet).
- Changing it to 2015 shows "Differs from baseline year (2018)."
- Setting it back to 2018 hides the badge.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/pages/steps/Step3Population.jsx
git commit -m "feat(step3): year picker prefilled from Step 2, diff badge, drops stale BUILTIN_DATASETS"
```

---

## Task 4: Step 4 — mirror the Step 3 treatment for incidence

**Files:**
- Modify: `frontend/src/pages/steps/Step4HealthData.jsx`

- [ ] **Step 1: Delete the hardcoded BUILTIN_DATASETS and add YearField prefill**

In `Step4HealthData.jsx`:

1. Delete `BUILTIN_DATASETS` constant (lines 17-22).
2. Add `import YearField from '../../components/YearField'`.
3. In the main component, destructure `step2`: `const { step1, step2, step4, setStep4, setStepValidity } = useAnalysisStore()`.
4. Compute `baselineYear = step2?.baseline?.year ?? null` and `effectiveYear = step4.year ?? baselineYear`.
5. Rewrite `BuiltinIncidenceLoader` to accept `year` instead of `years`:

```jsx
function BuiltinIncidenceLoader({ studyArea, year, uniqueEndpoints, onDataLoaded }) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [loadedCount, setLoadedCount] = useState(0)

  const country = studyArea?.id || ''

  useEffect(() => {
    if (!country || !year) return

    setLoading(true)
    setError(null)
    setLoadedCount(0)

    const causes = [...new Set(uniqueEndpoints.filter((ep) => ep.endpoint).map((ep) =>
      ep.endpoint.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, ''),
    ))]
    const causesToTry = ['all', ...causes]

    Promise.all(
      causesToTry.map((cause) =>
        fetchIncidence(country, cause, year).catch(() => null),
      ),
    )
      .then((results) => {
        const allUnits = results.filter(Boolean).flatMap((r) => r.units || [])
        if (allUnits.length === 0) {
          setError(`No built-in incidence data for ${studyArea?.name || country} in ${year}.`)
          return
        }
        const ratesMap = {}
        let matched = 0
        for (const ep of uniqueEndpoints) {
          const epLower = (ep.endpoint || '').toLowerCase()
          const match = allUnits.find((u) => {
            const cause = (u.cause || '').toLowerCase()
            return epLower.includes(cause) || cause.includes(epLower.split(' ')[0])
          })
          if (match && match.incidence_rate != null) {
            ratesMap[ep.id] = match.incidence_rate
            matched++
          }
        }
        setLoadedCount(matched)
        if (matched > 0) onDataLoaded(ratesMap)
        else setError(`No built-in incidence data matched endpoints for ${studyArea?.name || country} in ${year}.`)
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false))
  }, [country, year]) // eslint-disable-line react-hooks/exhaustive-deps

  if (!year) {
    return (
      <div className="p-3 bg-gray-50 border border-gray-200 rounded-lg text-sm text-gray-500">
        Set a year above to load incidence data.
      </div>
    )
  }

  return (
    <div className="space-y-3">
      {loading && (
        <div className="flex items-center gap-2 p-3 bg-blue-50 border border-blue-200 rounded-lg text-sm text-blue-700">
          <svg className="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
          Loading incidence data…
        </div>
      )}
      {error && (
        <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg text-sm text-amber-700">{error}</div>
      )}
      {loadedCount > 0 && !error && !loading && (
        <div className="p-3 bg-green-50 border border-green-200 rounded-lg text-sm text-green-700">
          <p className="font-medium">Incidence data loaded</p>
          <p className="text-xs text-green-600 mt-1">
            Pre-filled rates for {loadedCount} of {uniqueEndpoints.length} endpoints
          </p>
        </div>
      )}
    </div>
  )
}
```

6. Add `YearField` above the tabs inside the "Baseline Incidence Rates" fieldset and update validation:

```jsx
        <fieldset className="bg-white rounded-xl shadow-sm border border-gray-200 p-5">
          <legend className="text-sm font-semibold text-gray-700 px-1">Baseline Incidence Rates</legend>

          <div className="mb-4">
            <YearField
              id="step4-year"
              label="Year"
              value={effectiveYear}
              baselineYear={baselineYear}
              required
              onChange={(y) => setStep4({ year: y })}
            />
          </div>

          <TabBar tabs={tabs} activeTab={activeTab} onTabChange={handleTabChange} />

          {/* ... */}
```

Update validation (around line 389-406):

```jsx
  useEffect(() => {
    if (!pollutant) {
      setStepValidity(4, false)
      return
    }
    const hasYear = effectiveYear != null
    let valid = false
    if (incidenceType === 'manual') {
      valid = currentRates && Object.values(currentRates).some((v) => v != null && v !== '' && v > 0)
    } else if (incidenceType === 'file') {
      valid = step4.fileData?.name && !step4.fileData?.error && hasYear
    } else if (incidenceType === 'dataset') {
      valid = hasYear
    }
    setStepValidity(4, valid)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [incidenceType, currentRates, step4.fileData, effectiveYear, pollutant])
```

Update the `BuiltinIncidenceLoader` invocation to pass `year={effectiveYear}` and drop the dataset-id props.

- [ ] **Step 2: Run tests**

```bash
cd frontend && npm test
```

Expected: PASS.

- [ ] **Step 3: Manual smoke test**

Walk the wizard through Step 4 and confirm the year prefills from Step 2 and the diff badge appears/disappears correctly.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/pages/steps/Step4HealthData.jsx
git commit -m "feat(step4): year picker prefilled from Step 2, diff badge, drops stale BUILTIN_DATASETS"
```

---

## Task 5: Step 2 upload year input

**Files:**
- Modify: `frontend/src/pages/steps/Step2AirQuality.jsx`

- [ ] **Step 1: Add year input alongside baseline and control file uploads**

Just below each `FileDropzone` invocation (lines around 579-585 for baseline, 655-661 for control), add a `YearField` bound to the appropriate year field. Import it at the top:

```js
import YearField from '../../components/YearField'
```

Baseline — replace the current Upload branch (around line 578-585) with:

```jsx
          {baselineTab === 'upload' && (
            <div className="space-y-4">
              <FileDropzone
                fileData={baseline.fileData}
                onFile={handleBaselineFile}
                onClear={handleClearBaselineFile}
              />
              {baseline.fileData?.name && !baseline.fileData?.error && (
                <YearField
                  id="baseline-upload-year"
                  label="Year of uploaded data"
                  value={baseline.year}
                  baselineYear={null}
                  required
                  onChange={(y) =>
                    setStep2({ baseline: { ...baseline, year: y, type: 'file' } })
                  }
                />
              )}
            </div>
          )}
```

Control — same pattern:

```jsx
              {controlTab === 'upload' && (
                <div className="space-y-4">
                  <FileDropzone
                    fileData={control.fileData}
                    onFile={handleControlFile}
                    onClear={handleClearControlFile}
                  />
                  {control.fileData?.name && !control.fileData?.error && (
                    <YearField
                      id="control-upload-year"
                      label="Year of uploaded data"
                      value={control.year}
                      baselineYear={baseline.year}
                      required
                      onChange={(y) =>
                        setStep2({ control: { ...control, year: y, type: 'file' } })
                      }
                    />
                  )}
                </div>
              )}
```

- [ ] **Step 2: Update Step 2 validation so file-type baseline requires year**

Edit the validation effect (from Plan 1 Task 4 Step 4):

```jsx
  useEffect(() => {
    const hasBaseline =
      (baseline.type === 'manual' && baseline.value != null && baseline.value !== '' && baseline.value >= 0) ||
      (baseline.type === 'dataset' && baseline.datasetId != null && baseline.year != null) ||
      (baseline.type === 'file' && baseline.fileData?.name && !baseline.fileData?.error && baseline.year != null)
    setStepValidity(2, hasBaseline)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [baseline])
```

- [ ] **Step 3: Commit**

```bash
git add frontend/src/pages/steps/Step2AirQuality.jsx
git commit -m "feat(step2): require year input for concentration file uploads"
```

---

## Task 6: Step 3 upload year input

**Files:**
- Modify: `frontend/src/pages/steps/Step3Population.jsx`

The existing `CsvUpload` dropzone sits inside the `upload` tab. Since Step 3's year picker is at the fieldset level (Task 3), the upload already requires `effectiveYear != null`. No further UI changes are strictly needed — but add a helper note so users understand what the year applies to when they upload.

- [ ] **Step 1: Add a hint below the Step 3 YearField when populationType === 'file'**

Inside the component, below the `<YearField>`, add:

```jsx
          {populationType === 'file' && (
            <p className="mt-1 text-xs text-gray-500">
              Applies to the uploaded file.
            </p>
          )}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/pages/steps/Step3Population.jsx
git commit -m "feat(step3): contextual hint that year applies to uploaded file"
```

---

## Task 7: Step 4 upload year input

**Files:**
- Modify: `frontend/src/pages/steps/Step4HealthData.jsx`

Same pattern as Task 6.

- [ ] **Step 1: Add contextual hint below Step 4 YearField when incidenceType === 'file'**

```jsx
          {incidenceType === 'file' && (
            <p className="mt-1 text-xs text-gray-500">
              Applies to the uploaded rate file.
            </p>
          )}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/pages/steps/Step4HealthData.jsx
git commit -m "feat(step4): contextual hint that year applies to uploaded file"
```

---

## Task 8: Regression sweep

- [ ] **Step 1: Frontend tests**

```bash
cd frontend && npm test
```

Expected: PASS (YearField tests, existing App tests).

- [ ] **Step 2: Backend tests** (should be untouched but confirm)

```bash
venv/Scripts/python.exe -m pytest backend/tests/ -v
```

- [ ] **Step 3: End-to-end manual smoke**

Walk Mexico → PM2.5 → Step 2 Baseline Built-in → WHO AAP → 2018 → Step 3 (year prefilled 2018, set total pop manually to skip built-in for now) → Step 4 (year prefilled 2018) → run analysis. Confirm PDF export shows Year: 2018.

---

## Spec alignment check

| Spec item | Task |
|-----------|------|
| D4: Step 2 baseline year defaults to Steps 3/4 | Task 1 (store), 3 (Step 3), 4 (Step 4) |
| D4: "Differs from baseline" badge | Task 2 (YearField), 3, 4 |
| D5: Upload year for concentration | Task 5 |
| D5: Upload year for population | Task 6 (implicit — uses Step 3 YearField) |
| D5: Upload year for incidence | Task 7 (implicit — uses Step 4 YearField) |

Out-of-scope for Plan 2 (handled by Plan 3): D6 post-results multi-year, `results` list restructure.
