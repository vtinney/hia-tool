# EJ-framed HIA template implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the existing `us_tract_pm25_ej` template to render a tract-level Environmental Justice Context section on the Results page, without modifying the wizard or introducing a standard-vs-EJ mode toggle.

**Architecture:** The template JSON gains an `ejFraming: true` marker that flows into the Zustand store via `loadFromTemplate`. On the Results page, a new `<EJContextSection />` renders iff the store flag is set, the study area is a US admin boundary, and the analysis payload carries `per_tract_results` from the parallel agent's tract-resolution engine work. The section fetches tract-level ACS demographics via the existing `/api/data/demographics/{country}/{year}` endpoint, joins to `per_tract_results` by `tract_fips`, computes population-weighted aggregate stats, and renders a MapBox choropleth with a field toggle.

**Tech Stack:** React 18, Zustand 4, Vitest + @testing-library/react, MapBox GL JS 3. Existing test setup at `frontend/src/setupTests.js`.

**Spec:** `docs/superpowers/specs/2026-04-21-ej-template-design.md`

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `frontend/src/data/templates/us_tract_pm25_ej.json` | Modify | Add `"ejFraming": true` top-level marker |
| `frontend/src/stores/useAnalysisStore.js` | Modify | Add `ejFraming` top-level field, propagate in `loadFromTemplate`, persist, bump version |
| `frontend/src/stores/__tests__/useAnalysisStore.test.js` | Create | Unit tests for `ejFraming` behavior |
| `frontend/src/lib/demographics.js` | Create | Pure functions: `populationWeightedMean`, `pickVintage`, `studyAreaToFilter` |
| `frontend/src/lib/__tests__/demographics.test.js` | Create | Unit tests for the three pure functions |
| `frontend/src/lib/api.js` | Modify | Add `fetchDemographics(country, year, opts)` |
| `frontend/src/lib/__tests__/api.test.js` | Create | Unit test for `fetchDemographics` |
| `frontend/src/components/TractChoroplethMap.jsx` | Create | MapBox component; field-agnostic tract choropleth |
| `frontend/src/components/EJContextSection.jsx` | Create | Fetch + join + aggregate + render |
| `frontend/src/components/__tests__/EJContextSection.test.jsx` | Create | Component tests: gate, render, fallback banner, fetch error |
| `frontend/src/pages/Results.jsx` | Modify | Conditionally render `<EJContextSection />` per gate |

---

## Task 1: Store — add `ejFraming` field

**Files:**
- Modify: `frontend/src/stores/useAnalysisStore.js`
- Create: `frontend/src/stores/__tests__/useAnalysisStore.test.js`

- [ ] **Step 1: Write the failing test**

Create `frontend/src/stores/__tests__/useAnalysisStore.test.js`:

```javascript
import { describe, it, expect, beforeEach } from 'vitest'
import useAnalysisStore from '../useAnalysisStore'

describe('useAnalysisStore — ejFraming', () => {
  beforeEach(() => {
    useAnalysisStore.getState().reset()
  })

  it('initial state has ejFraming=false', () => {
    expect(useAnalysisStore.getState().ejFraming).toBe(false)
  })

  it('loadFromTemplate propagates ejFraming=true when template sets it', () => {
    useAnalysisStore.getState().loadFromTemplate({
      ejFraming: true,
      step1: { studyArea: { type: 'country', id: 'united-states', name: 'United States' } },
    })
    expect(useAnalysisStore.getState().ejFraming).toBe(true)
  })

  it('loadFromTemplate defaults ejFraming to false when template omits it', () => {
    useAnalysisStore.getState().loadFromTemplate({
      step1: { studyArea: { type: 'country', id: 'mexico', name: 'Mexico' } },
    })
    expect(useAnalysisStore.getState().ejFraming).toBe(false)
  })

  it('reset() clears ejFraming back to false', () => {
    useAnalysisStore.getState().loadFromTemplate({ ejFraming: true })
    expect(useAnalysisStore.getState().ejFraming).toBe(true)
    useAnalysisStore.getState().reset()
    expect(useAnalysisStore.getState().ejFraming).toBe(false)
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run from `frontend/`: `npm test -- useAnalysisStore`
Expected: FAIL — `ejFraming` is undefined (first test), or stays undefined after `loadFromTemplate`.

- [ ] **Step 3: Modify `initialState()` to include `ejFraming: false`**

In `frontend/src/stores/useAnalysisStore.js`, update `initialState()` (around line 75):

```javascript
function initialState() {
  return {
    // Wizard navigation
    currentStep: 1,
    totalSteps: 7,
    completedSteps: [],
    stepValidity: defaultStepValidity(),

    // Analysis configuration – one key per step
    step1: { ...DEFAULT_STEP1 },
    step2: { ...DEFAULT_STEP2, baseline: { ...DEFAULT_STEP2.baseline }, control: { ...DEFAULT_STEP2.control } },
    step3: { ...DEFAULT_STEP3 },
    step4: { ...DEFAULT_STEP4 },
    step5: { ...DEFAULT_STEP5, selectedCRFs: [] },
    step6: { ...DEFAULT_STEP6 },
    step7: { ...DEFAULT_STEP7 },

    // EJ framing (set by template; gates Results-page EJ section)
    ejFraming: false,

    // Results from the backend
    results: null,
  }
}
```

- [ ] **Step 4: Update `loadFromTemplate` to propagate `ejFraming`**

In `frontend/src/stores/useAnalysisStore.js`, update `loadFromTemplate` (around line 154):

```javascript
      loadFromTemplate: (config) => {
        const next = initialState()

        for (let i = 1; i <= 7; i++) {
          const key = `step${i}`
          if (config[key]) {
            next[key] = { ...STEP_DEFAULTS[key], ...config[key] }
          }
        }

        if (config.completedSteps) next.completedSteps = config.completedSteps
        if (config.stepValidity) next.stepValidity = { ...defaultStepValidity(), ...config.stepValidity }
        next.ejFraming = config.ejFraming === true

        set(next)
      },
```

- [ ] **Step 5: Update `partialize` and bump persist version**

In `frontend/src/stores/useAnalysisStore.js`, update the `persist` config (around line 179):

```javascript
    {
      name: 'hia-analysis',
      version: 7,
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
        ejFraming: state.ejFraming,
      }),
      migrate: (persisted, version) => {
        // v6 and older: hard reset is simpler than partial upgrade.
        // v7 added ejFraming.
        if (version < 7) return initialState()
        return persisted
      },
    },
```

- [ ] **Step 6: Run test to verify it passes**

Run: `npm test -- useAnalysisStore`
Expected: PASS (4 tests).

- [ ] **Step 7: Commit**

```bash
git add frontend/src/stores/useAnalysisStore.js frontend/src/stores/__tests__/useAnalysisStore.test.js
git commit -m "feat(store): add ejFraming flag for EJ-template gating"
```

---

## Task 2: Template — add `ejFraming: true` to EJ template

**Files:**
- Modify: `frontend/src/data/templates/us_tract_pm25_ej.json`

- [ ] **Step 1: Write a failing test**

Append to `frontend/src/stores/__tests__/useAnalysisStore.test.js` (inside the same describe):

```javascript
  it('the us_tract_pm25_ej template carries ejFraming=true', async () => {
    const tpl = await import('../../data/templates/us_tract_pm25_ej.json')
    expect(tpl.default.ejFraming).toBe(true)
  })

  it('the us_national_pm25 template does NOT carry ejFraming', async () => {
    const tpl = await import('../../data/templates/us_national_pm25.json')
    expect(tpl.default.ejFraming).toBeUndefined()
  })
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- useAnalysisStore`
Expected: FAIL on the first new test (`ejFraming` is undefined on the template).

- [ ] **Step 3: Add `ejFraming: true` to the EJ template**

Edit `frontend/src/data/templates/us_tract_pm25_ej.json` — insert as a top-level field between `"description"` and `"step1"`:

```json
{
  "name": "U.S. Census Tract PM₂.₅ (Environmental Justice)",
  "description": "Tract-level spatial analysis for environmental justice screening. Requires gridded PM₂.₅ and population raster uploads. Uses Di 2017 CRFs.",
  "ejFraming": true,
  "step1": {
    "studyArea": { "type": "country", "id": "united-states", "name": "United States" },
    "pollutant": "pm25",
    "years": { "start": 2020, "end": 2020 },
    "analysisName": "U.S. Tract-Level PM₂.₅ — EJ Analysis"
  },
```
(remainder of file unchanged)

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- useAnalysisStore`
Expected: PASS (6 tests total).

- [ ] **Step 5: Commit**

```bash
git add frontend/src/data/templates/us_tract_pm25_ej.json frontend/src/stores/__tests__/useAnalysisStore.test.js
git commit -m "feat(templates): mark us_tract_pm25_ej with ejFraming flag"
```

---

## Task 3: `lib/demographics.js` — `populationWeightedMean`

**Files:**
- Create: `frontend/src/lib/demographics.js`
- Create: `frontend/src/lib/__tests__/demographics.test.js`

- [ ] **Step 1: Write the failing test**

Create `frontend/src/lib/__tests__/demographics.test.js`:

```javascript
import { describe, it, expect } from 'vitest'
import { populationWeightedMean } from '../demographics'

describe('populationWeightedMean', () => {
  it('computes population-weighted mean across tracts', () => {
    const tracts = [
      { total_pop: 1000, pct_minority: 0.5 },
      { total_pop: 3000, pct_minority: 0.9 },
    ]
    // (1000*0.5 + 3000*0.9) / 4000 = 3200/4000 = 0.8
    expect(populationWeightedMean(tracts, 'pct_minority')).toBeCloseTo(0.8, 5)
  })

  it('skips tracts where the target field is NaN', () => {
    const tracts = [
      { total_pop: 1000, pct_minority: 0.5 },
      { total_pop: 3000, pct_minority: NaN },
    ]
    expect(populationWeightedMean(tracts, 'pct_minority')).toBeCloseTo(0.5, 5)
  })

  it('skips tracts where the target field is null or undefined', () => {
    const tracts = [
      { total_pop: 1000, pct_minority: 0.5 },
      { total_pop: 3000, pct_minority: null },
      { total_pop: 2000 },
    ]
    expect(populationWeightedMean(tracts, 'pct_minority')).toBeCloseTo(0.5, 5)
  })

  it('returns null when all tracts have NaN for the field', () => {
    const tracts = [
      { total_pop: 1000, pct_minority: NaN },
      { total_pop: 3000, pct_minority: NaN },
    ]
    expect(populationWeightedMean(tracts, 'pct_minority')).toBeNull()
  })

  it('returns null for empty tract array', () => {
    expect(populationWeightedMean([], 'pct_minority')).toBeNull()
  })

  it('treats zero-population tracts as weight 0', () => {
    const tracts = [
      { total_pop: 0, pct_minority: 0.99 },
      { total_pop: 1000, pct_minority: 0.3 },
    ]
    expect(populationWeightedMean(tracts, 'pct_minority')).toBeCloseTo(0.3, 5)
  })

  it('returns null when all weights sum to zero', () => {
    const tracts = [
      { total_pop: 0, pct_minority: 0.5 },
      { total_pop: 0, pct_minority: 0.8 },
    ]
    expect(populationWeightedMean(tracts, 'pct_minority')).toBeNull()
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- demographics`
Expected: FAIL — module not found.

- [ ] **Step 3: Create the module with `populationWeightedMean`**

Create `frontend/src/lib/demographics.js`:

```javascript
/**
 * Population-weighted mean of a field across an array of tract-like objects.
 *
 * Skips tracts where the target field is NaN / null / undefined. Tracts with
 * zero population naturally drop out of the weighted mean. Returns null when
 * no valid (field, weight) pairs exist.
 *
 * @param {Array<{total_pop: number}>} tracts
 * @param {string} field - Property name on each tract to aggregate.
 * @returns {number|null}
 */
export function populationWeightedMean(tracts, field) {
  let numerator = 0
  let denominator = 0
  for (const t of tracts) {
    const v = t[field]
    if (v == null || Number.isNaN(v)) continue
    const w = Number(t.total_pop) || 0
    numerator += w * v
    denominator += w
  }
  if (denominator === 0) return null
  return numerator / denominator
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- demographics`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add frontend/src/lib/demographics.js frontend/src/lib/__tests__/demographics.test.js
git commit -m "feat(lib): add populationWeightedMean for tract aggregation"
```

---

## Task 4: `lib/demographics.js` — `pickVintage`

**Files:**
- Modify: `frontend/src/lib/demographics.js`
- Modify: `frontend/src/lib/__tests__/demographics.test.js`

- [ ] **Step 1: Write the failing test**

Append to `frontend/src/lib/__tests__/demographics.test.js`:

```javascript
import { pickVintage } from '../demographics'

describe('pickVintage', () => {
  const ALL = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]

  it('returns exact match when available', () => {
    expect(pickVintage(2022, ALL)).toBe(2022)
  })

  it('for pre-2020 years with no exact match, falls back down same side', () => {
    // Pretend 2017 is missing; year 2017 should fall back to 2016 (closer than 2018 on the other side is still fine because both are pre-2020)
    expect(pickVintage(2017, [2015, 2016, 2018, 2019, 2020, 2021])).toBe(2018)
  })

  it('never crosses the 2019 → 2020 boundary', () => {
    // Ask for 2019 but only 2020+ available: return the closest post-2020, NOT cross back.
    // pickVintage is about minimizing tract-boundary mismatch; crossing is the last resort.
    expect(pickVintage(2019, [2020, 2021, 2022])).toBe(2020)
  })

  it('prefers same-side-of-2020 match over closer across-boundary match', () => {
    // Ask 2019, available = [2018, 2020]. Both are distance 1, but 2018 is same-side (pre-2020) so win.
    expect(pickVintage(2019, [2018, 2020])).toBe(2018)
  })

  it('for 2020+ years, prefers closest post-2020 vintage', () => {
    expect(pickVintage(2023, [2020, 2021, 2022])).toBe(2022)
  })

  it('returns null when availableVintages is empty', () => {
    expect(pickVintage(2022, [])).toBeNull()
  })

  it('returns the only available vintage even if far from target', () => {
    expect(pickVintage(2030, [2020])).toBe(2020)
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- demographics`
Expected: FAIL — `pickVintage` not exported.

- [ ] **Step 3: Add `pickVintage` to `frontend/src/lib/demographics.js`**

Append to the file:

```javascript
/**
 * Select the best ACS vintage for an analysis year, preferring an exact
 * match, then the nearest vintage on the same side of the 2020 tract-
 * boundary redraw, then crossing the boundary only as a last resort.
 *
 * Why: 2015-2019 use the pre-2020 tract geometry (~73.7k tracts), and
 * 2020+ use the post-decennial redraw (~85k tracts). Silent cross-boundary
 * fallback would mismatch tract FIPS and distort downstream joins.
 *
 * @param {number} analysisYear
 * @param {number[]} availableVintages
 * @returns {number|null}
 */
export function pickVintage(analysisYear, availableVintages) {
  if (!availableVintages || availableVintages.length === 0) return null
  if (availableVintages.includes(analysisYear)) return analysisYear

  const sideOf = (y) => (y < 2020 ? 'pre' : 'post')
  const targetSide = sideOf(analysisYear)

  const sameSide = availableVintages.filter((y) => sideOf(y) === targetSide)
  const pool = sameSide.length > 0 ? sameSide : availableVintages

  let best = pool[0]
  let bestDist = Math.abs(best - analysisYear)
  for (const y of pool) {
    const d = Math.abs(y - analysisYear)
    if (d < bestDist) {
      best = y
      bestDist = d
    }
  }
  return best
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- demographics`
Expected: PASS (14 tests total across `populationWeightedMean` and `pickVintage`).

- [ ] **Step 5: Commit**

```bash
git add frontend/src/lib/demographics.js frontend/src/lib/__tests__/demographics.test.js
git commit -m "feat(lib): add pickVintage with 2020-boundary fallback"
```

---

## Task 5: `lib/demographics.js` — `studyAreaToFilter`

**Files:**
- Modify: `frontend/src/lib/demographics.js`
- Modify: `frontend/src/lib/__tests__/demographics.test.js`

- [ ] **Step 1: Write the failing test**

Append to `frontend/src/lib/__tests__/demographics.test.js`:

```javascript
import { studyAreaToFilter } from '../demographics'

describe('studyAreaToFilter', () => {
  it('returns empty filter for nationwide US', () => {
    const sa = { type: 'country', id: 'united-states', name: 'United States' }
    expect(studyAreaToFilter(sa)).toEqual({})
  })

  it('returns state filter for us-state id (e.g. us-48 for Texas)', () => {
    const sa = { type: 'state', id: 'us-48', name: 'Texas' }
    expect(studyAreaToFilter(sa)).toEqual({ state: '48' })
  })

  it('returns state+county filter for us-county id (e.g. us-48-201 for Harris County)', () => {
    const sa = { type: 'county', id: 'us-48-201', name: 'Harris County, Texas' }
    expect(studyAreaToFilter(sa)).toEqual({ state: '48', county: '201' })
  })

  it('returns null for non-US country', () => {
    const sa = { type: 'country', id: 'mexico', name: 'Mexico' }
    expect(studyAreaToFilter(sa)).toBeNull()
  })

  it('returns null for non-admin-boundary types', () => {
    expect(studyAreaToFilter({ type: 'polygon', id: '', name: '' })).toBeNull()
    expect(studyAreaToFilter({ type: 'upload', id: '', name: '' })).toBeNull()
  })

  it('returns null for malformed state id', () => {
    expect(studyAreaToFilter({ type: 'state', id: 'texas', name: 'Texas' })).toBeNull()
    expect(studyAreaToFilter({ type: 'state', id: 'us-4', name: '' })).toBeNull()
  })

  it('returns null for malformed county id', () => {
    expect(studyAreaToFilter({ type: 'county', id: 'us-48', name: '' })).toBeNull()
    expect(studyAreaToFilter({ type: 'county', id: 'us-48-20', name: '' })).toBeNull()
  })

  it('returns null for undefined or null studyArea', () => {
    expect(studyAreaToFilter(null)).toBeNull()
    expect(studyAreaToFilter(undefined)).toBeNull()
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- demographics`
Expected: FAIL — `studyAreaToFilter` not exported.

- [ ] **Step 3: Add `studyAreaToFilter` to `frontend/src/lib/demographics.js`**

Append to the file:

```javascript
/**
 * Derive a demographics endpoint filter object from a study area.
 *
 * Returns:
 *   - {} for nationwide US (type='country', id='united-states')
 *   - {state: 'XX'} for type='state' with id 'us-XX'
 *   - {state: 'XX', county: 'YYY'} for type='county' with id 'us-XX-YYY'
 *   - null for non-US or non-admin-boundary types, which should NOT render
 *     the EJ section.
 *
 * FIPS codes are returned as strings (state 2 digits, county 3 digits) to
 * match the `/api/data/demographics` query param shape.
 *
 * @param {{type: string, id: string}|null|undefined} studyArea
 * @returns {{state?: string, county?: string}|null}
 */
export function studyAreaToFilter(studyArea) {
  if (!studyArea) return null
  const { type, id } = studyArea

  if (type === 'country' && id === 'united-states') return {}

  if (type === 'state') {
    const m = /^us-(\d{2})$/.exec(id ?? '')
    if (!m) return null
    return { state: m[1] }
  }

  if (type === 'county') {
    const m = /^us-(\d{2})-(\d{3})$/.exec(id ?? '')
    if (!m) return null
    return { state: m[1], county: m[2] }
  }

  return null
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- demographics`
Expected: PASS (22 tests total).

- [ ] **Step 5: Commit**

```bash
git add frontend/src/lib/demographics.js frontend/src/lib/__tests__/demographics.test.js
git commit -m "feat(lib): add studyAreaToFilter for demographics endpoint"
```

---

## Task 6: `lib/api.js` — `fetchDemographics`

**Files:**
- Modify: `frontend/src/lib/api.js`
- Create: `frontend/src/lib/__tests__/api.test.js`

- [ ] **Step 1: Write the failing test**

Create `frontend/src/lib/__tests__/api.test.js`:

```javascript
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { fetchDemographics } from '../api'

describe('fetchDemographics', () => {
  let fetchSpy

  beforeEach(() => {
    fetchSpy = vi.spyOn(global, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
  })

  function ok(payload) {
    return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(payload) })
  }

  it('calls the endpoint with no query params for nationwide', async () => {
    fetchSpy.mockReturnValueOnce(ok({ type: 'FeatureCollection', features: [] }))
    const res = await fetchDemographics('us', 2022)
    expect(fetchSpy).toHaveBeenCalledWith('/api/data/demographics/us/2022')
    expect(res).toEqual({ type: 'FeatureCollection', features: [] })
  })

  it('includes state when provided', async () => {
    fetchSpy.mockReturnValueOnce(ok({ type: 'FeatureCollection', features: [] }))
    await fetchDemographics('us', 2022, { state: '48' })
    expect(fetchSpy).toHaveBeenCalledWith('/api/data/demographics/us/2022?state=48')
  })

  it('includes state and county when both provided', async () => {
    fetchSpy.mockReturnValueOnce(ok({ type: 'FeatureCollection', features: [] }))
    await fetchDemographics('us', 2022, { state: '48', county: '201' })
    expect(fetchSpy).toHaveBeenCalledWith(
      '/api/data/demographics/us/2022?state=48&county=201',
    )
  })

  it('includes simplify when provided', async () => {
    fetchSpy.mockReturnValueOnce(ok({ type: 'FeatureCollection', features: [] }))
    await fetchDemographics('us', 2022, { state: '48', simplify: 0 })
    expect(fetchSpy).toHaveBeenCalledWith(
      '/api/data/demographics/us/2022?state=48&simplify=0',
    )
  })

  it('returns null on 404', async () => {
    fetchSpy.mockReturnValueOnce(Promise.resolve({ ok: false, status: 404 }))
    const res = await fetchDemographics('us', 1999)
    expect(res).toBeNull()
  })

  it('throws on non-404 error', async () => {
    fetchSpy.mockReturnValueOnce(Promise.resolve({ ok: false, status: 500 }))
    await expect(fetchDemographics('us', 2022)).rejects.toThrow(/500/)
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- api`
Expected: FAIL — `fetchDemographics` not exported.

- [ ] **Step 3: Add `fetchDemographics` to `frontend/src/lib/api.js`**

Append to `frontend/src/lib/api.js` (after the other `fetch*` helpers):

```javascript
/**
 * Fetch ACS 5-year tract demographics for a country/year.
 *
 * @param {string} country - Country slug (e.g. 'us').
 * @param {number} year - ACS 5-year vintage (end year).
 * @param {object} [opts]
 * @param {string} [opts.state] - 2-digit state FIPS filter.
 * @param {string} [opts.county] - 3-digit county FIPS filter (requires state).
 * @param {number} [opts.simplify] - Geometry simplification tolerance in degrees.
 * @returns {Promise<object|null>} GeoJSON FeatureCollection, or null on 404.
 */
export async function fetchDemographics(country, year, opts = {}) {
  const params = new URLSearchParams()
  if (opts.state) params.set('state', opts.state)
  if (opts.county) params.set('county', opts.county)
  if (opts.simplify !== undefined) params.set('simplify', String(opts.simplify))
  const qs = params.toString()
  const res = await fetch(
    `${API_BASE}/data/demographics/${country}/${year}${qs ? `?${qs}` : ''}`,
  )
  if (!res.ok) {
    if (res.status === 404) return null
    throw new Error(`Failed to fetch demographics: ${res.status}`)
  }
  return res.json()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- api`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add frontend/src/lib/api.js frontend/src/lib/__tests__/api.test.js
git commit -m "feat(api): add fetchDemographics wrapper"
```

---

## Task 7: `components/TractChoroplethMap.jsx`

**Files:**
- Create: `frontend/src/components/TractChoroplethMap.jsx`

This is a thin wrapper around MapBox GL that renders a `FeatureCollection` of tract polygons, colored by a configurable numeric property. It's deliberately minimal and field-agnostic — no tests here, because MapBox GL doesn't render in jsdom. Behavioral verification lives in manual-verification step at the end.

- [ ] **Step 1: Create the component**

Create `frontend/src/components/TractChoroplethMap.jsx`:

```jsx
import { useEffect, useRef } from 'react'
import mapboxgl from 'mapbox-gl'
import 'mapbox-gl/dist/mapbox-gl.css'

// Color ramp for percentage fields (0.0 – 1.0). 7 stops from light to dark.
const PCT_RAMP = [
  0.0, '#f7fbff',
  0.15, '#deebf7',
  0.30, '#c6dbef',
  0.45, '#9ecae1',
  0.60, '#6baed6',
  0.75, '#3182bd',
  0.90, '#08519c',
]

/**
 * Render a FeatureCollection of tract polygons as a choropleth on a MapBox GL map.
 *
 * @param {object} props
 * @param {object} props.geojson - FeatureCollection with numeric `field` on each feature.
 * @param {string} props.field - Property name to drive choropleth color.
 * @param {string} [props.accessToken] - Mapbox access token. Falls back to VITE_MAPBOX_TOKEN env.
 * @param {[number, number, number, number]} [props.bbox] - Optional fitBounds bbox.
 */
export default function TractChoroplethMap({ geojson, field, accessToken, bbox }) {
  const containerRef = useRef(null)
  const mapRef = useRef(null)

  useEffect(() => {
    if (!containerRef.current) return
    const token = accessToken || import.meta.env.VITE_MAPBOX_TOKEN
    if (!token) {
      console.warn('TractChoroplethMap: no Mapbox token; map will not render')
      return
    }
    mapboxgl.accessToken = token

    const map = new mapboxgl.Map({
      container: containerRef.current,
      style: 'mapbox://styles/mapbox/light-v11',
      center: [-98, 39],
      zoom: 3,
    })
    mapRef.current = map

    map.on('load', () => {
      map.addSource('tracts', { type: 'geojson', data: geojson })
      map.addLayer({
        id: 'tracts-fill',
        type: 'fill',
        source: 'tracts',
        paint: {
          'fill-color': [
            'case',
            ['!=', ['typeof', ['get', field]], 'number'],
            '#eeeeee', // no-data hatch color (flat fallback)
            ['interpolate', ['linear'], ['get', field], ...PCT_RAMP],
          ],
          'fill-opacity': 0.75,
        },
      })
      map.addLayer({
        id: 'tracts-line',
        type: 'line',
        source: 'tracts',
        paint: { 'line-color': '#ffffff', 'line-width': 0.3, 'line-opacity': 0.4 },
      })

      if (bbox) {
        map.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], { padding: 24, duration: 0 })
      }
    })

    return () => {
      map.remove()
      mapRef.current = null
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Update data when geojson changes
  useEffect(() => {
    const map = mapRef.current
    if (!map || !map.getSource || !map.getSource('tracts')) return
    map.getSource('tracts').setData(geojson)
  }, [geojson])

  // Update paint expression when field changes
  useEffect(() => {
    const map = mapRef.current
    if (!map || !map.getLayer || !map.getLayer('tracts-fill')) return
    map.setPaintProperty('tracts-fill', 'fill-color', [
      'case',
      ['!=', ['typeof', ['get', field]], 'number'],
      '#eeeeee',
      ['interpolate', ['linear'], ['get', field], ...PCT_RAMP],
    ])
  }, [field])

  return <div ref={containerRef} className="w-full h-[480px] rounded-xl overflow-hidden" />
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/TractChoroplethMap.jsx
git commit -m "feat(components): add TractChoroplethMap choropleth wrapper"
```

---

## Task 8: `components/EJContextSection.jsx`

**Files:**
- Create: `frontend/src/components/EJContextSection.jsx`
- Create: `frontend/src/components/__tests__/EJContextSection.test.jsx`

EJContextSection owns the full EJ rendering: fetch demographics, join to `per_tract_results`, compute aggregates, render UI. Tests mock the fetch and skip the map (the map renders a placeholder in jsdom).

- [ ] **Step 1: Write failing tests**

Create `frontend/src/components/__tests__/EJContextSection.test.jsx`:

```jsx
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import EJContextSection from '../EJContextSection'

// Stub the choropleth — MapBox doesn't render in jsdom.
vi.mock('../TractChoroplethMap', () => ({
  default: ({ field }) => <div data-testid="choropleth" data-field={field} />,
}))

function mockGeojson(features) {
  return { type: 'FeatureCollection', features }
}

function tract(geoid, total_pop, pct_minority, pct_below_200_pov) {
  return {
    type: 'Feature',
    properties: { geoid, total_pop, pct_minority, pct_below_200_pov },
    geometry: { type: 'Polygon', coordinates: [[[0,0],[0,1],[1,1],[1,0],[0,0]]] },
  }
}

describe('EJContextSection', () => {
  let fetchSpy

  beforeEach(() => {
    fetchSpy = vi.spyOn(global, 'fetch')
  })

  afterEach(() => {
    fetchSpy.mockRestore()
  })

  function okJson(payload) {
    return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(payload) })
  }

  const studyArea = { type: 'country', id: 'united-states', name: 'United States' }
  const perTractResults = [
    { tract_fips: '01', population: 1000, baseline_concentration: 12, control_concentration: 9,
      attributable_cases: { mean: 2, lower95: 1, upper95: 3 } },
    { tract_fips: '02', population: 3000, baseline_concentration: 14, control_concentration: 9,
      attributable_cases: { mean: 8, lower95: 5, upper95: 11 } },
  ]

  it('renders fallback banner when perTractResults is absent', () => {
    render(
      <EJContextSection
        studyArea={studyArea}
        analysisYear={2022}
        perTractResults={null}
        availableVintages={[2020, 2021, 2022]}
      />,
    )
    expect(screen.getByText(/EJ context requires tract-resolution output/i)).toBeInTheDocument()
    expect(fetchSpy).not.toHaveBeenCalled()
  })

  it('fetches, computes, and renders aggregate stats + choropleth', async () => {
    fetchSpy.mockReturnValueOnce(okJson(mockGeojson([
      tract('01', 1000, 0.5, 0.3),
      tract('02', 3000, 0.9, 0.4),
    ])))
    render(
      <EJContextSection
        studyArea={studyArea}
        analysisYear={2022}
        perTractResults={perTractResults}
        availableVintages={[2020, 2021, 2022]}
      />,
    )
    await waitFor(() => expect(screen.getByTestId('choropleth')).toBeInTheDocument())
    // Weighted: (1000*0.5 + 3000*0.9)/4000 = 0.8
    expect(screen.getByTestId('pct-minority-value').textContent).toMatch(/80\.0%/)
    // Weighted: (1000*0.3 + 3000*0.4)/4000 = 0.375
    expect(screen.getByTestId('pct-below-200-pov-value').textContent).toMatch(/37\.5%/)
  })

  it('renders error + retry button on fetch failure', async () => {
    fetchSpy.mockReturnValueOnce(Promise.resolve({ ok: false, status: 500 }))
    render(
      <EJContextSection
        studyArea={studyArea}
        analysisYear={2022}
        perTractResults={perTractResults}
        availableVintages={[2020, 2021, 2022]}
      />,
    )
    await waitFor(() =>
      expect(screen.getByText(/couldn't load demographic data/i)).toBeInTheDocument(),
    )
    expect(screen.getByRole('button', { name: /retry/i })).toBeInTheDocument()
  })

  it('shows the chosen vintage in the provenance footer', async () => {
    fetchSpy.mockReturnValueOnce(okJson(mockGeojson([tract('01', 1000, 0.5, 0.3)])))
    render(
      <EJContextSection
        studyArea={studyArea}
        analysisYear={2025}
        perTractResults={perTractResults}
        availableVintages={[2020, 2021, 2022]}
      />,
    )
    // 2025 falls back to 2022 (post-2020 side, closest).
    await waitFor(() => expect(screen.getByText(/2022 ACS/i)).toBeInTheDocument())
  })

  it('toggles the map field when user clicks the toggle', async () => {
    fetchSpy.mockReturnValueOnce(okJson(mockGeojson([
      tract('01', 1000, 0.5, 0.3),
      tract('02', 3000, 0.9, 0.4),
    ])))
    const { findByTestId, getByRole } = render(
      <EJContextSection
        studyArea={studyArea}
        analysisYear={2022}
        perTractResults={perTractResults}
        availableVintages={[2020, 2021, 2022]}
      />,
    )
    const cp = await findByTestId('choropleth')
    expect(cp.getAttribute('data-field')).toBe('pct_minority')
    getByRole('button', { name: /below 200% poverty/i }).click()
    await waitFor(() =>
      expect(cp.getAttribute('data-field')).toBe('pct_below_200_pov'),
    )
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- EJContextSection`
Expected: FAIL — module not found.

- [ ] **Step 3: Create the component**

Create `frontend/src/components/EJContextSection.jsx`:

```jsx
import { useEffect, useMemo, useState, useCallback } from 'react'
import TractChoroplethMap from './TractChoroplethMap'
import { fetchDemographics } from '../lib/api'
import {
  populationWeightedMean,
  pickVintage,
  studyAreaToFilter,
} from '../lib/demographics'

function fmtPct(v) {
  if (v == null || Number.isNaN(v)) return '—'
  return `${(v * 100).toFixed(1)}%`
}

export default function EJContextSection({
  studyArea,
  analysisYear,
  perTractResults,
  availableVintages,
}) {
  const [geojson, setGeojson] = useState(null)
  const [error, setError] = useState(null)
  const [field, setField] = useState('pct_minority')
  const [fetchNonce, setFetchNonce] = useState(0)

  const vintage = useMemo(
    () => pickVintage(analysisYear, availableVintages ?? []),
    [analysisYear, availableVintages],
  )
  const filter = useMemo(() => studyAreaToFilter(studyArea), [studyArea])

  const hasTractResults = Array.isArray(perTractResults) && perTractResults.length > 0

  useEffect(() => {
    if (!hasTractResults) return
    if (!vintage || filter == null) return
    let cancelled = false
    setError(null)
    setGeojson(null)
    fetchDemographics('us', vintage, filter)
      .then((data) => {
        if (cancelled) return
        if (!data) {
          setError(new Error(`Demographics not available for ${vintage}`))
          return
        }
        setGeojson(data)
      })
      .catch((err) => {
        if (!cancelled) setError(err)
      })
    return () => {
      cancelled = true
    }
  }, [vintage, filter, hasTractResults, fetchNonce])

  const retry = useCallback(() => setFetchNonce((n) => n + 1), [])

  // Join demographics features to per-tract HIA results by tract FIPS,
  // then compute population-weighted aggregates on the joined set.
  const joinedTracts = useMemo(() => {
    if (!geojson) return []
    const byFips = new Map(
      (perTractResults ?? []).map((r) => [String(r.tract_fips), r]),
    )
    return geojson.features.map((f) => {
      const hia = byFips.get(String(f.properties?.geoid))
      return {
        geoid: f.properties?.geoid,
        total_pop: f.properties?.total_pop,
        pct_minority: f.properties?.pct_minority,
        pct_below_200_pov: f.properties?.pct_below_200_pov,
        hia,
      }
    })
  }, [geojson, perTractResults])

  const pctMinority = useMemo(
    () => populationWeightedMean(joinedTracts, 'pct_minority'),
    [joinedTracts],
  )
  const pctBelow200Pov = useMemo(
    () => populationWeightedMean(joinedTracts, 'pct_below_200_pov'),
    [joinedTracts],
  )

  if (!hasTractResults) {
    return (
      <section className="mt-12 border-t border-zinc-200/80 pt-10">
        <h2 className="text-[22px] font-semibold tracking-tight mb-4">Environmental Justice Context</h2>
        <div className="rounded-xl border border-amber-300 bg-amber-50 p-5 text-[14px] text-amber-900">
          EJ context requires tract-resolution output; this analysis ran at zone resolution.
        </div>
      </section>
    )
  }

  return (
    <section className="mt-12 border-t border-zinc-200/80 pt-10">
      <div className="flex items-baseline justify-between mb-6">
        <h2 className="text-[22px] font-semibold tracking-tight">Environmental Justice Context</h2>
        <span className="font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-500">
          {studyArea?.name} · {vintage ? `${vintage} ACS` : 'vintage unavailable'}
        </span>
      </div>

      {error ? (
        <div className="rounded-xl border border-rose-300 bg-rose-50 p-5 text-[14px] text-rose-900 flex items-center justify-between">
          <span>Couldn't load demographic data.</span>
          <button
            type="button"
            onClick={retry}
            className="font-mono text-[11px] uppercase tracking-[0.12em] bg-rose-700 text-white px-3 py-1.5 rounded"
          >
            Retry
          </button>
        </div>
      ) : (
        <>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
            <div className="rounded-xl border border-zinc-200 p-6">
              <p className="font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-500 mb-2">
                Minority population share
              </p>
              <p
                data-testid="pct-minority-value"
                className="font-mono text-[40px] tabular-nums font-semibold text-ink leading-none"
              >
                {fmtPct(pctMinority)}
              </p>
            </div>
            <div className="rounded-xl border border-zinc-200 p-6">
              <p className="font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-500 mb-2">
                Population below 200% poverty line
              </p>
              <p
                data-testid="pct-below-200-pov-value"
                className="font-mono text-[40px] tabular-nums font-semibold text-ink leading-none"
              >
                {fmtPct(pctBelow200Pov)}
              </p>
            </div>
          </div>

          <div className="flex items-center gap-2 mb-4">
            <button
              type="button"
              onClick={() => setField('pct_minority')}
              className={`font-mono text-[11px] uppercase tracking-[0.12em] px-3 py-1.5 rounded border ${
                field === 'pct_minority'
                  ? 'bg-ink text-paper border-ink'
                  : 'bg-paper text-zinc-600 border-zinc-300'
              }`}
            >
              % Minority
            </button>
            <button
              type="button"
              onClick={() => setField('pct_below_200_pov')}
              className={`font-mono text-[11px] uppercase tracking-[0.12em] px-3 py-1.5 rounded border ${
                field === 'pct_below_200_pov'
                  ? 'bg-ink text-paper border-ink'
                  : 'bg-paper text-zinc-600 border-zinc-300'
              }`}
            >
              Below 200% poverty
            </button>
          </div>

          {geojson ? (
            <TractChoroplethMap geojson={geojson} field={field} />
          ) : (
            <div className="h-[480px] rounded-xl border border-zinc-200 bg-zinc-50 flex items-center justify-center text-zinc-400 font-mono text-[11px] uppercase tracking-[0.14em]">
              Loading demographics…
            </div>
          )}

          <p className="mt-4 font-mono text-[10px] uppercase tracking-[0.14em] text-zinc-400">
            Source: U.S. Census ACS 5-year estimates, vintage {vintage}
          </p>
        </>
      )}
    </section>
  )
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- EJContextSection`
Expected: PASS (5 tests).

- [ ] **Step 5: Run the entire test suite to catch regressions**

Run: `npm test`
Expected: all tests PASS (existing + new).

- [ ] **Step 6: Commit**

```bash
git add frontend/src/components/EJContextSection.jsx frontend/src/components/__tests__/EJContextSection.test.jsx
git commit -m "feat(components): add EJContextSection results-page section"
```

---

## Task 9: Wire `EJContextSection` into `Results.jsx`

**Files:**
- Modify: `frontend/src/pages/Results.jsx`

The gate check:

1. `ejFraming === true`
2. `studyAreaToFilter(step1.studyArea) !== null` (covers US + admin-boundary requirement)
3. Analysis payload carries `per_tract_results` array (from parallel agent's engine; treat the optional field as the signal)

- [ ] **Step 1: Locate the render region in `Results.jsx`**

Read the full file and identify the JSX return statement block that produces the main results layout. The EJ section should render **below** the main tabbed content (below the closing of the tabs container) and **above** any footer / export controls.

- [ ] **Step 2: Import and add the gate**

At the top of `frontend/src/pages/Results.jsx`, add after the existing imports:

```javascript
import EJContextSection from '../components/EJContextSection'
import { studyAreaToFilter } from '../lib/demographics'
```

Inside the component function, after existing hooks and before the JSX return, compute the gate:

```javascript
  const ejFraming = useAnalysisStore((s) => s.ejFraming)
  const step1 = useAnalysisStore((s) => s.step1)
  const step2 = useAnalysisStore((s) => s.step2)
  const perTractResults = results?.per_tract_results ?? null
  const availableVintages = results?.demographics_vintages ?? [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
  const analysisYear = step2?.baseline?.year ?? null
  const ejGatePasses =
    ejFraming === true &&
    studyAreaToFilter(step1?.studyArea) !== null &&
    Array.isArray(perTractResults) &&
    perTractResults.length > 0
```

Rationale for `availableVintages` fallback: the vintage list is stable and matches what's on disk (`docs/ACS_NEXT_STEPS.md` confirms 2015–2024 built). If the backend later returns this list in the results payload, swap the fallback.

- [ ] **Step 3: Render the section**

In the JSX, immediately before the component's closing tag / footer, add:

```jsx
      {ejGatePasses && (
        <EJContextSection
          studyArea={step1.studyArea}
          analysisYear={analysisYear}
          perTractResults={perTractResults}
          availableVintages={availableVintages}
        />
      )}
```

- [ ] **Step 4: Verify existing tests still pass**

Run: `npm test`
Expected: all tests PASS. Results.jsx has no direct test, but any tests importing Results must still render.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/pages/Results.jsx
git commit -m "feat(results): render EJContextSection under ejFraming gate"
```

---

## Task 10: Manual verification

**Files:** none modified.

- [ ] **Step 1: Start the backend**

From `hia-tool/`:
```bash
cd backend && uvicorn main:app --reload --port 8000
```
Expected: server starts; `/api/data/demographics/us/2022?state=48` returns a FeatureCollection with tract features.

- [ ] **Step 2: Start the frontend**

In a second terminal, from `hia-tool/frontend`:
```bash
npm run dev
```
Expected: Vite dev server on http://localhost:5173. Set `VITE_MAPBOX_TOKEN` in `.env.local` if not already present.

- [ ] **Step 3: Load the EJ template, run end-to-end**

Navigate to the Home page. In the "Start from a template" section at the bottom, click the **EJ** template card. Complete the wizard (all pre-filled) and run the analysis.

- [ ] **Step 4: Verify the EJ section on Results**

On the Results page:
- Confirm the **Environmental Justice Context** section appears below the main results.
- Two headline numbers are populated and plausible.
- The choropleth renders as a tract-level map with the study-area geometry visible.
- Clicking the **Below 200% poverty** toggle updates the choropleth colors.
- The provenance footer names the correct vintage.

If `per_tract_results` is not yet emitted by the engine (parallel agent's work), the section will render the "zone-resolution fallback" banner instead. Note this as a known dependency but do NOT implement per-tract results in this plan.

- [ ] **Step 5: Verify standard templates are unchanged**

Navigate back to Home, click a non-EJ template (e.g. "U.S. National PM₂.₅"), run it, and confirm the Results page has **no** Environmental Justice Context section.

- [ ] **Step 6: Verify silent opt-out**

Load the EJ template, but before running, edit Step 1 to change country away from US. Run. Confirm the EJ section does NOT appear on Results (silent gate opt-out per design D3).

- [ ] **Step 7: Final commit (if any changes arose from manual verification)**

If verification surfaced small fixes, commit them with descriptive messages. Otherwise, no commit.

- [ ] **Step 8: Summary commit reference**

Record the final state:

```bash
git log --oneline feature/ej-template-results ^master || git log --oneline -12
```
Expected: ~9 small commits, one per task 1–9.

---

## Out of scope (DO NOT implement in this plan)

These are captured in `Obsidian > System Map > HIA Tool - EJ Template Phase 2 Backlog`:

- Custom polygon / uploaded boundary study areas under EJ.
- Step 1 map demographics overlay.
- Comparison baselines (state/national).
- Additional ACS fields.
- Stratified HIA outcomes.
- Exposure-weighted demographics.
- Backend aggregate-stats helpers in `hia_engine.py`.
- Non-US countries.
- Vector tiles, FastAPI lifespan migration, FIPS regex validation, parquet indexes.

## Dependency on parallel engine work

The gate in Task 9 requires `results.per_tract_results` to be populated by the backend's tract-resolution engine. If that work has not landed when this plan is executed, Task 10 step 4 will show the fallback banner. That is **expected behavior** — the feature integrates cleanly once the engine work merges without any follow-up code change on the EJ side, provided the payload matches the placeholder shape in the spec (`{ tract_fips, population, baseline_concentration, control_concentration, attributable_cases: { mean, lower95, upper95 } }[]`). If the parallel agent's final shape differs, the only adapter is the `byFips` join inside `EJContextSection.jsx` (Task 8, look for `r.tract_fips`), which can be updated with a single change.
