# Plan 1: Step 2 dataset-year reorder + real availability

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the "not available" bug on Step 2 by grounding the dataset dropdown in real backend availability, reordering so dataset is picked before year, and constraining the year picker to what the chosen dataset actually covers for the chosen country. Remove the now-redundant Step 1 year picker.

**Architecture:** Backend `_scan_datasets` emits a `countries_covered` list per dataset so the frontend can filter options by study country. Step 2's `BuiltinConcentrationLoader` drops its hardcoded `BUILTIN_DATASETS` fallback, only shows real options, and renders a year `<select>` after a dataset is chosen, populated from that dataset's years. Step 1's `YearPicker` component and `step1.years` field are removed; Zustand store version bumps.

**Tech stack:** Python / FastAPI / pandas on backend, React / Zustand / Vite / Vitest on frontend.

**Repo root:** `C:/Users/vsoutherland/Claude/hia-tool`. All paths below are relative to repo root unless otherwise noted.

**Dev server commands (for manual verification):**
- Backend: `venv/Scripts/python.exe -m backend.main` (serves on `127.0.0.1:8000`)
- Frontend: `cd frontend && npm run dev` (serves on `5173`, proxies `/api` → `8000`)
- Frontend tests: `cd frontend && npm test`
- Backend tests: `venv/Scripts/python.exe -m pytest backend/tests/ -v`

---

## Task 1: Backend — extend dataset scanner with per-country coverage

**Files:**
- Modify: `backend/routers/data.py:575-779` (`_scan_datasets`)
- Test: `backend/tests/test_scan_datasets.py` (create)

- [ ] **Step 1: Write the failing test**

Create `backend/tests/test_scan_datasets.py`:

```python
"""Tests for dataset-listing behavior in ``backend.routers.data``.

These tests stub DATA_ROOT to a tmp path so they're hermetic and cover
the contract the frontend relies on: ``countries_covered`` must be set
for WHO-AAP-style global datasets so the UI can filter dataset options
by study-area country.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from backend.routers import data as data_module


@pytest.fixture
def tmp_data_root(tmp_path, monkeypatch):
    monkeypatch.setattr(data_module, "DATA_ROOT", tmp_path)
    data_module._read_parquet.cache_clear()
    data_module._read_csv.cache_clear()
    return tmp_path


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow")


def test_who_aap_entry_includes_countries_covered(tmp_data_root: Path):
    # Two years, each with a different overlap of countries.
    _write_parquet(
        tmp_data_root / "who_aap" / "ne_countries" / "2017.parquet",
        pd.DataFrame({
            "admin_id": ["MEX", "USA", "CAN"],
            "mean_pm25": [15.0, 8.0, 7.0],
            "geometry": [None, None, None],
        }),
    )
    _write_parquet(
        tmp_data_root / "who_aap" / "ne_countries" / "2018.parquet",
        pd.DataFrame({
            "admin_id": ["USA", "FRA"],
            "mean_pm25": [8.0, 10.0],
            "geometry": [None, None],
        }),
    )

    datasets = data_module._scan_datasets()
    who = [d for d in datasets if d.get("id") == "who_aap_pm25_global"]
    assert len(who) == 1
    assert sorted(who[0]["countries_covered"]) == ["CAN", "FRA", "MEX", "USA"]


def test_epa_aqs_entry_includes_us_states_covered(tmp_data_root: Path):
    _write_parquet(
        tmp_data_root / "epa_aqs" / "pm25" / "ne_states" / "2020.parquet",
        pd.DataFrame({
            "admin_id": ["US-CA", "US-NY"],
            "mean_pm25": [10.0, 9.0],
            "geometry": [None, None],
        }),
    )

    datasets = data_module._scan_datasets()
    epa = [d for d in datasets if d.get("id") == "epa_aqs_pm25"]
    assert len(epa) == 1
    assert sorted(epa[0]["countries_covered"]) == ["US-CA", "US-NY"]


def test_direct_country_dataset_sets_countries_covered_from_path(
    tmp_data_root: Path,
):
    # Direct pollutant/country/year files already carry country in the
    # path; surface it in countries_covered too for UI uniformity.
    _write_parquet(
        tmp_data_root / "pm25" / "mexico" / "2019.parquet",
        pd.DataFrame({"admin_id": ["MX-01"], "mean_pm25": [16.0], "geometry": [None]}),
    )

    datasets = data_module._scan_datasets()
    direct = [
        d for d in datasets
        if d.get("type") == "concentration"
        and d.get("pollutant") == "pm25"
        and d.get("country") == "mexico"
    ]
    assert len(direct) == 1
    assert direct[0]["countries_covered"] == ["mexico"]
```

- [ ] **Step 2: Run test to verify it fails**

```bash
venv/Scripts/python.exe -m pytest backend/tests/test_scan_datasets.py -v
```

Expected: all three tests FAIL (either `KeyError: 'countries_covered'` or `AssertionError`).

- [ ] **Step 3: Implement `countries_covered`**

Edit `backend/routers/data.py`. In `_scan_datasets`:

1. For the direct-pollutant loop (around line 589-612), add `countries_covered` to the entry:

```python
            if years:
                datasets.append({
                    "type": "concentration",
                    "pollutant": key,
                    "pollutant_label": pollutant_names.get(key, key),
                    "country": country_dir.name,
                    "countries_covered": [country_dir.name],
                    "years": years,
                    "source": f"Processed {pollutant_names.get(key, key)} raster",
                })
```

2. For the EPA AQS loop (around line 614-642), compute distinct `admin_id` values across years and add `countries_covered`:

```python
    aqs_dir = DATA_ROOT / "epa_aqs"
    if aqs_dir.exists():
        for pollutant_dir in sorted(aqs_dir.iterdir()):
            if not pollutant_dir.is_dir():
                continue
            pkey = pollutant_dir.name
            state_sub = pollutant_dir / "ne_states"
            if not state_sub.exists():
                continue
            year_files = [
                f for f in state_sub.iterdir()
                if f.suffix in (".parquet", ".csv") and f.stem.isdigit()
            ]
            years = sorted(int(f.stem) for f in year_files)
            if years:
                covered: set[str] = set()
                for f in year_files:
                    try:
                        df = _read_table(f)
                        if "admin_id" in df.columns:
                            covered.update(
                                str(x) for x in df["admin_id"].dropna().unique()
                            )
                    except Exception:
                        logger.warning("Failed to read %s for coverage", f, exc_info=True)
                datasets.append({
                    "id": f"epa_aqs_{pkey}",
                    "type": "concentration",
                    "pollutant": pkey,
                    "pollutant_label": pollutant_names.get(pkey, pkey),
                    "country": "us",
                    "countries_covered": sorted(covered),
                    "years": years,
                    "aggregation": "state",
                    "source": "EPA AQS — state-level monitor means",
                    "label": (
                        f"EPA AQS — {pollutant_names.get(pkey, pkey)} "
                        "(US state-level)"
                    ),
                })
```

3. For the WHO AAP loop (around line 647-664), compute distinct `admin_id` values across years:

```python
    who_countries = DATA_ROOT / "who_aap" / "ne_countries"
    if who_countries.exists():
        year_files = [
            f for f in who_countries.iterdir()
            if f.suffix in (".parquet", ".csv") and f.stem.isdigit()
        ]
        years = sorted(int(f.stem) for f in year_files)
        if years:
            covered: set[str] = set()
            for f in year_files:
                try:
                    df = _read_table(f)
                    if "admin_id" in df.columns:
                        covered.update(
                            str(x) for x in df["admin_id"].dropna().unique()
                        )
                except Exception:
                    logger.warning("Failed to read %s for coverage", f, exc_info=True)
            datasets.append({
                "id": "who_aap_pm25_global",
                "type": "concentration",
                "pollutant": "pm25",
                "pollutant_label": "PM2.5",
                "country": "global",
                "countries_covered": sorted(covered),
                "years": years,
                "aggregation": "country",
                "source": "WHO Ambient Air Pollution Database",
                "label": "WHO AAP — PM2.5 (global, country-level)",
            })
```

- [ ] **Step 4: Run test to verify it passes**

```bash
venv/Scripts/python.exe -m pytest backend/tests/test_scan_datasets.py -v
```

Expected: all three tests PASS.

- [ ] **Step 5: Run the full backend test suite to check for regressions**

```bash
venv/Scripts/python.exe -m pytest backend/tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add backend/routers/data.py backend/tests/test_scan_datasets.py
git commit -m "feat(data): include countries_covered in dataset metadata

Adds a countries_covered list to each concentration dataset entry so
the frontend can filter dataset options by study-area country without
probing individual year files. Direct-country datasets get a single-
entry list; WHO AAP and EPA AQS datasets read distinct admin_id values
across their year files."
```

---

## Task 2: Frontend — map year coverage helper

**Files:**
- Create: `frontend/src/lib/datasets.js`
- Test: `frontend/src/lib/__tests__/datasets.test.js` (create)

Utility that returns which years a given dataset covers for a given country, given the `countries_covered` and `years` from the backend. This decouples the Step 2 UI from the raw dataset shape and lets us unit-test the filtering logic.

- [ ] **Step 1: Write the failing test**

Create `frontend/src/lib/__tests__/datasets.test.js`:

```js
import { describe, it, expect } from 'vitest'
import { datasetCoversCountry, yearsFor } from '../datasets'

describe('datasetCoversCountry', () => {
  it('matches direct country slug', () => {
    const ds = { countries_covered: ['mexico'] }
    expect(datasetCoversCountry(ds, 'MEX')).toBe(true)
    expect(datasetCoversCountry(ds, 'USA')).toBe(false)
  })

  it('matches ISO-3 in coverage list', () => {
    const ds = { countries_covered: ['MEX', 'USA', 'CAN'] }
    expect(datasetCoversCountry(ds, 'MEX')).toBe(true)
    expect(datasetCoversCountry(ds, 'FRA')).toBe(false)
  })

  it('treats US-XX state codes as US coverage', () => {
    const ds = { countries_covered: ['US-CA', 'US-NY'] }
    expect(datasetCoversCountry(ds, 'USA')).toBe(true)
    expect(datasetCoversCountry(ds, 'MEX')).toBe(false)
  })

  it('returns false when coverage list is missing', () => {
    expect(datasetCoversCountry({}, 'MEX')).toBe(false)
    expect(datasetCoversCountry(null, 'MEX')).toBe(false)
  })
})

describe('yearsFor', () => {
  it('returns the dataset years when country is covered', () => {
    const ds = { countries_covered: ['MEX'], years: [2015, 2016, 2017] }
    expect(yearsFor(ds, 'MEX')).toEqual([2015, 2016, 2017])
  })

  it('returns an empty array when country is not covered', () => {
    const ds = { countries_covered: ['USA'], years: [2018, 2019] }
    expect(yearsFor(ds, 'MEX')).toEqual([])
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd frontend && npm test -- src/lib/__tests__/datasets.test.js
```

Expected: FAIL with "Cannot find module '../datasets'".

- [ ] **Step 3: Implement the helper**

Create `frontend/src/lib/datasets.js`:

```js
const ISO3_TO_SLUG = {
  USA: ['us', 'usa'],
  MEX: ['mexico', 'mex'],
}

export function datasetCoversCountry(dataset, countryIso3) {
  if (!dataset?.countries_covered?.length || !countryIso3) return false
  const iso = countryIso3.toUpperCase()
  const aliases = ISO3_TO_SLUG[iso] || []
  for (const entry of dataset.countries_covered) {
    const upper = String(entry).toUpperCase()
    if (upper === iso) return true
    if (iso === 'USA' && upper.startsWith('US-')) return true
    if (aliases.includes(String(entry).toLowerCase())) return true
  }
  return false
}

export function yearsFor(dataset, countryIso3) {
  if (!datasetCoversCountry(dataset, countryIso3)) return []
  return [...(dataset.years || [])]
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd frontend && npm test -- src/lib/__tests__/datasets.test.js
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/lib/datasets.js frontend/src/lib/__tests__/datasets.test.js
git commit -m "feat(frontend): dataset coverage helpers for country/year filtering"
```

---

## Task 3: Frontend — remove Step 1 year picker; prep store for year-in-step-2

**Files:**
- Modify: `frontend/src/stores/useAnalysisStore.js`
- Modify: `frontend/src/pages/steps/Step1StudyArea.jsx`

- [ ] **Step 1: Update store — remove step1.years, add step2.baseline.year and step2.control.year, bump version**

Edit `frontend/src/stores/useAnalysisStore.js`:

Replace the `DEFAULT_STEP1` and `DEFAULT_STEP2` blocks (lines 6-23) with:

```js
const DEFAULT_STEP1 = {
  studyArea: { type: 'country', id: '', name: '', geometry: null, boundaryUploadId: null },
  pollutant: null,
  analysisName: '',
  analysisDescription: '',
}

const DEFAULT_STEP2 = {
  baseline: { type: 'manual', value: null, datasetId: null, fileData: null, uploadId: null, year: null },
  control: {
    type: 'none',
    value: null,
    benchmarkId: null,
    rollbackPercent: null,
    uploadId: null,
    year: null,
  },
}
```

Bump version from 4 → 5 and add migration (lines 178-200):

```js
      name: 'hia-analysis',
      version: 5,
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
      }),
      migrate: (persisted, version) => {
        // v5: removed step1.years; added step2.baseline.year and
        // step2.control.year. Migrations drop the stored state so the
        // user starts with defaults rather than running with a partial
        // old shape.
        if (version < 5) return initialState()
        return persisted
      },
    },
```

- [ ] **Step 2: Remove the YearPicker component and year fieldset from Step 1**

Edit `frontend/src/pages/steps/Step1StudyArea.jsx`:

1. Delete the `YearPicker` function definition (lines 208-300).
2. Delete the `YEAR_MIN` and `YEAR_MAX` constants at the top (lines 38-39).
3. In `Step1StudyArea`, delete `years` from the destructure at line 306:

```js
  const { studyArea, pollutant, analysisName, analysisDescription } = step1
```

4. Remove the "Analysis Period" `<fieldset>` block (lines 569-576) entirely.

- [ ] **Step 3: Run frontend tests to check nothing broke**

```bash
cd frontend && npm test
```

Expected: existing tests PASS (App.test.jsx may need updating if it referenced `step1.years` — skip to Step 4 if so).

- [ ] **Step 4: Fix any test regressions**

If `frontend/src/__tests__/App.test.jsx` references `step1.years` or the YearPicker, update it to match the new shape. Run tests again until they pass.

- [ ] **Step 5: Manual smoke test**

Start dev server (`cd frontend && npm run dev`), navigate to `/analysis/1`. Verify:
- No "Analysis Period" / year picker visible on Step 1.
- Country, pollutant, custom boundary, name, description still present and functional.
- Step 1 is marked valid with country + pollutant selected (check the "Next" button becomes enabled).

- [ ] **Step 6: Commit**

```bash
git add frontend/src/stores/useAnalysisStore.js frontend/src/pages/steps/Step1StudyArea.jsx frontend/src/__tests__/App.test.jsx
git commit -m "refactor(step1): remove year picker; year moves to Step 2 per dataset

The wizard now picks year in Step 2 alongside the concentration dataset
so the picker can be constrained to the dataset's coverage. Store bumps
to v5 and migration resets persisted state to the new shape."
```

---

## Task 4: Frontend — Step 2 `BuiltinConcentrationLoader` uses real availability + year picker

**Files:**
- Modify: `frontend/src/pages/steps/Step2AirQuality.jsx`

- [ ] **Step 1: Delete the hardcoded `BUILTIN_DATASETS` constant**

In `Step2AirQuality.jsx`, delete lines 24-31 (the `BUILTIN_DATASETS` array). Nothing else references it.

- [ ] **Step 2: Rewrite `BuiltinConcentrationLoader` to filter datasets by country and render the year picker**

Replace the entire `BuiltinConcentrationLoader` function (lines 227-321) with:

```jsx
function BuiltinConcentrationLoader({
  pollutant,
  studyArea,
  selectedDatasetId,
  selectedYear,
  onSelect,
  onYearChange,
  onDataLoaded,
}) {
  const [datasets, setDatasets] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [geojsonPreview, setGeojsonPreview] = useState(null)

  const country = studyArea?.id || ''

  // Fetch available datasets on mount and whenever pollutant changes
  useEffect(() => {
    if (!pollutant) return
    setDatasets(null)
    fetchDatasets({ pollutant, type: 'concentration' })
      .then((res) => setDatasets(res.datasets || []))
      .catch(() => setDatasets([]))
  }, [pollutant])

  const filteredOptions = useMemo(() => {
    if (!datasets) return null
    return datasets.filter((d) => datasetCoversCountry(d, country))
  }, [datasets, country])

  const selectedDataset = useMemo(
    () =>
      (filteredOptions || []).find(
        (d) => (d.id || `${d.pollutant}-${d.country}`) === selectedDatasetId,
      ) || null,
    [filteredOptions, selectedDatasetId],
  )

  const availableYears = useMemo(
    () => (selectedDataset ? yearsFor(selectedDataset, country) : []),
    [selectedDataset, country],
  )

  // Fetch concentration data when a dataset + year are selected
  useEffect(() => {
    if (!selectedDatasetId || !pollutant || !country || !selectedYear) return

    setLoading(true)
    setError(null)
    setGeojsonPreview(null)

    fetchConcentration(pollutant, country, selectedYear)
      .then((geojson) => {
        if (!geojson) {
          setError(`Built-in data not available for ${studyArea?.name || country} in ${selectedYear}.`)
          return
        }
        setGeojsonPreview(geojson)
        const features = geojson.features || []
        if (features.length > 0) {
          const concentrations = features
            .map((f) => f.properties?.mean_pm25 ?? f.properties?.mean ?? f.properties?.concentration)
            .filter((v) => v != null)
          if (concentrations.length > 0) {
            const mean = concentrations.reduce((a, b) => a + b, 0) / concentrations.length
            onDataLoaded(Math.round(mean * 100) / 100, geojson)
          }
        }
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false))
  }, [selectedDatasetId, pollutant, country, selectedYear]) // eslint-disable-line react-hooks/exhaustive-deps

  // Loading state for the dataset list itself
  if (datasets === null) {
    return (
      <div className="flex items-center gap-2 p-3 bg-blue-50 border border-blue-200 rounded-lg text-sm text-blue-700">
        <svg className="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
        </svg>
        Loading available datasets…
      </div>
    )
  }

  // Empty state when no dataset covers the chosen country
  if (filteredOptions.length === 0) {
    return (
      <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg text-sm text-amber-700">
        <p className="font-medium">No built-in concentration data for {studyArea?.name || country}.</p>
        <p className="mt-1 text-xs">Switch to Manual Entry or File Upload, or choose a different pollutant.</p>
      </div>
    )
  }

  return (
    <div className="space-y-3">
      {/* Dataset picker */}
      <div>
        <label className="block text-xs text-gray-500 mb-1">Dataset</label>
        <select
          value={selectedDatasetId || ''}
          onChange={(e) => {
            onSelect(e.target.value)
            onYearChange(null)
          }}
          className="w-full rounded-lg border border-gray-300 px-3 py-2.5 text-sm
                     focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
        >
          <option value="">Select a dataset…</option>
          {filteredOptions.map((d) => {
            const id = d.id || `${d.pollutant}-${d.country}`
            const years = yearsFor(d, country)
            const yearRange = years.length > 0
              ? (years[0] === years[years.length - 1]
                  ? `${years[0]}`
                  : `${years[0]}–${years[years.length - 1]}`)
              : '—'
            const label = d.label || `${d.pollutant_label || d.pollutant} — ${d.country}`
            return (
              <option key={id} value={id}>
                {label} ({yearRange})
              </option>
            )
          })}
        </select>
      </div>

      {/* Year picker — only after dataset chosen */}
      {selectedDataset && (
        <div>
          <label className="block text-xs text-gray-500 mb-1">Year</label>
          <select
            value={selectedYear ?? ''}
            onChange={(e) => onYearChange(e.target.value ? Number(e.target.value) : null)}
            className="w-full rounded-lg border border-gray-300 px-3 py-2.5 text-sm
                       focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
          >
            <option value="">Select a year…</option>
            {availableYears.map((y) => (
              <option key={y} value={y}>{y}</option>
            ))}
          </select>
        </div>
      )}

      {loading && (
        <div className="flex items-center gap-2 p-3 bg-blue-50 border border-blue-200 rounded-lg text-sm text-blue-700">
          <svg className="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
          Loading concentration data…
        </div>
      )}

      {error && (
        <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg text-sm text-amber-700">
          {error}
        </div>
      )}

      {geojsonPreview && !error && (
        <div className="p-3 bg-green-50 border border-green-200 rounded-lg text-sm text-green-700">
          <p className="font-medium">Data loaded successfully</p>
          <p className="text-xs text-green-600 mt-1">
            {geojsonPreview.features?.length || 0} admin units with concentration values
          </p>
        </div>
      )}
    </div>
  )
}
```

Also add the new imports at the top of the file (around line 3):

```js
import { uploadFile, fetchConcentration, fetchDatasets } from '../../lib/api'
import { datasetCoversCountry, yearsFor } from '../../lib/datasets'
```

- [ ] **Step 3: Wire `selectedYear` + `onYearChange` from the parent into both baseline and control `BuiltinConcentrationLoader` call sites**

In `Step2AirQuality`, update the two invocations (around line 587-601 for baseline, 664-678 for control) to pass year state:

```jsx
{/* Baseline — Built-in Data */}
{baselineTab === 'builtin' && (
  <BuiltinConcentrationLoader
    pollutant={pollutant}
    studyArea={step1.studyArea}
    selectedDatasetId={baseline.datasetId}
    selectedYear={baseline.year}
    onSelect={handleBaselineDataset}
    onYearChange={(year) =>
      setStep2({ baseline: { ...baseline, year, type: 'dataset' } })
    }
    onDataLoaded={(value, geojson) => {
      setStep2({
        baseline: { ...baseline, value, datasetId: baseline.datasetId, type: 'dataset', builtinGeojson: geojson },
      })
    }}
  />
)}
```

```jsx
{/* Control — Built-in Data */}
{controlTab === 'builtin' && (
  <BuiltinConcentrationLoader
    pollutant={pollutant}
    studyArea={step1.studyArea}
    selectedDatasetId={control.datasetId}
    selectedYear={control.year}
    onSelect={handleControlDataset}
    onYearChange={(year) =>
      setStep2({ control: { ...control, year, type: 'dataset' } })
    }
    onDataLoaded={(value, geojson) => {
      setStep2({
        control: { ...control, value, datasetId: control.datasetId, type: 'dataset', builtinGeojson: geojson },
      })
    }}
  />
)}
```

- [ ] **Step 4: Update Step 2 validation to require baseline.year when using dataset source**

In `Step2AirQuality`, replace the validation useEffect (around line 345-352) with:

```jsx
  useEffect(() => {
    const hasBaseline =
      (baseline.type === 'manual' && baseline.value != null && baseline.value !== '' && baseline.value >= 0) ||
      (baseline.type === 'dataset' && baseline.datasetId != null && baseline.year != null) ||
      (baseline.type === 'file' && baseline.fileData?.name && !baseline.fileData?.error)
    setStepValidity(2, hasBaseline)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [baseline])
```

- [ ] **Step 5: Manual smoke test — Mexico + PM2.5 scenario**

Start backend and frontend. Navigate through:

1. Step 1: pick Mexico, PM2.5, advance.
2. Step 2 → Baseline → Built-in Data.
3. Expected: "Loading available datasets…" briefly, then the dropdown shows **only** `WHO AAP — PM2.5 (global, country-level) (2015–2018)` (Mexico isn't in 2019/2020/2021 rows, so the dataset's effective coverage is 2015–2018).

   Wait — `countries_covered` is the union across years, so WHO AAP shows `(2015–2021)` in the dropdown. The year-level filtering happens when the user picks a year and it 404s. For this plan that's acceptable; Plan 2 can refine if needed.

4. Pick the WHO AAP dataset. Year picker appears with 2015-2021.
5. Pick 2018. Expected: "Data loaded successfully" with N admin units.
6. Pick 2020. Expected: "Built-in data not available for Mexico in 2020." (MEX row is missing from that year.)

- [ ] **Step 6: Commit**

```bash
git add frontend/src/pages/steps/Step2AirQuality.jsx
git commit -m "feat(step2): real dataset availability + year-after-dataset flow

The Built-in Data loader now shows only datasets that actually cover
the selected country (per backend countries_covered) and renders a year
picker constrained to the chosen dataset's years. Hardcoded
BUILTIN_DATASETS placeholders are removed. Step 2 is invalid until a
year is picked for dataset-sourced baselines."
```

---

## Task 5: Update `Results.jsx` to stop referencing `step1.years` in PDF export

**Files:**
- Modify: `frontend/src/pages/Results.jsx:492-496`

The PDF export references `step1.years` which no longer exists. Point it at the baseline year from Step 2 instead.

- [ ] **Step 1: Replace the Years row in the PDF parameters**

In `Results.jsx`, replace the line (around line 495):

```js
        ['Years', step1?.years ? (step1.years.start === step1.years.end ? String(step1.years.start) : `${step1.years.start}–${step1.years.end}`) : '—'],
```

with:

```js
        ['Year', results?.meta?.year ?? '—'],
```

Then in `Results`, pull `step2` into the destructure so we can pass the baseline year via `results.meta`. Simpler: the compute response already includes metadata — if it does not carry `year`, source it from `step1.analysisName` is not right. Use the zustand store instead: update the destructure at line 584:

```js
  const { results, step1, step2, step6, step7, exportConfig } = useAnalysisStore()
```

And change the `['Year', ...]` row to:

```js
        ['Year', step2?.baseline?.year ?? '—'],
```

Then pass `step2` into `ExportTab` (around line 747-753):

```jsx
                    step1={step1}
                    step2={step2}
                    step6={step6}
                    step7={step7}
```

And update `ExportTab`'s signature (line 448) to accept `step2` and use it:

```js
function ExportTab({ results, analysisName, hasValuation, summaryRef, tableRef, step1, step2, step6, step7, exportConfig, onOpenTemplateModal }) {
```

Replace `['Years', ...]` with `['Year', step2?.baseline?.year ?? '—']`.

- [ ] **Step 2: Run frontend tests**

```bash
cd frontend && npm test
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add frontend/src/pages/Results.jsx
git commit -m "fix(results): read analysis year from step2 since step1.years is gone"
```

---

## Task 6: Full regression sweep

- [ ] **Step 1: Run backend tests**

```bash
venv/Scripts/python.exe -m pytest backend/tests/ -v
```

Expected: all PASS.

- [ ] **Step 2: Run frontend tests**

```bash
cd frontend && npm test
```

Expected: all PASS.

- [ ] **Step 3: Manual end-to-end smoke**

With backend + frontend running, walk the wizard Mexico → PM2.5 → Step 2 Baseline → Built-in → WHO AAP → 2018 → Step 3 → Step 4 → Step 5 → Step 6 Run. Confirm a result is produced and the PDF export's parameter block shows "Year: 2018" (or whatever year was picked).

Steps 3/4 still default their year to the current-year stub because Plan 2 hasn't run yet — that's expected for now; the analysis can still complete because their fallback paths either bypass year or hit other backend endpoints.

- [ ] **Step 4: If issues surface, fix inline. Otherwise done.**

---

## Spec alignment check

| Spec item | Task(s) |
|-----------|---------|
| D1: Remove year from Step 1 | Task 3 |
| D2: Dataset before year in Step 2 | Task 4 |
| D2: Built-in dropdown shows year coverage | Task 4 Step 2 |
| D3: Real availability, no placeholders | Task 1 (backend), Task 4 Step 1-2 |
| D3: `countries_covered` on dataset entries | Task 1 |
| D3: Actionable empty state | Task 4 Step 2 |
| Data model: `step1.years` removed | Task 3 Step 1-2 |
| Data model: `step2.baseline.year`, `step2.control.year` added | Task 3 Step 1 |
| Data model: store version bumped | Task 3 Step 1 |

Out-of-scope for Plan 1 (handled by Plan 2/3): D4 year cascade, D5 upload year fields, D6 multi-year post-results, `results` becoming a list.
