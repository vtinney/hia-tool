# PM2.5 Population-Weighted Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Google Earth Engine script that computes annual population-weighted PM2.5 (2015–latest) for 9 pre-uploaded boundary sets, and a local Python script that converts the exported CSVs into Parquet for the HIA tool.

**Architecture:** Two-stage pipeline. Stage 1 is an Earth Engine JavaScript script that reads Van Donkelaar V5GL04 PM2.5 and WorldPop `pop_age_sex`, resamples WorldPop onto the PM2.5 grid with a sum reducer, computes `pm25 × pop` per pixel, and uses `reduceRegions` to aggregate per feature per year into CSVs on Google Drive. Stage 2 is a Python script (pandas + pyarrow) that reads those CSVs, computes `pm25_popweighted = sum(pm25*pop) / sum(pop)`, and writes one long-format Parquet file per boundary set into `data/processed/`.

**Tech Stack:** Google Earth Engine JavaScript API, Python 3 (pandas, pyarrow), pytest.

**Spec:** `hia-tool/docs/superpowers/specs/2026-04-09-pm25-popweighted-design.md`
**Methods doc:** `hia-tool/docs/methods/pm25_popweighted_methods.md`

---

## File Structure

- **Create:** `hia-tool/scripts/pm25_popweighted.js` — Earth Engine script, copy-pasted into the GEE Code Editor to run
- **Create:** `hia-tool/scripts/pm25_csv_to_parquet.py` — CLI that converts exported CSVs to Parquet
- **Create:** `hia-tool/scripts/tests/test_pm25_csv_to_parquet.py` — pytest tests for the Python script
- **Create:** `hia-tool/scripts/tests/fixtures/sample_pm25.csv` — tiny fixture CSV for tests
- **Create:** `hia-tool/data/processed/` (directory only, if not already present) — Parquet output destination
- **Modify (optional):** `hia-tool/requirements.txt` — add `pyarrow` if not already present

The GEE script is the bulk of the work but is not testable with pytest. It is validated manually by running in the GEE Code Editor, inspecting `print()` output, and confirming exports finish. The Python script is unit-tested.

---

## Pre-flight: Verify boundary asset field names

The script needs the correct `idField` and `nameField` for each boundary asset. These vary (e.g. GADM uses `GID_0`/`NAME_0`, NE uses `ADM0_A3`/`NAME`, etc.). Before writing the main script, inspect each asset once in the GEE editor and record the field names.

### Task 0: Inspect boundary assets in GEE editor

**Files:**
- Scratch file (not committed): any GEE code editor tab

- [ ] **Step 1: Open the GEE Code Editor and paste this probe**

```javascript
var assets = [
  'projects/hia-tool/assets/GADM_1',
  'projects/hia-tool/assets/GADM_2',
  'projects/hia-tool/assets/GADM_3',
  'projects/hia-tool/assets/GADM_4',
  'projects/hia-tool/assets/GADM_5',
  'projects/hia-tool/assets/GADM_6',
  'projects/hia-tool/assets/ne_countries',
  'projects/hia-tool/assets/ne_states',
  'projects/hia-tool/assets/GHS_SMOD',
];
assets.forEach(function(id) {
  var fc = ee.FeatureCollection(id);
  print(id, 'first feature:', fc.first());
  print(id, 'size:', fc.size());
});
```

- [ ] **Step 2: Run and record the property names**

In the console output, expand `first feature` → `properties` for each asset. Write down the field most suitable as a stable ID and the field most suitable as a human-readable name. Expected results (verify these — adjust if they differ):

| Asset | Likely idField | Likely nameField |
|---|---|---|
| GADM_1 | `GID_0` | `COUNTRY` or `NAME_0` |
| GADM_2 | `GID_1` | `NAME_1` |
| GADM_3 | `GID_2` | `NAME_2` |
| GADM_4 | `GID_3` | `NAME_3` |
| GADM_5 | `GID_4` | `NAME_4` |
| GADM_6 | `GID_5` | `NAME_5` |
| ne_countries | `ADM0_A3` | `NAME` |
| ne_states | `adm1_code` | `name` |
| GHS_SMOD | `ID_HDC_G0` | `UC_NM_MN` |

- [ ] **Step 3: Record the confirmed values**

Write the confirmed idField/nameField pairs into a comment block you'll paste into the script CONFIG in Task 1. Do not commit anything yet.

---

## Stage 1 — Earth Engine script

### Task 1: Scaffold the GEE script with CONFIG

**Files:**
- Create: `hia-tool/scripts/pm25_popweighted.js`

- [ ] **Step 1: Create the file with CONFIG and stubs**

```javascript
// pm25_popweighted.js
// Compute annual population-weighted PM2.5 per feature for 9 boundary sets.
// Inputs: Van Donkelaar V5GL04 PM2.5, WorldPop pop_age_sex.
// Output: one CSV per boundary set exported to Google Drive.
//
// Run in the Google Earth Engine Code Editor. Review the Tasks panel to
// start the exports once the script finishes compiling.

var CONFIG = {
  pm25Collection: 'projects/sat-io/open-datasets/GLOBAL-SATELLITE-PM2-5/ANNUAL-MEAN/V5GL04',
  wpCollection: 'WorldPop/GP/100m/pop_age_sex',
  startYear: 2015,
  endYear: 2022,              // clipped to PM2.5 availability at runtime
  wpLastYear: 2020,           // fallback year for WorldPop pop_age_sex
  driveFolder: 'hia_tool_pm25',
  scale: 11132,               // ~0.1 deg; set per-boundary below if needed
  tileScale: 8,
  // Replace idField/nameField with the values confirmed in Task 0.
  boundaries: [
    {assetId: 'projects/hia-tool/assets/GADM_1', name: 'gadm_1', idField: 'GID_0',  nameField: 'COUNTRY'},
    {assetId: 'projects/hia-tool/assets/GADM_2', name: 'gadm_2', idField: 'GID_1',  nameField: 'NAME_1'},
    {assetId: 'projects/hia-tool/assets/GADM_3', name: 'gadm_3', idField: 'GID_2',  nameField: 'NAME_2'},
    {assetId: 'projects/hia-tool/assets/GADM_4', name: 'gadm_4', idField: 'GID_3',  nameField: 'NAME_3'},
    {assetId: 'projects/hia-tool/assets/GADM_5', name: 'gadm_5', idField: 'GID_4',  nameField: 'NAME_4'},
    {assetId: 'projects/hia-tool/assets/GADM_6', name: 'gadm_6', idField: 'GID_5',  nameField: 'NAME_5'},
    {assetId: 'projects/hia-tool/assets/ne_countries', name: 'ne_countries', idField: 'ADM0_A3',  nameField: 'NAME'},
    {assetId: 'projects/hia-tool/assets/ne_states',    name: 'ne_states',    idField: 'adm1_code', nameField: 'name'},
    {assetId: 'projects/hia-tool/assets/GHS_SMOD',     name: 'ghs_smod',     idField: 'ID_HDC_G0', nameField: 'UC_NM_MN'},
  ],
  ageBins: [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80],
};

// --- Stubs, filled in by later tasks ---
function loadPM25(year) { throw new Error('not implemented'); }
function loadWorldPop(year) { throw new Error('not implemented'); }
function prepAgeBands(wp) { throw new Error('not implemented'); }
function alignToPM25(img, pm25) { throw new Error('not implemented'); }
function computeStatsForYear(boundaries, year, idField, nameField) { throw new Error('not implemented'); }
function processBoundarySet(cfg) { throw new Error('not implemented'); }

// MAIN — commented out until stubs are filled in
// CONFIG.boundaries.forEach(processBoundarySet);

print('pm25_popweighted.js loaded. Stubs not yet implemented.');
```

- [ ] **Step 2: Commit**

```bash
cd hia-tool
git add scripts/pm25_popweighted.js
git commit -m "feat(pm25): scaffold GEE pop-weighted PM2.5 script"
```

---

### Task 2: Implement `loadPM25(year)` and verify the band name

**Files:**
- Modify: `hia-tool/scripts/pm25_popweighted.js`

- [ ] **Step 1: Replace the `loadPM25` stub**

```javascript
function loadPM25(year) {
  // V5GL04 is an ImageCollection with one annual image per year.
  // Band name is 'b1' in the sat-io mirror; we rename to 'pm25'.
  var col = ee.ImageCollection(CONFIG.pm25Collection)
    .filterDate(year + '-01-01', (year + 1) + '-01-01');
  var img = ee.Image(col.first()).rename(['pm25']);
  return img.set('year', year);
}
```

- [ ] **Step 2: Verify the band name assumption**

In the GEE editor, run this probe after the file loads:

```javascript
print('pm25 2020 bands:', loadPM25(2020).bandNames());
print('pm25 2020 sample:', loadPM25(2020));
Map.addLayer(loadPM25(2020), {min: 0, max: 50, palette: ['white','yellow','orange','red','purple']}, 'PM2.5 2020');
```

Expected: the bandNames output shows `['pm25']` and the map displays a global PM2.5 layer. If bandNames shows something other than `['b1']` pre-rename, update the `rename` call accordingly and re-run.

- [ ] **Step 3: Find the actual last year available**

```javascript
var allYears = ee.ImageCollection(CONFIG.pm25Collection)
  .aggregate_array('system:time_start')
  .map(function(ms) { return ee.Date(ms).get('year'); });
print('Available PM2.5 years:', allYears);
```

Note the maximum year in the console. Update `CONFIG.endYear` in the file to match if it differs from 2022.

- [ ] **Step 4: Commit**

```bash
git add scripts/pm25_popweighted.js
git commit -m "feat(pm25): implement loadPM25 with band verification"
```

---

### Task 3: Implement `loadWorldPop(year)` with fallback

**Files:**
- Modify: `hia-tool/scripts/pm25_popweighted.js`

- [ ] **Step 1: Replace the `loadWorldPop` stub**

```javascript
function loadWorldPop(year) {
  // WorldPop pop_age_sex ends at CONFIG.wpLastYear. For later years,
  // carry the last available year forward and tag the source year.
  var effectiveYear = Math.min(year, CONFIG.wpLastYear);
  var col = ee.ImageCollection(CONFIG.wpCollection)
    .filterDate(effectiveYear + '-01-01', (effectiveYear + 1) + '-01-01');
  // pop_age_sex is per-country; mosaic into a global image.
  var img = col.mosaic();
  return img.set('year', year).set('pop_source_year', effectiveYear);
}
```

- [ ] **Step 2: Verify band names**

```javascript
print('wp 2020 bands:', loadWorldPop(2020).bandNames());
```

Expected output includes `population`, `M_0`, `F_0`, `M_1`, `F_1`, `M_5`, `F_5`, …, `M_80`, `F_80` (37 bands total). If names differ, update `prepAgeBands` accordingly in the next task.

- [ ] **Step 3: Commit**

```bash
git add scripts/pm25_popweighted.js
git commit -m "feat(pm25): implement loadWorldPop with 2020 fallback"
```

---

### Task 4: Implement `prepAgeBands(wp)` to compute age totals

**Files:**
- Modify: `hia-tool/scripts/pm25_popweighted.js`

- [ ] **Step 1: Replace the `prepAgeBands` stub**

```javascript
function prepAgeBands(wp) {
  // pop_total comes directly from the 'population' band.
  var popTotal = wp.select('population').rename('pop_total');

  // For each 5-year bin x, age_x = M_x + F_x.
  var ageImages = CONFIG.ageBins.map(function(bin) {
    var m = wp.select('M_' + bin);
    var f = wp.select('F_' + bin);
    return m.add(f).rename('age_' + bin);
  });

  return ee.Image.cat([popTotal].concat(ageImages));
}
```

- [ ] **Step 2: Verify output bands**

```javascript
var prepped = prepAgeBands(loadWorldPop(2020));
print('prepped bands:', prepped.bandNames());
// Expected: ['pop_total', 'age_0', 'age_1', 'age_5', ..., 'age_80'] (19 bands)
```

- [ ] **Step 3: Commit**

```bash
git add scripts/pm25_popweighted.js
git commit -m "feat(pm25): compute age-total bands from M+F"
```

---

### Task 5: Implement `alignToPM25(img, pm25)` with sum reducer

**Files:**
- Modify: `hia-tool/scripts/pm25_popweighted.js`

- [ ] **Step 1: Replace the `alignToPM25` stub**

```javascript
function alignToPM25(img, pm25) {
  // Aggregate from WorldPop's 100m grid onto the PM2.5 grid using a SUM
  // reducer so that total population counts are preserved.
  // setDefaultProjection() is required because pop_age_sex mosaic loses it.
  return img
    .setDefaultProjection(ee.Projection('EPSG:4326').atScale(100))
    .reduceResolution({
      reducer: ee.Reducer.sum().unweighted(),
      maxPixels: 1024
    })
    .reproject(pm25.projection());
}
```

- [ ] **Step 2: Visual verification**

```javascript
var pm25 = loadPM25(2020);
var aligned = alignToPM25(prepAgeBands(loadWorldPop(2020)), pm25);
print('aligned bands:', aligned.bandNames());
print('aligned projection:', aligned.projection());
Map.addLayer(aligned.select('pop_total'),
  {min: 0, max: 10000, palette: ['white','blue','purple','red']},
  'pop_total aligned');
```

Expected: the projection matches `pm25.projection()` and the map shows a global population surface at PM2.5 grid resolution.

- [ ] **Step 3: Commit**

```bash
git add scripts/pm25_popweighted.js
git commit -m "feat(pm25): align WorldPop to PM2.5 grid with sum reducer"
```

---

### Task 6: Implement `computeStatsForYear`

**Files:**
- Modify: `hia-tool/scripts/pm25_popweighted.js`

- [ ] **Step 1: Replace the `computeStatsForYear` stub**

```javascript
function computeStatsForYear(boundaries, year, idField, nameField) {
  var pm25 = loadPM25(year);
  var wp = loadWorldPop(year);
  var popSourceYear = ee.Image(wp).get('pop_source_year');
  var prepped = prepAgeBands(wp);
  var aligned = alignToPM25(prepped, pm25);

  // Per-pixel pm25 * pop_total for the numerator of the weighted mean.
  var pm25xPop = pm25.multiply(aligned.select('pop_total')).rename('pm25_x_pop');

  // Stack everything we need to SUM.
  var sumStack = ee.Image.cat([aligned, pm25xPop]);
  // sumStack bands: pop_total, age_0..age_80, pm25_x_pop

  // Slim down the boundary feature properties to just id + name to avoid
  // collisions on reduceRegions output.
  var slim = boundaries.map(function(f) {
    return ee.Feature(f.geometry(), {
      feature_id: f.get(idField),
      name: f.get(nameField)
    });
  });

  // Sum reducer for all count-like quantities.
  var summed = sumStack.reduceRegions({
    collection: slim,
    reducer: ee.Reducer.sum(),
    scale: pm25.projection().nominalScale(),
    tileScale: CONFIG.tileScale
  });

  // Separate mean reducer for unweighted PM2.5.
  var meanPm25 = pm25.reduceRegions({
    collection: slim,
    reducer: ee.Reducer.mean(),
    scale: pm25.projection().nominalScale(),
    tileScale: CONFIG.tileScale
  });

  // Join by feature_id.
  var join = ee.Join.inner('primary', 'secondary').apply(
    summed, meanPm25,
    ee.Filter.equals({leftField: 'feature_id', rightField: 'feature_id'})
  );

  return join.map(function(pair) {
    var p = ee.Feature(ee.Feature(pair.get('primary')));
    var s = ee.Feature(ee.Feature(pair.get('secondary')));
    return p
      .set('pm25_mean', s.get('mean'))
      .set('year', year)
      .set('pop_source_year', popSourceYear);
  });
}
```

- [ ] **Step 2: Dry run on ne_countries for 2020**

```javascript
var ne = ee.FeatureCollection('projects/hia-tool/assets/ne_countries');
var stats2020 = computeStatsForYear(ne, 2020, 'ADM0_A3', 'NAME');
print('first stats feature:', stats2020.first());
print('size:', stats2020.size());
```

Expected console output: a Feature with properties including `feature_id`, `name`, `year`, `pop_source_year`, `pop_total`, `age_0`…`age_80`, `pm25_x_pop`, `pm25_mean`. Size should match the ne_countries feature count.

- [ ] **Step 3: Commit**

```bash
git add scripts/pm25_popweighted.js
git commit -m "feat(pm25): compute per-feature pop-weighted PM2.5 stats"
```

---

### Task 7: Implement `processBoundarySet` and export loop

**Files:**
- Modify: `hia-tool/scripts/pm25_popweighted.js`

- [ ] **Step 1: Replace the `processBoundarySet` stub and uncomment MAIN**

```javascript
function processBoundarySet(cfg) {
  var boundaries = ee.FeatureCollection(cfg.assetId);
  var years = ee.List.sequence(CONFIG.startYear, CONFIG.endYear).getInfo();

  var perYear = years.map(function(y) {
    return computeStatsForYear(boundaries, y, cfg.idField, cfg.nameField);
  });

  // Concatenate all years into a single FeatureCollection.
  var merged = ee.FeatureCollection(perYear).flatten();

  // Columns we want in the exported CSV, in order.
  var selectors = ['feature_id', 'name', 'year', 'pop_source_year',
                   'pop_total', 'pm25_x_pop', 'pm25_mean'];
  CONFIG.ageBins.forEach(function(bin) { selectors.push('age_' + bin); });

  Export.table.toDrive({
    collection: merged,
    description: 'pm25_' + cfg.name,
    folder: CONFIG.driveFolder,
    fileNamePrefix: 'pm25_' + cfg.name,
    fileFormat: 'CSV',
    selectors: selectors
  });
}

// MAIN
CONFIG.boundaries.forEach(processBoundarySet);
print('Queued ' + CONFIG.boundaries.length + ' exports. Open the Tasks panel to start them.');
```

- [ ] **Step 2: Commit**

```bash
git add scripts/pm25_popweighted.js
git commit -m "feat(pm25): queue drive exports for all boundary sets"
```

---

### Task 8: Smoke test on the smallest boundary set

**Files:**
- Temporarily modify `hia-tool/scripts/pm25_popweighted.js` (do NOT commit the temporary change)

- [ ] **Step 1: Temporarily restrict MAIN to one boundary and one year**

In the editor (not the repo file), replace MAIN with:

```javascript
// SMOKE TEST — do not commit
CONFIG.endYear = CONFIG.startYear; // one year only
processBoundarySet(CONFIG.boundaries[6]); // ne_countries
```

- [ ] **Step 2: Run and start the single task from the Tasks panel**

Click **Run**, then open the **Tasks** tab in the GEE editor and click **Run** on the `pm25_ne_countries` task. Wait for it to finish (usually under 2 minutes for ne_countries × 1 year).

- [ ] **Step 3: Verify the CSV**

Open Google Drive, navigate to `hia_tool_pm25/`, open `pm25_ne_countries.csv`. Expected columns (in order): `feature_id, name, year, pop_source_year, pop_total, pm25_x_pop, pm25_mean, age_0, age_1, …, age_80`. Every row should have a non-zero `pop_total` (except possibly Antarctica), and `pm25_mean` values in the 0–100 µg/m³ range. Spot-check one country: divide `pm25_x_pop / pop_total` and confirm the result is plausible for that country.

- [ ] **Step 4: Restore MAIN and do NOT commit the smoke-test version**

Revert the smoke-test change in the editor before proceeding. The repo's committed version should still loop over all boundaries and all years.

---

### Task 9: Full production run

**Files:** none changed

- [ ] **Step 1: Paste the committed `pm25_popweighted.js` into the GEE editor and click Run**

- [ ] **Step 2: Start each queued task from the Tasks panel**

Nine tasks will appear: `pm25_gadm_1` … `pm25_gadm_6`, `pm25_ne_countries`, `pm25_ne_states`, `pm25_ghs_smod`. Start them all. GADM_5 and GADM_6 can take hours due to feature count; this is expected.

- [ ] **Step 3: Monitor for failures**

If any task fails with a "computation timed out" or "too many concurrent aggregations" error, increase `CONFIG.tileScale` to 16 and re-run that boundary set only (temporarily filter `CONFIG.boundaries` to the failed one, then restore).

- [ ] **Step 4: Download all 9 CSVs**

Download `hia_tool_pm25/pm25_*.csv` from Google Drive into a local folder, e.g. `hia-tool/data/raw/pm25_csv/`.

---

## Stage 2 — Python CSV → Parquet conversion

### Task 10: Add pyarrow to requirements and create the tests directory

**Files:**
- Modify: `hia-tool/requirements.txt`
- Create: `hia-tool/scripts/tests/__init__.py`
- Create: `hia-tool/scripts/tests/fixtures/.gitkeep`

- [ ] **Step 1: Check whether pyarrow is already in requirements.txt**

Run from `hia-tool/`:

```bash
grep -i pyarrow requirements.txt || echo "not present"
```

- [ ] **Step 2: If not present, append it**

```
pyarrow>=14.0
```

- [ ] **Step 3: Install into the venv**

```bash
# Windows (user's environment)
venv\Scripts\pip install pyarrow
```

Expected: successful install with no errors.

- [ ] **Step 4: Create empty test package files**

Create `hia-tool/scripts/tests/__init__.py` as an empty file.
Create `hia-tool/scripts/tests/fixtures/.gitkeep` as an empty file.

- [ ] **Step 5: Commit**

```bash
git add requirements.txt scripts/tests/__init__.py scripts/tests/fixtures/.gitkeep
git commit -m "chore(pm25): add pyarrow dep and test scaffolding"
```

---

### Task 11: Create a fixture CSV

**Files:**
- Create: `hia-tool/scripts/tests/fixtures/sample_pm25.csv`

- [ ] **Step 1: Write the fixture**

Three rows, two features, two years. Includes all expected columns in the order the GEE script exports them.

```csv
feature_id,name,year,pop_source_year,pop_total,pm25_x_pop,pm25_mean,age_0,age_1,age_5,age_10,age_15,age_20,age_25,age_30,age_35,age_40,age_45,age_50,age_55,age_60,age_65,age_70,age_75,age_80
USA,United States of America,2015,2015,320000000.0,3840000000.0,9.5,4000000,16000000,20000000,21000000,22000000,22000000,21000000,20000000,20000000,21000000,22000000,21000000,20000000,18000000,15000000,11000000,8000000,10000000
USA,United States of America,2020,2020,330000000.0,3300000000.0,8.9,4100000,16400000,20500000,21500000,22500000,22500000,21500000,20500000,20500000,21500000,22500000,21500000,20500000,18500000,15500000,11500000,8500000,10500000
IND,India,2015,2015,1300000000.0,78000000000.0,55.0,26000000,104000000,104000000,104000000,104000000,104000000,104000000,91000000,78000000,78000000,65000000,52000000,52000000,39000000,26000000,13000000,6500000,6500000
```

- [ ] **Step 2: Commit**

```bash
git add scripts/tests/fixtures/sample_pm25.csv
git commit -m "test(pm25): add sample CSV fixture"
```

---

### Task 12: Write the failing test for `load_csv`

**Files:**
- Create: `hia-tool/scripts/tests/test_pm25_csv_to_parquet.py`

- [ ] **Step 1: Write the test file**

```python
"""Tests for pm25_csv_to_parquet."""
from pathlib import Path

import pandas as pd
import pytest

from scripts.pm25_csv_to_parquet import (
    AGE_COLUMNS,
    compute_popweighted,
    load_csv,
    write_parquet,
)

FIXTURE = Path(__file__).parent / "fixtures" / "sample_pm25.csv"


def test_load_csv_returns_expected_columns():
    df = load_csv(FIXTURE)
    expected_cols = {
        "feature_id", "name", "year", "pop_source_year",
        "pop_total", "pm25_x_pop", "pm25_mean",
    } | set(AGE_COLUMNS)
    assert expected_cols.issubset(df.columns)


def test_load_csv_dtypes():
    df = load_csv(FIXTURE)
    assert df["year"].dtype.kind == "i"
    assert df["pop_source_year"].dtype.kind == "i"
    assert df["pop_total"].dtype.kind == "f"
    assert df["pm25_mean"].dtype.kind == "f"
    assert df["feature_id"].dtype == object
```

- [ ] **Step 2: Run the test to verify it fails**

From `hia-tool/`:

```bash
venv\Scripts\python -m pytest scripts/tests/test_pm25_csv_to_parquet.py -v
```

Expected: ImportError on `scripts.pm25_csv_to_parquet` (module does not exist yet).

---

### Task 13: Implement `load_csv` and make the test pass

**Files:**
- Create: `hia-tool/scripts/pm25_csv_to_parquet.py`
- Create/modify: `hia-tool/scripts/__init__.py` (empty, only if needed for import)

- [ ] **Step 1: Check whether `scripts/` is importable as a package**

```bash
ls scripts/__init__.py 2>/dev/null || echo "missing"
```

If missing, create an empty `hia-tool/scripts/__init__.py`.

- [ ] **Step 2: Write the module**

```python
"""Convert PM2.5 GEE export CSVs into long-format Parquet files."""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

AGE_BINS = [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80]
AGE_COLUMNS = [f"age_{b}" for b in AGE_BINS]

INT_COLUMNS = ["year", "pop_source_year"]
FLOAT_COLUMNS = ["pop_total", "pm25_x_pop", "pm25_mean", *AGE_COLUMNS]
STRING_COLUMNS = ["feature_id", "name"]

REQUIRED_COLUMNS = STRING_COLUMNS + INT_COLUMNS + FLOAT_COLUMNS


def load_csv(path: Path) -> pd.DataFrame:
    """Read a GEE-exported PM2.5 CSV and coerce dtypes."""
    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{path}: missing columns {missing}")
    for col in INT_COLUMNS:
        df[col] = df[col].astype("int64")
    for col in FLOAT_COLUMNS:
        df[col] = df[col].astype("float64")
    for col in STRING_COLUMNS:
        df[col] = df[col].astype("string").astype(object)
    return df


def compute_popweighted(df: pd.DataFrame) -> pd.DataFrame:
    """Add pm25_popweighted = pm25_x_pop / pop_total and drop the intermediate."""
    out = df.copy()
    with pd.option_context("mode.use_inf_as_na", True):
        out["pm25_popweighted"] = out["pm25_x_pop"] / out["pop_total"]
    out = out.drop(columns=["pm25_x_pop"])
    return out


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write the dataframe to Parquet using pyarrow."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow", index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert PM2.5 CSVs to Parquet")
    parser.add_argument("--input-dir", type=Path, required=True,
                        help="Directory containing pm25_*.csv files")
    parser.add_argument("--output-dir", type=Path, required=True,
                        help="Directory to write pm25_*.parquet files")
    args = parser.parse_args()

    csvs = sorted(args.input_dir.glob("pm25_*.csv"))
    if not csvs:
        raise SystemExit(f"No pm25_*.csv files found in {args.input_dir}")

    for csv in csvs:
        df = load_csv(csv)
        df = compute_popweighted(df)
        out = args.output_dir / (csv.stem + ".parquet")
        write_parquet(df, out)
        print(f"wrote {out} ({len(df)} rows)")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Run the load_csv tests**

```bash
venv\Scripts\python -m pytest scripts/tests/test_pm25_csv_to_parquet.py::test_load_csv_returns_expected_columns scripts/tests/test_pm25_csv_to_parquet.py::test_load_csv_dtypes -v
```

Expected: both tests PASS.

- [ ] **Step 4: Commit**

```bash
git add scripts/pm25_csv_to_parquet.py scripts/__init__.py scripts/tests/test_pm25_csv_to_parquet.py
git commit -m "feat(pm25): load_csv with dtype coercion"
```

---

### Task 14: Write the failing test for `compute_popweighted`

**Files:**
- Modify: `hia-tool/scripts/tests/test_pm25_csv_to_parquet.py`

- [ ] **Step 1: Append the test**

```python
def test_compute_popweighted_matches_manual_ratio():
    df = load_csv(FIXTURE)
    out = compute_popweighted(df)
    # India 2015: 78_000_000_000 / 1_300_000_000 = 60.0
    india_2015 = out[(out.feature_id == "IND") & (out.year == 2015)].iloc[0]
    assert india_2015["pm25_popweighted"] == pytest.approx(60.0, rel=1e-6)
    # USA 2015: 3_840_000_000 / 320_000_000 = 12.0
    usa_2015 = out[(out.feature_id == "USA") & (out.year == 2015)].iloc[0]
    assert usa_2015["pm25_popweighted"] == pytest.approx(12.0, rel=1e-6)


def test_compute_popweighted_drops_intermediate():
    out = compute_popweighted(load_csv(FIXTURE))
    assert "pm25_x_pop" not in out.columns
    assert "pm25_popweighted" in out.columns
```

- [ ] **Step 2: Run the tests**

```bash
venv\Scripts\python -m pytest scripts/tests/test_pm25_csv_to_parquet.py -v
```

Expected: the two new tests PASS (compute_popweighted already implemented in Task 13).

- [ ] **Step 3: Commit**

```bash
git add scripts/tests/test_pm25_csv_to_parquet.py
git commit -m "test(pm25): verify compute_popweighted ratio and column drop"
```

---

### Task 15: Write the failing test for `write_parquet` round-trip

**Files:**
- Modify: `hia-tool/scripts/tests/test_pm25_csv_to_parquet.py`

- [ ] **Step 1: Append the test**

```python
def test_write_parquet_roundtrip(tmp_path):
    df = compute_popweighted(load_csv(FIXTURE))
    out_path = tmp_path / "pm25_sample.parquet"
    write_parquet(df, out_path)
    assert out_path.exists()
    roundtrip = pd.read_parquet(out_path)
    assert len(roundtrip) == len(df)
    assert list(roundtrip.columns) == list(df.columns)
    assert roundtrip["pm25_popweighted"].iloc[0] == pytest.approx(
        df["pm25_popweighted"].iloc[0]
    )
```

- [ ] **Step 2: Run the test**

```bash
venv\Scripts\python -m pytest scripts/tests/test_pm25_csv_to_parquet.py::test_write_parquet_roundtrip -v
```

Expected: PASS (write_parquet already implemented in Task 13).

- [ ] **Step 3: Run the whole test module**

```bash
venv\Scripts\python -m pytest scripts/tests/test_pm25_csv_to_parquet.py -v
```

Expected: all 5 tests PASS.

- [ ] **Step 4: Commit**

```bash
git add scripts/tests/test_pm25_csv_to_parquet.py
git commit -m "test(pm25): verify Parquet round-trip preserves values"
```

---

### Task 16: End-to-end CLI test on real exported CSVs

**Files:** none changed

- [ ] **Step 1: Run the converter against the downloaded CSVs**

```bash
cd hia-tool
venv\Scripts\python -m scripts.pm25_csv_to_parquet \
  --input-dir data/raw/pm25_csv \
  --output-dir data/processed
```

Expected output: nine lines like `wrote data/processed/pm25_gadm_1.parquet (NNNN rows)`.

- [ ] **Step 2: Spot-check the outputs**

```bash
venv\Scripts\python -c "import pandas as pd; df = pd.read_parquet('data/processed/pm25_ne_countries.parquet'); print(df.shape); print(df.columns.tolist()); print(df[df.feature_id=='USA'][['year','pop_total','pm25_mean','pm25_popweighted']])"
```

Expected: 8 years of USA rows with plausible `pop_total` (~310–340M), `pm25_mean` (~6–12 µg/m³), `pm25_popweighted` (~7–13 µg/m³), and `pm25_popweighted` ≥ `pm25_mean` for most years (dense urban populations raise the weighted mean in the US).

- [ ] **Step 3: Do NOT commit the Parquet outputs**

They belong in `data/processed/`, which (check `.gitignore`) should already be ignored. If it isn't, add `data/processed/pm25_*.parquet` to `.gitignore` in this step and commit only the `.gitignore` change:

```bash
grep -q "data/processed" .gitignore || echo "data/processed/" >> .gitignore
git add .gitignore
git diff --cached .gitignore
```

If the diff shows a new line, commit it:

```bash
git commit -m "chore: ignore processed PM2.5 parquet outputs"
```

---

## Self-Review Notes

**Spec coverage:**
- Input datasets ✅ Task 1 CONFIG, Tasks 2–3 loaders
- 2015 → latest year with WorldPop fallback ✅ Tasks 2, 3
- Grid harmonization with sum reducer ✅ Task 5
- Age-only totals from M+F ✅ Task 4
- `pm25 × pop` numerator + sum reducer + ratio ✅ Tasks 6, 13
- Unweighted mean reference column ✅ Task 6
- 9 boundary sets, per-set CSV exports ✅ Tasks 1, 7, 9
- Parquet conversion with long-format schema ✅ Tasks 10–15
- Output location `data/processed/` ✅ Task 16
- `pop_source_year` column for carry-forward transparency ✅ Tasks 3, 6, 13

**Risks handled:**
- tileScale bump at 8 by default (Task 1), documented escalation to 16 (Task 9)
- Asset field names verified before coding (Task 0)
- Smoke test before full run (Task 8)
- Feature property slim-down to avoid `reduceRegions` collisions (Task 6)
- `setDefaultProjection` on the WorldPop mosaic before `reduceResolution` (Task 5)

**Known gaps the engineer may need to resolve at runtime:**
- If V5GL04 band name is not `b1`, update the `rename` in Task 2.
- If WorldPop band naming differs from `M_x`/`F_x`, update `prepAgeBands` in Task 4.
- If GADM_5 / GADM_6 exports time out even at tileScale 16, split the boundary FC by continent (filter on `GID_0` prefix) and export in chunks. This is a runtime fallback, not part of the plan's happy path.
