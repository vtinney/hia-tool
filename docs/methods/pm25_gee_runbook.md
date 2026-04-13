# PM2.5 × Population Earth Engine Runbook

A standalone, step-by-step walkthrough for running `scripts/pm25_popweighted.js`
in the Google Earth Engine Code Editor to produce population-weighted PM2.5
summaries for the HIA tool's 9 boundary sets.

---

## 1. Overview

You are about to run an Earth Engine JavaScript script that computes annual
population-weighted and unweighted PM2.5 plus age-structured population counts
for every feature in 9 boundary FeatureCollections. The script queues 9 CSV
exports to a Google Drive folder; you start each export from the Tasks panel
and then download the CSVs locally so the Python converter can turn them into
Parquet files for the HIA tool.

---

## 2. Prerequisites

- A Google account with **Earth Engine access** enabled
  (https://code.earthengine.google.com/ must load without a signup wall).
- **Read access** to the 3 boundary assets under `projects/hia-tool/assets/`:
  `ne_countries`, `ne_states`, `GHS_SMOD`. If you are not the asset owner,
  ask the project owner to share them with your Google account.
- **Google Drive** space (a few tens of MB is plenty) for the exported CSVs.
- Local clone of `hia-tool` with the script at
  `scripts/pm25_popweighted.js`.

---

## 3. Step 1 — Verify boundary field names

The script identifies each boundary feature by a stable `idField` (e.g.
`GID_2`) and labels it with a human-readable `nameField` (e.g. `NAME_2`). These
field names vary by dataset. Before running anything heavy, verify the
assumptions baked into `CONFIG.boundaries`.

**Probe.** Paste the following into a scratch GEE script and click Run:

```javascript
var assets = [
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

**Where to look in the console.** Each asset produces two `print` lines. Click
the triangle next to `first feature:` → expand `Feature` → expand
`properties`. You will see every available field name with a sample value
next to it. Pick:

- the field that looks most like a **stable unique ID** (codes, not names) for
  `idField`;
- the field that looks most like a **human-readable label** (country / region
  name) for `nameField`.

**Defaults baked into the script.** Compare what you see to these:

| Asset           | idField        | nameField   |
|-----------------|----------------|-------------|
| ne_countries    | `ADM0_A3`      | `NAME`      |
| ne_states       | `adm1_code`    | `name`      |
| GHS_SMOD        | `ID_HDC_G0`    | `UC_NM_MN`  |

**If any differ**, edit `CONFIG.boundaries` in `scripts/pm25_popweighted.js`
before running, changing the `idField` / `nameField` string for that row to
match what you found. Save the file.

---

## 4. Step 2 — Verify PM2.5 band name and available years

**Probe A — band name.** Paste into the GEE editor after loading the script
(or into a scratch script with an inline copy of `loadPM25`):

```javascript
print('pm25 2020 bands:', loadPM25(2020).bandNames());
print('pm25 2020 sample:', loadPM25(2020));
Map.addLayer(loadPM25(2020),
  {min: 0, max: 50, palette: ['white','yellow','orange','red','purple']},
  'PM2.5 2020');
```

Expected: `bandNames` output is `['pm25']` (because `loadPM25` renames the
single band), and the map shows a plausible global PM2.5 layer.

**What to do if the band name is not `b1`.** There is nothing to change. The
`.rename(['pm25'])` call in `loadPM25` works on whatever single band the image
carries — this probe simply confirms the image has exactly one band (it
does).

**Probe B — available years.**

```javascript
var allYears = ee.ImageCollection(
  'projects/sat-io/open-datasets/GLOBAL-SATELLITE-PM25/ANNUAL'
).aggregate_array('system:time_start')
 .map(function(ms) { return ee.Date(ms).get('year'); });
print('Available PM2.5 years:', allYears);
```

Note the maximum year in the printed list. **If the latest year is not 2022**,
edit `CONFIG.endYear` in `scripts/pm25_popweighted.js` to the latest year you
see (for example `endYear: 2021` or `endYear: 2023`) and save.

---

## 5. Step 3 — Verify WorldPop band names

The script uses the sat-io community mirror
`projects/sat-io/open-datasets/WORLDPOP/agesex`, which covers 2015–2030
annually. Each image's bands are lowercase and zero-padded:
`m_00, m_01, m_05, m_10, …, m_85, m_90` and the same for `f_*`.

**Probe.**

```javascript
print('wp 2020 bands:', loadWorldPop(2020).bandNames());
print('wp 2015 size:',
      ee.ImageCollection(CONFIG.wpCollection)
        .filterDate('2015-01-01', '2016-01-01').size());
```

Expected:
- `wp 2020 bands` → a list containing `m_00, f_00, m_01, f_01, m_05, f_05, …,
  m_90, f_90` (40 age bands). A `population` total band may or may not be
  present — the script does not depend on it; `pop_total` is computed as the
  sum over all 20 `age_*` bands.
- `wp 2015 size` → a number greater than zero, confirming 2015 data is
  present in the collection.

**What to do if band names differ.** If you see uppercase names (`M_00`,
`F_00`), edit `prepAgeBands` in `scripts/pm25_popweighted.js` and change
`'m_' + pad` / `'f_' + pad` to `'M_' + pad` / `'F_' + pad`. If the top age
bin is not 90 (for example if the collection stops at `m_80`), edit
`CONFIG.ageBins` and remove the bins that don't exist. If `wp 2015 size`
returns 0, the collection may not cover that year in your project — stop and
report back.

---

## 6. Step 4 — Smoke test on one boundary × one year

This is a fast end-to-end dry run on `ne_countries` for a single year, so you
can catch any schema or access issues before running the full pipeline.

**Paste this snippet at the very bottom of the script, below the existing
MAIN loop:**

```javascript
// SMOKE TEST — DO NOT commit this block.
// Overrides the main loop by restricting to ne_countries × one year.
CONFIG.endYear = CONFIG.startYear; // 2015 only
processBoundarySet(CONFIG.boundaries[0]); // index 0 == ne_countries
```

1. Click **Run**.
2. Open the **Tasks** tab in the right sidebar. A task called
   `pm25_ne_countries` should appear at the top.
3. Click **Run** on `pm25_ne_countries`. Confirm in the dialog and wait —
   usually under 2 minutes for ne_countries × 1 year.
4. Open Google Drive, navigate to the `hia_tool_pm25` folder, download
   `pm25_ne_countries.csv`.
5. **Sanity-check the columns.** Open the CSV and confirm these columns
   exist (in order):
   `feature_id, name, year, pop_source_year, pop_total, pm25_x_pop,
   pm25_mean, age_0, age_1, age_5, age_10, age_15, age_20, age_25, age_30,
   age_35, age_40, age_45, age_50, age_55, age_60, age_65, age_70, age_75,
   age_80, age_85, age_90`.
6. **Sanity-check a few values.** For a country you know well (e.g. India,
   Germany, Nigeria), compute `pm25_x_pop / pop_total` by hand in the CSV and
   confirm the result is in a plausible µg/m³ range (India ~40–70, Germany
   ~10–15, Nigeria ~30–60). Confirm `pm25_mean` is also in-range but generally
   lower than the weighted value for unevenly-populated countries.

**Before moving on, REVERT the smoke-test override.** Delete the
`CONFIG.endYear = CONFIG.startYear;` and the `processBoundarySet(...)` lines
you pasted. The script should once again end with the main
`CONFIG.boundaries.forEach(processBoundarySet);` loop only.

---

## 7. Step 5 — Full production run

1. Make sure your editor contains the committed, unmodified contents of
   `scripts/pm25_popweighted.js` (smoke-test override reverted) and that any
   `CONFIG` edits from Steps 1–3 are in place.
2. Click **Run**. After a few seconds, the Tasks panel should show 3 queued
   tasks: `pm25_ne_countries`, `pm25_ne_states`, `pm25_ghs_smod`.
3. Click **Run** on each task in turn. You can start them all in parallel —
   GEE will schedule them on its backend.
4. **Expected durations.** Each task typically finishes within tens of minutes
   depending on feature count and year range.
5. Leave the tasks running. They will complete in the background even if you
   close the browser tab. Check back by reopening the Code Editor and viewing
   the Tasks panel, or by checking the Drive folder.

---

## 8. Step 6 — Download CSVs

When all 3 tasks show **COMPLETED** in the Tasks panel:

1. Open Google Drive → the `hia_tool_pm25` folder.
2. Select all 3 `pm25_*.csv` files and download them.
3. On your local machine, move them to:
   ```
   hia-tool/data/raw/pm25_csv/
   ```
   Create that directory if it doesn't exist.
4. Verify all 3 filenames are present:
   `pm25_ne_countries.csv`, `pm25_ne_states.csv`, `pm25_ghs_smod.csv`.

---

## 9. Troubleshooting

### 9.1 Export times out or hits "too many concurrent aggregations"

Symptom: a task fails with a message like `Computation timed out` or
`Too many concurrent aggregations`.

Fix:

1. In `scripts/pm25_popweighted.js`, change `tileScale: 8` to `tileScale: 16`.
2. Temporarily restrict the main loop to the failed boundary only, e.g.:
   ```javascript
   // Temporary: re-run only the failed boundary.
   processBoundarySet(CONFIG.boundaries[1]); // ne_states
   ```
   (Comment out or delete the `CONFIG.boundaries.forEach(processBoundarySet);`
   line while you do this.)
3. Click Run, start the new task, wait for completion.
4. **Restore the full main loop** before committing or re-running.

### 9.2 `Image.select: Band pattern 'm_XX' was applied to an Image with no bands`

Symptom: the script errors during `prepAgeBands` with a message about an
Image with no bands. This usually means `filterDate` returned zero images for
the requested year, so `col.mosaic()` produced an empty image and every
subsequent `select` fails.

Fix: re-run the WorldPop band-name probe from Step 3, paying attention to
the `wp 2015 size` line. If it is 0, the requested year is not present in
`CONFIG.wpCollection`. Double-check that `CONFIG.wpCollection` is set to
`projects/sat-io/open-datasets/WORLDPOP/agesex` (NOT the Google-maintained
`WorldPop/GP/100m/pop_age_sex`, which at time of writing contains only 2020).

If the band names are present but have a different case or padding
(e.g. `M_0` instead of `m_00`), edit the `'m_' + pad` / `'f_' + pad` lines in
`prepAgeBands` to match.

### 9.3 PM2.5 band is not `b1`

This is a non-issue. `loadPM25` uses `.rename(['pm25'])` which works on any
single-band image regardless of the original band name. The Step 2 probe
simply confirms there is exactly one band in the image (there is). No code
change needed.

### 9.4 `reduceRegions` output is missing the `feature_id` column

Symptom: the exported CSV has rows but `feature_id` is blank or absent.

Cause: the `idField` string in `CONFIG.boundaries` for that asset does not
match an actual property on the asset's features, so `f.get(idField)` returns
null in the `slim` mapping.

Fix: re-run the Step 1 probe for that specific asset, inspect the actual
property names in the console, update the corresponding row in
`CONFIG.boundaries`, and re-run that boundary's export.

---

## 10. What the output CSV looks like

Each `pm25_*.csv` is long-format: **one row per (feature, year)**. Columns,
in order:

| Column            | Type    | Description                                                       |
|-------------------|---------|-------------------------------------------------------------------|
| `feature_id`      | string  | Stable ID from the boundary's native ID field (e.g. `ADM0_A3`)    |
| `name`            | string  | Human-readable name from the boundary's name field                |
| `year`            | int     | Calendar year (2015 through `CONFIG.endYear`)                     |
| `pop_source_year` | int     | WorldPop year used (equals `year` — sat-io covers 2015–2030)      |
| `pop_total`       | float64 | Sum over all 20 age bins within the feature (people)              |
| `pm25_x_pop`      | float64 | Sum of (pm25 × pop_total) within the feature (µg/m³ × people)    |
| `pm25_mean`       | float64 | Unweighted spatial mean of pm25 within the feature (µg/m³)        |
| `age_0`           | float64 | Sum of m_00 + f_00 (ages 0–1) within the feature                 |
| `age_1`           | float64 | Sum of m_01 + f_01 (ages 1–4)                                     |
| `age_5`           | float64 | Sum of m_05 + f_05 (ages 5–9)                                     |
| `age_10`          | float64 | Ages 10–14                                                        |
| `age_15`          | float64 | Ages 15–19                                                        |
| `age_20`          | float64 | Ages 20–24                                                        |
| `age_25`          | float64 | Ages 25–29                                                        |
| `age_30`          | float64 | Ages 30–34                                                        |
| `age_35`          | float64 | Ages 35–39                                                        |
| `age_40`          | float64 | Ages 40–44                                                        |
| `age_45`          | float64 | Ages 45–49                                                        |
| `age_50`          | float64 | Ages 50–54                                                        |
| `age_55`          | float64 | Ages 55–59                                                        |
| `age_60`          | float64 | Ages 60–64                                                        |
| `age_65`          | float64 | Ages 65–69                                                        |
| `age_70`          | float64 | Ages 70–74                                                        |
| `age_75`          | float64 | Ages 75–79                                                        |
| `age_80`          | float64 | Ages 80–84                                                        |
| `age_85`          | float64 | Ages 85–89                                                        |
| `age_90`          | float64 | Ages 90+                                                          |

The population-weighted mean itself is **not** a column in the CSV. The
downstream Parquet converter computes
`pm25_popweighted = pm25_x_pop / pop_total` and adds it as a new column,
mapping null/zero `pop_total` rows to null safely.

---

## 11. Next step

Run the Python converter:

```
python -m scripts.pm25_csv_to_parquet \
  --input-dir data/raw/pm25_csv \
  --output-dir data/processed
```

This reads every `pm25_*.csv` from `data/raw/pm25_csv/`, validates the
schema, computes `pm25_popweighted`, and writes one Parquet file per
boundary set into `data/processed/` (`pm25_ne_countries.parquet`,
`pm25_ne_states.parquet`, `pm25_ghs_smod.parquet`). Those Parquet files
are the inputs the HIA tool actually ingests.
