# Outstanding work — HIA tool

Living checklist of known gaps that need follow-up work. Tracks items that were flagged in review but not shipped in the same commit, so they don't get lost.

## CRF / engine

- [x] ~~**Wire Fusion marginal-risk tables into the backend.**~~ Done 2026-04-21 in commits adding `backend/etl/process_fusion.py` and the spline-lookup branch in `fusion()`. `fusion_pm25_acm` now interpolates RR from the Weichenthal et al. (2022) hybrid table (eSCHIF below 9.8 μg/m³, Fusion above). Source parquet under `data/processed/fusion/pm25/all_cause_mortality.parquet`.
- [ ] **Publish Fusion parameters for CVD and lung-cancer endpoints.** The Vohra HealthBurden repo only ships non-accidental-deaths parameters, so `fusion_pm25_cvd` and `fusion_pm25_lc` still fall back to log-linear. Once endpoint-specific CSVs are available, drop them into `data/raw/fusion/` and extend `backend/etl/process_fusion.py` to produce `data/processed/fusion/pm25/cardiovascular_mortality.parquet` and `.../lung_cancer.parquet`, then add entries to `_CRF_ID_TO_FUSION` in `backend/services/hia_engine.py`.

- [ ] **Confirm the GBD 2023 beta for the all-age LRI CRF.** `crf-library.json` entry `gbd_pm25_lri` had `ageRange: "0–4"`; the label was flipped to "All ages" on 2026-04-21 per product direction, but the beta (`0.00978`, CI `0.00437–0.01514`) may still be the under-5 value from IHME. Cross-check against IHME's all-age LRI exposure-response file and update beta/betaLow/betaHigh if the all-age curve differs.

## Multi-year results

- [x] ~~**Plan 3 — post-results "Compare another year" flow.**~~ Shipped 2026-04-22 on `feature/polygon-results`. Store bumped to v8 with `additionalRuns`; `frontend/src/lib/api.js` exports `cloneConfigWithYear` and `runAnalysisForYear`; `Results.jsx` renders `CompareAnotherYearCard` below the tabs and stacks `AdditionalRunSummary` cards above it. Cap at 10 total runs; confirmation modal fires after the second additional run.

### Follow-ups suggested by Plan 3 Non-goals

- [x] ~~**Narrow the "Compare another year" picker to dataset-supported years.**~~ Shipped 2026-05-01. `Results.jsx` now fetches `/api/data/datasets`, looks up the primary run's baseline dataset, and passes `yearsFor(baseline, country)` (intersected with the control dataset when present) as `allowedYears`. Falls back to 1990..current for manual / file-upload baselines.

## Year pickers — per-country coverage

- [x] ~~**Backend: emit `years_by_country` per dataset.**~~ Shipped 2026-05-01 (`96dc925`). `_scan_datasets` now attaches a `years_by_country` map to direct concentration, EPA AQS, and WHO AAP entries alongside the union `years` list. EPA AQS scans each year file's `admin_id` to record per-state coverage; WHO AAP does the same per ISO3.
- [x] ~~**Frontend: `yearsFor` consumes `years_by_country`.**~~ Shipped 2026-05-01 (`96dc925`). Returns the union over keys matching the country's equivalence set (with `US-XX` collapsing into USA), falls back to `dataset.years` when `years_by_country` is absent.
- [x] ~~**Wire Step 3 population picker to dataset coverage.**~~ Shipped 2026-05-01. `Step3Population.jsx` now fetches `/api/data/datasets?type=population&country={country}` and constrains the YearField's options to the union of those datasets' years — but only when the Built-in tab is active. Manual entry and file upload retain the unconstrained year range since the year is metadata about user-supplied numbers in those cases.
- [ ] **Wire Step 4 incidence picker through `yearsFor`.** Step 4 still derives year options independently. Requested 2026-04-22 during demo prep.
- [ ] **Trend chart visualization across stacked year runs.** Current display is side-by-side cards; a follow-up can add a sparkline/trend chart tab surfacing the year-over-year mortality and CI envelope.
- [ ] **Include additional runs in PDF / CSV exports.** Export currently writes only the primary run. A follow-up can append a "Multi-year comparison" table / page summarising each additional run's year + totalDeaths.
