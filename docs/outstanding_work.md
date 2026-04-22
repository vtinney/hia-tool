# Outstanding work — HIA tool

Living checklist of known gaps that need follow-up work. Tracks items that were flagged in review but not shipped in the same commit, so they don't get lost.

## CRF / engine

- [x] ~~**Wire Fusion marginal-risk tables into the backend.**~~ Done 2026-04-21 in commits adding `backend/etl/process_fusion.py` and the spline-lookup branch in `fusion()`. `fusion_pm25_acm` now interpolates RR from the Weichenthal et al. (2022) hybrid table (eSCHIF below 9.8 μg/m³, Fusion above). Source parquet under `data/processed/fusion/pm25/all_cause_mortality.parquet`.
- [ ] **Publish Fusion parameters for CVD and lung-cancer endpoints.** The Vohra HealthBurden repo only ships non-accidental-deaths parameters, so `fusion_pm25_cvd` and `fusion_pm25_lc` still fall back to log-linear. Once endpoint-specific CSVs are available, drop them into `data/raw/fusion/` and extend `backend/etl/process_fusion.py` to produce `data/processed/fusion/pm25/cardiovascular_mortality.parquet` and `.../lung_cancer.parquet`, then add entries to `_CRF_ID_TO_FUSION` in `backend/services/hia_engine.py`.

- [ ] **Confirm the GBD 2023 beta for the all-age LRI CRF.** `crf-library.json` entry `gbd_pm25_lri` had `ageRange: "0–4"`; the label was flipped to "All ages" on 2026-04-21 per product direction, but the beta (`0.00978`, CI `0.00437–0.01514`) may still be the under-5 value from IHME. Cross-check against IHME's all-age LRI exposure-response file and update beta/betaLow/betaHigh if the all-age curve differs.

## Multi-year results

- [x] ~~**Plan 3 — post-results "Compare another year" flow.**~~ Shipped 2026-04-22 on `feature/polygon-results`. Store bumped to v8 with `additionalRuns`; `frontend/src/lib/api.js` exports `cloneConfigWithYear` and `runAnalysisForYear`; `Results.jsx` renders `CompareAnotherYearCard` below the tabs and stacks `AdditionalRunSummary` cards above it. Cap at 10 total runs; confirmation modal fires after the second additional run.

### Follow-ups suggested by Plan 3 Non-goals

- [ ] **Narrow the "Compare another year" picker to dataset-supported years.** The picker currently offers 1990..current year. It should query `/api/data/datasets` (filtered to the primary run's pollutant + country) and only surface years the backing dataset actually covers — mirrors the Step 2 year-after-dataset constraint.
- [ ] **Trend chart visualization across stacked year runs.** Current display is side-by-side cards; a follow-up can add a sparkline/trend chart tab surfacing the year-over-year mortality and CI envelope.
- [ ] **Include additional runs in PDF / CSV exports.** Export currently writes only the primary run. A follow-up can append a "Multi-year comparison" table / page summarising each additional run's year + totalDeaths.
