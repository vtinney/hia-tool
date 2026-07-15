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
- [x] ~~**Wire Step 4 incidence picker to dataset coverage.**~~ Shipped 2026-05-01. `Step4HealthData.jsx` now fetches `/api/data/datasets?type=incidence&country={country}` and constrains the YearField to the union of years across all incidence datasets for the country — Built-in tab only. Per-cause availability is still surfaced via the existing `builtinAvailability` probe (greys out endpoints with no data for the chosen year).
- [x] ~~**Trend chart visualization across stacked year runs.**~~ Shipped — `TrendTab` in `Results.jsx` renders a Recharts mean + 95% CI envelope from `buildTrendSeries`.
- [x] ~~**Include additional runs in PDF / CSV exports.**~~ Shipped 2026-07-13. CSV already appended a "Multi-year comparison" block; PDF now gets a matching comparison page. Also fixed a latent bug where `ExportTab` didn't receive `primaryYear`/`additionalRuns` as props, which crashed *every* CSV download.

## Built-in spatial routing

- [x] ~~**Route built-in US state/county runs to the backend spatial engine.**~~ Shipped 2026-07-13. `shouldUseBuiltinSpatial` now accepts `state`/`county` (with a state selected); `buildBuiltinSpatialConfig` passes the real `analysisLevel` + `countyFilter`. Verified live: DE county = 3 zones, DE state = 1 zone, consistent totals. Country level intentionally stays on the scalar path (avoids an all-US tract dissolve).

## GADM admin-2 global export

- [ ] **Confirm 760/760 GADM admin-2 shards, then build parquet.** As of 2026-07-13, 747/760 were complete; the residual 14 shards were relaunched (`relaunch_missing_shards_v2.py` for timeouts, `relaunch_arctic_oom.py` for the Baffin/Nunavut out-of-memory units). Once the EE queue drains, re-audit Drive (`rclone lsf gdrive: --include "pm25_gadm_adm2_*.csv" -R`), expect 760/760, then `pm25_csv_to_parquet.py --boundary gadm_adm2`. See vault note *HIA Tool - GADM Admin-2 GEE Timeout Retries* (2026-07-13).

## GHS-SMOD urban centres (2026-07-15)

- [x] ~~**Wire the GHS-SMOD urban-centre data into the app.**~~ Shipped 2026-07-15 on `feature/polygon-results` (plan: `docs/superpowers/plans/2026-07-15-ghs-smod-urban-centre-wiring.md`). New boundary gpkg built from UCDB R2024A (`scripts/ghs_ucdb_to_boundaries.py` — the old `ghs_smod_named.gpkg` is the wrong R2023A vintage, now unused); resolver `prepare_urban_centre_inputs`; `analysisLevel: "urban"` + `cityIds` on `/api/compute/spatial`; `ghs_smod_pm25_global` dataset entry; `GET /api/data/urban-centres/{country}` picker endpoint; Step 1 "Urban centres (cities)" level with searchable multi-select + top-N shortcut (PM2.5-only, gated). Verified live: Mexico 2020 = 182 centres, 69.2 M urban pop, Mexico City top (19,409,568 / 22.4 µg/m³), 54,404 attributable all-cause deaths vs 0 µg/m³ (single EPA ACM CRF, rate 0.008); top-2 subset = 2 zones / 22,313. Backend 118 pytest, frontend 133 vitest, build clean.
- [ ] **Browser click-through of the Step 1 city picker** (radio → picker loads → search → top-N → run → per-city zones). API + unit layers verified; the visual pass needs a human (Chrome extension wasn't connected this session). Dev servers: `venv\Scripts\python.exe -m uvicorn backend.main:app --port 8000` + `npm run dev` (frontend, port 3000).
- [ ] **Delete or archive `data/processed/boundaries/ghs_smod/ghs_smod_named.gpkg`?** Wrong GHS-UCDB vintage (R2023A, 11,534 features; 151 exported centres missing incl. Wenzhou). Nothing reads it after this wiring — flagging for Veronica's call rather than deleting.
- [ ] **US urban centres are excluded by design** (US flow keeps tract/county/state). Revisit only if a US city-level use case appears.
- [x] ~~**Surface WorldPop population on Step 3.**~~ Shipped 2026-07-15 (follow-on request). `_scan_datasets` emits `gadm_adm2_pop_global` + `ghs_smod_pop_global` population entries; `/api/data/datasets?country=` now admits global datasets whose `countries_covered` includes the country; `/api/data/population/{country}/{year}?dataset=` serves per-zone WorldPop totals + age bins from the GEE parquets; Step 3's Built-in tab gained a "Data source" dropdown (defaults to the WorldPop product matching the Step 1 analysis level, else legacy country totals). Verified live: MEX adm2 = 2,457 units / 112.0M, urban = 182 units / 69.2M.
