# Outstanding work — HIA tool

Living checklist of known gaps that need follow-up work. Tracks items that were flagged in review but not shipped in the same commit, so they don't get lost.

## CRF / engine

- [x] ~~**Wire Fusion marginal-risk tables into the backend.**~~ Done 2026-04-21 in commits adding `backend/etl/process_fusion.py` and the spline-lookup branch in `fusion()`. `fusion_pm25_acm` now interpolates RR from the Weichenthal et al. (2022) hybrid table (eSCHIF below 9.8 μg/m³, Fusion above). Source parquet under `data/processed/fusion/pm25/all_cause_mortality.parquet`.
- [ ] **Publish Fusion parameters for CVD and lung-cancer endpoints.** The Vohra HealthBurden repo only ships non-accidental-deaths parameters, so `fusion_pm25_cvd` and `fusion_pm25_lc` still fall back to log-linear. Once endpoint-specific CSVs are available, drop them into `data/raw/fusion/` and extend `backend/etl/process_fusion.py` to produce `data/processed/fusion/pm25/cardiovascular_mortality.parquet` and `.../lung_cancer.parquet`, then add entries to `_CRF_ID_TO_FUSION` in `backend/services/hia_engine.py`.

- [ ] **Confirm the GBD 2023 beta for the all-age LRI CRF.** `crf-library.json` entry `gbd_pm25_lri` had `ageRange: "0–4"`; the label was flipped to "All ages" on 2026-04-21 per product direction, but the beta (`0.00978`, CI `0.00437–0.01514`) may still be the under-5 value from IHME. Cross-check against IHME's all-age LRI exposure-response file and update beta/betaLow/betaHigh if the all-age curve differs.

## Multi-year results (Plan 3 not started)

- [ ] Plan file: `docs/superpowers/plans/2026-04-21-plan3-post-results-multi-year.md`.
  Implementation deferred while spatial-resolution work happens in parallel. Before resuming, re-check that the "clone analysis config, change year, run again" assumption still holds against any compute-path changes from the spatial work.
