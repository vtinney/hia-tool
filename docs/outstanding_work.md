# Outstanding work — HIA tool

Living checklist of known gaps that need follow-up work. Tracks items that were flagged in review but not shipped in the same commit, so they don't get lost.

## CRF / engine

- [ ] **Wire Fusion marginal-risk tables into the backend.** `backend/services/hia_engine.py:538` dispatches `fusion(beta, c_base, c_ctrl, y0, pop)` without an `mr_table`, so every fusion-hybrid CRF currently falls back to the log-linear placeholder. The three fusion CRFs in `frontend/src/data/crf-library.json` are labelled Fusion but don't compute as Fusion until this is fixed.
  - Parameter source: https://github.com/karnvohra/HealthBurden/tree/main (Vohra et al. HealthBurden repo). Specifically need the CSV file with the non-accidental-death parameters plus the accompanying R code so the integration matches the published method.
  - Target: an ETL that produces `data/processed/fusion/{pollutant}/{endpoint}.parquet` with columns `[concentration, marginal_risk]`, plus a lookup (mirror of `_CRF_ID_TO_SPLINE`) wired through `fusion(...)` in the dispatcher.

- [ ] **Confirm the GBD 2023 beta for the all-age LRI CRF.** `crf-library.json` entry `gbd_pm25_lri` had `ageRange: "0–4"`; the label was flipped to "All ages" on 2026-04-21 per product direction, but the beta (`0.00978`, CI `0.00437–0.01514`) may still be the under-5 value from IHME. Cross-check against IHME's all-age LRI exposure-response file and update beta/betaLow/betaHigh if the all-age curve differs.

## Multi-year results (Plan 3 not started)

- [ ] Plan file: `docs/superpowers/plans/2026-04-21-plan3-post-results-multi-year.md`.
  Implementation deferred while spatial-resolution work happens in parallel. Before resuming, re-check that the "clone analysis config, change year, run again" assumption still holds against any compute-path changes from the spatial work.
