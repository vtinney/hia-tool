# ACS demographics — next steps

Status snapshot as of 2026-04-23. The mechanical work (merge + full
backfill) is **done**. What remains is product-shaped (EJ format) and
low-priority cleanup. This file tracks what's left.

## Current state

- **Branch:** `feature/acs-demographics` was **merged into `master` on
  2026-04-13** (merge commit `5262cdc`). Master has moved 52 commits
  beyond the merge, including GBD baseline rates ETL and the 2026-04-21
  EJ template wiring (EJContextSection, TractChoroplethMap,
  fetchDemographics, studyAreaToFilter, pickVintage, `ejFraming` gate).
- **ETL:** `backend/etl/process_acs.py` — CLI with `--vintage` and `--all`.
  Pulls ACS 5-year tables B03002, B19013, C17002 for all 50 states + DC +
  PR via cenpy, joins to TIGER cartographic-boundary tracts via pygris.
- **Data built:** All 10 vintages **2015–2024** present in
  `hia-tool/data/processed/demographics/us/` as of 2026-04-21
  (~650 MB total, gitignored).
- **Endpoint:** `GET /api/data/demographics/{country}/{year}` with optional
  `state` (2-digit FIPS), `county` (3-digit FIPS, requires state), and
  `simplify` (default 0.0001°, 0 disables) query params. Full state
  response ~13 MB; single county ~2 MB.
- **Tests:** 38 passing (29 ETL + health + hia-engine, 9 endpoint).
- **Venv:** `hia-tool/venv` (shared with master worktree) is fully
  provisioned on Python 3.13.

## Immediate next steps (mechanical)

1. ~~**Merge `feature/acs-demographics` → `master`.**~~ Done 2026-04-13
   (`5262cdc`).
2. ~~**Backfill remaining vintages.**~~ Done 2026-04-13 / 2026-04-21 —
   all 10 vintages (2015–2024) built.

## Product-shaped next steps

Scope guidance in memory under `project_hia_acs_scope.md`:

> ACS data is opt-in EJ only — does not appear in standard HIA runs.
> When a user picks an EJ-framed analysis format, the initial scope is
> just two fields: `pct_minority` and `pct_below_200_pov`.

3. ~~**Design the EJ analysis format.**~~ Closed 2026-04-23. Verified
   landing on master:
   - Opt-in via template pick on Home.jsx (`us_tract_pm25_ej.json`,
     tagged "EJ"). No in-wizard toggle.
   - Post-run results section gated on `ejFraming` (`Results.jsx:608-612`,
     `EJContextSection.jsx`); presentational only, HIA engine untouched.
   - Two-field scope confirmed (`pct_minority`, `pct_below_200_pov`) —
     no out-of-scope columns surfaced.
   - Aggregates computed client-side via population-weighted means,
     weighted only over tracts the HIA engine computed (spec D4).
   - 40/40 EJ-related vitest suites pass.
4. ~~**Wire frontend to endpoint.**~~ Closed 2026-04-23. `fetchDemographics`
   in `lib/api.js`, `studyAreaToFilter` + `pickVintage` in
   `lib/demographics.js`, `TractChoroplethMap` renders the choropleth with
   a field toggle, error/loading/retry states wired.
5. **Aggregate stats** — client-side only today, which is sufficient for
   the EJ results section. A backend helper in `hia_engine.py` is only
   needed if aggregates need to surface outside the JS runtime (e.g., PDF
   export). Out of scope until that requirement lands.

## EJ carve-outs (small, not blockers)

6. **Backend should return `demographics_vintages`.** `Results.jsx:603-606`
   has a TODO fallback hardcoded to `[2015..2024]`. Add
   `demographics_vintages` to the analysis payload in
   `backend/services/hia_engine.py` (or wherever the result envelope is
   assembled) so added/removed vintages flow through without a frontend
   code change.
7. **`exportConfig` drops `ejFraming`.** `useAnalysisStore.js:177-182`
   omits the flag when building a saveable config, so user-saved custom
   templates built from an EJ run replay as non-EJ. Add `ejFraming` to
   the JSON.stringify payload.

## Known issues worth cleaning up (low priority)

6. **FastAPI `on_event` deprecation.** `backend/main.py:35` uses
   `@app.on_event("startup")`, which is deprecated in favor of lifespan
   handlers. Cosmetic, pre-existing, unrelated to ACS. Two warnings
   show in every test run.
7. **Endpoint unit-level validation of `state`/`county` format.**
   Current code accepts any 2-char / 3-char string. Census FIPS are
   numeric — a typo like `?state=ca` silently returns 404 via the empty-
   filter path. Consider adding a `regex=r"^\d{2}$"` constraint on
   `state` and `r"^\d{3}$"` on `county` in the Query params so the
   endpoint returns 422 on malformed input instead of 404.
8. **No index on `state_fips` / `county_fips`.** Parquet filtering
   currently does a full-column scan in pandas. At 85k rows this is
   imperceptible (~20 ms), so only worth revisiting if nationwide
   multi-vintage queries become a hot path.

## Nice-to-haves (not on any critical path)

9. **Vector tiles** if demographics ever becomes a primary map layer.
   Tippecanoe → pmtiles served from `/data/processed/demographics/`
   would let the frontend render the full US smoothly without ever
   touching the filter/simplify endpoint. Way more work than it's worth
   for beta.
10. **Additional ACS variables.** The current pipeline is hard-coded to
    B03002, B19013, C17002. If a future EJ scope needs e.g. age
    (B01001), language (B16001), or education (B15003), extend
    `VARIABLES` and `RENAMES` in `process_acs.py` — the rest of the
    pipeline is variable-agnostic.
11. **Non-US countries.** Path structure already assumes
    `demographics/{country}/{year}.parquet`, so a future `mx`, `br`,
    etc. would slot in cleanly. Each country would need its own
    fetcher module (cenpy is US-only) under `backend/etl/`.
