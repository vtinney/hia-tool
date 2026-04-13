# ACS demographics — next steps

Status snapshot as of 2026-04-09. The `feature/acs-demographics` branch
is shippable: ETL, endpoint, tests, and a verified nationwide 2022 build
are all in place. This file tracks what's left if/when someone picks the
work back up.

## Current state

- **Branch:** `feature/acs-demographics`, 16 commits ahead of `master`.
- **ETL:** `backend/etl/process_acs.py` — CLI with `--vintage` and `--all`.
  Pulls ACS 5-year tables B03002, B19013, C17002 for all 50 states + DC +
  PR via cenpy, joins to TIGER cartographic-boundary tracts via pygris.
- **Data built:** `data/processed/demographics/us/2022.parquet` only
  (85,059 tracts, 65.7 MB, gitignored). Other vintages unbuilt.
- **Endpoint:** `GET /api/data/demographics/{country}/{year}` with optional
  `state` (2-digit FIPS), `county` (3-digit FIPS, requires state), and
  `simplify` (default 0.0001°, 0 disables) query params. Full state
  response ~13 MB; single county ~2 MB.
- **Tests:** 38 passing (29 ETL + health + hia-engine, 9 endpoint).
- **Venv:** `hia-tool/venv` (shared with master worktree) is fully
  provisioned on Python 3.13.

## Immediate next steps (mechanical)

1. **Merge `feature/acs-demographics` → `master`.** The branch is clean
   and has no dependencies on uncommitted master work. Squash vs merge
   commit is a style call — 16 commits tell a clean TDD story, so a
   merge commit preserves history nicely.
2. **Backfill remaining vintages** — run
   `python -m backend.etl.process_acs --all` to produce 2015–2024.
   Estimated ~30 min total (2022 nationwide took ~3 min). Each vintage
   writes ~65 MB; total ~650 MB on disk. Not needed until historical
   comparisons matter.

## Product-shaped next steps (need design input first)

These are blocked on a product decision, not code. Saved in memory under
`project_hia_acs_scope.md`:

> ACS data is opt-in EJ only — does not appear in standard HIA runs.
> When a user picks an EJ-framed analysis format, the initial scope is
> just two fields: `pct_minority` and `pct_below_200_pov`.

3. **Design the EJ analysis format.** Open questions:
   - Is EJ a new wizard step, a variant of Step 3 Population, or a
     post-run layer on the results page?
   - Is it chosen at template-pick time (Home.jsx) or toggled inside
     the wizard?
   - Does it change the computation, or only the presentation?
4. **Wire frontend to endpoint.** Once the format exists: fetch
   `/api/data/demographics/us/{year}?state=XX` (or narrower), render
   `pct_minority` and `pct_below_200_pov` as either a map overlay on
   the Step 1 map or a sidebar panel. Use the existing MapBox GL JS
   setup — the response is already GeoJSON-ready.
5. **Decide on aggregate stats.** If the format wants "X% of exposed
   population lives below 200% poverty" style numbers, add a helper in
   `backend/services/hia_engine.py` that overlays demographics on the
   study area and aggregates. Out of scope until the format is defined.

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
