#!/usr/bin/env python3
"""ETL: Process GBD 2023 air pollution exposure data.

Runs four sub-steps in order:
1. Build GBD → Natural Earth location crosswalk (fails loudly on
   unresolved rows — the engineer edits ``gbd_to_ne.csv`` and re-runs).
2. Ingest NO2 / ozone / PM2.5 tabular CSVs into a tidy parquet, joined
   to the crosswalk.
3. Copy PM2.5 GeoTIFFs (2015–2023) to ``data/processed/pollution/
   pm25_gbd2023/`` and write a raster catalog parquet.
4. Build the GHS SMOD → Natural Earth spatial join parquet.

Usage
-----
    python -m backend.etl.process_gbd_pollution

Idempotent: each step skips when its output already exists (use
``--force`` to rebuild).
"""

from __future__ import annotations

import argparse
import logging
import sys

from backend.etl.gbd_pollution.constants import (
    CROSSWALK_CSV,
    GHS_SMOD_JOIN_PARQUET,
    GHS_SMOD_SHP,
    NE_COUNTRIES_SHP,
    NE_STATES_SHP,
    NO2_CSV,
    OZONE_CSV,
    PM25_CSV,
    PM25_RASTER_TEMPLATE,
    POLLUTION_PARQUET,
    PROCESSED_PM25_DIR,
    RASTER_CATALOG_PARQUET,
    YEAR_MAX,
    YEAR_MIN,
)
from backend.etl.gbd_pollution.crosswalk import (
    CrosswalkError,
    build_crosswalk,
)
from backend.etl.gbd_pollution.ghs_join import build_ghs_to_ne_join
from backend.etl.gbd_pollution.parsers import (
    parse_no2_csv,
    parse_ozone_csv,
    parse_pm25_csv,
)
from backend.etl.gbd_pollution.rasters import build_raster_catalog
from backend.etl.gbd_pollution.tabular import (
    build_unique_locations,
    ingest_tabular,
)


def _run_crosswalk(force: bool) -> None:
    logger = logging.getLogger("gbd_pollution.main")
    if CROSSWALK_CSV.exists() and not force:
        logger.info("crosswalk exists, skipping: %s", CROSSWALK_CSV)
        return

    # Unique locations come from parsing all three CSVs first.
    no2 = parse_no2_csv(NO2_CSV)
    ozone = parse_ozone_csv(OZONE_CSV)
    pm25 = parse_pm25_csv(PM25_CSV)
    unique = build_unique_locations(no2=no2, ozone=ozone, pm25=pm25)

    # parent_iso3 is needed for subnational matching. For v1 we infer it
    # from the ``ihme_loc_id`` — rows with a code like "IND_4841" have
    # parent "IND". Non-subnational rows get None.
    def _parent(ihme: str | None) -> str | None:
        if isinstance(ihme, str) and "_" in ihme:
            return ihme.split("_", 1)[0]
        return None

    unique["parent_iso3"] = unique["ihme_loc_id"].map(_parent)

    build_crosswalk(
        locations=unique,
        ne_countries_path=NE_COUNTRIES_SHP,
        ne_states_path=NE_STATES_SHP,
        output_csv=CROSSWALK_CSV,
    )


def _run_tabular(force: bool) -> None:
    logger = logging.getLogger("gbd_pollution.main")
    if POLLUTION_PARQUET.exists() and not force:
        logger.info("pollution parquet exists, skipping: %s", POLLUTION_PARQUET)
        return
    ingest_tabular(
        no2_csv=NO2_CSV, ozone_csv=OZONE_CSV, pm25_csv=PM25_CSV,
        crosswalk_csv=CROSSWALK_CSV, output_parquet=POLLUTION_PARQUET,
    )


def _run_rasters(force: bool) -> None:
    logger = logging.getLogger("gbd_pollution.main")
    if RASTER_CATALOG_PARQUET.exists() and not force:
        logger.info("raster catalog exists, skipping: %s", RASTER_CATALOG_PARQUET)
        return
    years = list(range(YEAR_MIN, YEAR_MAX + 1))
    build_raster_catalog(
        years=years,
        raw_template=PM25_RASTER_TEMPLATE,
        output_dir=PROCESSED_PM25_DIR,
    )


def _run_ghs_join(force: bool) -> None:
    logger = logging.getLogger("gbd_pollution.main")
    if GHS_SMOD_JOIN_PARQUET.exists() and not force:
        logger.info("GHS join parquet exists, skipping: %s", GHS_SMOD_JOIN_PARQUET)
        return
    build_ghs_to_ne_join(
        ghs_smod_path=GHS_SMOD_SHP,
        ne_countries_path=NE_COUNTRIES_SHP,
        ne_states_path=NE_STATES_SHP,
        output_parquet=GHS_SMOD_JOIN_PARQUET,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--force", action="store_true",
                    help="Rebuild every output even if it already exists")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger("gbd_pollution.main")

    try:
        _run_crosswalk(force=args.force)
    except CrosswalkError as exc:
        logger.error(str(exc))
        return 2

    _run_tabular(force=args.force)
    _run_rasters(force=args.force)
    _run_ghs_join(force=args.force)

    logger.info("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
