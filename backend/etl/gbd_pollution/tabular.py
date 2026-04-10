"""Tabular ingest step: parse → unique-locations → join crosswalk → parquet."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from backend.etl.gbd_pollution.parsers import (
    parse_no2_csv,
    parse_ozone_csv,
    parse_pm25_csv,
)

logger = logging.getLogger("gbd_pollution.tabular")


def build_unique_locations(
    *, no2: pd.DataFrame, ozone: pd.DataFrame, pm25: pd.DataFrame,
) -> pd.DataFrame:
    """Collect unique GBD locations across the three pollutant frames.

    Name and ihme_loc_id are back-filled from whichever source has them
    (NO2 has both, ozone has only name, PM2.5 has neither). Location
    level is inferred from the ``ihme_loc_id`` shape:

    - 3-letter code (e.g. ``"CHN"``)   → country, level 3
    - code containing an underscore    → subnational, level 4
    - ``"G"``                          → global, level 0
    - anything else / missing          → level 0 placeholder, gets
      flagged as unmatched in the crosswalk step.
    """
    frames = [f[["gbd_location_id", "ihme_loc_id", "location_name"]]
              for f in (no2, ozone, pm25) if not f.empty]
    if not frames:
        return pd.DataFrame(
            columns=["gbd_location_id", "ihme_loc_id",
                     "location_name", "location_level"]
        )

    stacked = pd.concat(frames, ignore_index=True)

    # For each location_id, take the first non-null name and ihme_loc_id.
    def _first_non_null(series: pd.Series):
        for v in series:
            if isinstance(v, str) and v:
                return v
        return None

    uniq = (
        stacked.groupby("gbd_location_id", as_index=False)
        .agg(
            ihme_loc_id=("ihme_loc_id", _first_non_null),
            location_name=("location_name", _first_non_null),
        )
    )

    def _level(ihme: str | None) -> int:
        if not isinstance(ihme, str):
            return 0
        if ihme == "G":
            return 0
        if "_" in ihme:
            return 4
        if len(ihme) == 3:
            return 3
        return 0

    uniq["location_level"] = uniq["ihme_loc_id"].map(_level).astype("int8")
    return uniq


def ingest_tabular(
    *,
    no2_csv: Path,
    ozone_csv: Path,
    pm25_csv: Path,
    crosswalk_csv: Path,
    output_parquet: Path,
) -> None:
    """Parse → join crosswalk → write the tidy pollution parquet."""
    no2 = parse_no2_csv(no2_csv)
    ozone = parse_ozone_csv(ozone_csv)
    pm25 = parse_pm25_csv(pm25_csv)

    tidy = pd.concat([no2, ozone, pm25], ignore_index=True)
    crosswalk = pd.read_csv(
        crosswalk_csv, dtype={"ne_country_uid": "string",
                              "ne_state_uid": "string",
                              "ne_country_iso3": "string",
                              "ihme_loc_id": "string"},
    )
    crosswalk_trim = crosswalk[[
        "gbd_location_id", "location_level",
        "ne_country_iso3", "ne_country_uid", "ne_state_uid",
    ]]

    merged = tidy.merge(
        crosswalk_trim, on="gbd_location_id", how="left",
        suffixes=("", "_xw"),
    )

    # Back-fill ihme_loc_id on rows where the source CSV didn't have one,
    # using the crosswalk's ne_country_iso3 as the country-level fallback.
    mask = merged["ihme_loc_id"].isna() & merged["ne_country_iso3"].notna()
    merged.loc[mask, "ihme_loc_id"] = merged.loc[mask, "ne_country_iso3"]

    # Location level from the crosswalk (overwrites the parser default).
    merged["location_level"] = merged["location_level"].fillna(0).astype("int8")

    final = merged[[
        "pollutant", "gbd_location_id", "ihme_loc_id", "location_name",
        "location_level", "ne_country_iso3", "ne_country_uid",
        "ne_state_uid", "year", "mean", "lower", "upper", "unit", "release",
    ]].copy()

    final["pollutant"] = final["pollutant"].astype("category")
    final["unit"] = final["unit"].astype("category")
    final["release"] = final["release"].astype("category")

    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    final.to_parquet(output_parquet, index=False)
    logger.info("wrote %d rows to %s", len(final), output_parquet)
