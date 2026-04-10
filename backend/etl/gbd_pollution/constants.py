"""Constants for the GBD air pollution exposure ETL.

Holds source file paths, year range, output paths, and unit
normalization for the NO2 / ozone / PM2.5 tabular + raster pull.
"""

from __future__ import annotations

from pathlib import Path

# ── Years in scope ───────────────────────────────────────────────
YEAR_MIN = 2015
YEAR_MAX = 2023

# ── Source files (raw) ───────────────────────────────────────────
RAW_POLLUTION_DIR = Path("data/raw/gbd/pollution")

NO2_CSV = RAW_POLLUTION_DIR / "IHME_GBD_2023_AIR_POLLUTION_1990_2023_NO2_Y20251010.csv"
OZONE_CSV = RAW_POLLUTION_DIR / "IHME_GBD_2021_AIR_POLLUTION_1990_2021_OZONE_Y2022M01D31.csv"
PM25_CSV = RAW_POLLUTION_DIR / "IHME_GBD_2023_AIR_POLLUTION_1990_2023_PM_Y20250930.CSV"

# PM2.5 raster filename template — substitute {year}.
PM25_RASTER_TEMPLATE = (
    RAW_POLLUTION_DIR
    / "IHME_GBD_2023_AIR_POLLUTION_1990_2023_PM_{year}_Y2025M02D13.TIF"
)

# ── Source files (boundaries) ────────────────────────────────────
RAW_BOUNDARIES_DIR = Path("data/raw/boundaries")
NE_COUNTRIES_SHP = RAW_BOUNDARIES_DIR / "natural_earth_gee/ne_countries/ne_countries.shp"
NE_STATES_SHP = RAW_BOUNDARIES_DIR / "natural_earth_gee/ne_states/ne_states.shp"
GHS_SMOD_SHP = (
    RAW_BOUNDARIES_DIR
    / "GHS_SMOD/GHS_SMOD_E2020_GLOBE_R2023A_54009_1000_UC_V2_0.shp"
)

# ── Output files (processed) ─────────────────────────────────────
PROCESSED_DIR = Path("data/processed")
PROCESSED_BOUNDARIES_DIR = PROCESSED_DIR / "boundaries"
PROCESSED_POLLUTION_DIR = PROCESSED_DIR / "pollution"
PROCESSED_PM25_DIR = PROCESSED_POLLUTION_DIR / "pm25_gbd2023"

CROSSWALK_CSV = PROCESSED_BOUNDARIES_DIR / "gbd_to_ne.csv"
GHS_SMOD_JOIN_PARQUET = PROCESSED_BOUNDARIES_DIR / "ghs_smod_to_ne.parquet"
POLLUTION_PARQUET = PROCESSED_POLLUTION_DIR / "gbd_pollution.parquet"
RASTER_CATALOG_PARQUET = PROCESSED_PM25_DIR / "catalog.parquet"

# ── Normalization ────────────────────────────────────────────────
# Maps raw unit strings (as they appear in the CSVs) to short codes.
UNIT_MAP: dict[str, str] = {
    "micrograms per cubic meter": "ug_m3",
    "ppb": "ppb",
}

# Pollutant codes used in the normalized parquet.
POLLUTANT_PM25 = "pm25"
POLLUTANT_NO2 = "no2"
POLLUTANT_OZONE = "ozone"

# Release codes stamped onto each row.
RELEASE_GBD_2023 = "gbd_2023"
RELEASE_GBD_2021 = "gbd_2021"

# ── Crosswalk ────────────────────────────────────────────────────
# Rapidfuzz token-set ratio threshold for a fuzzy match to be
# auto-accepted. Below this, the row is flagged for manual review.
FUZZY_AUTO_ACCEPT_THRESHOLD = 98
# Rapidfuzz token-set ratio floor — below this, no suggestion is made.
FUZZY_SUGGEST_FLOOR = 70
