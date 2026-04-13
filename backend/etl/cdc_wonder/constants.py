"""Constants for the CDC Wonder ETL.

Holds the databases, years, ICD-10 groups, and age buckets that define
the full matrix of 216 CDC Wonder queries for the HIA baseline rate pull.
"""

from __future__ import annotations

from pathlib import Path

# Database IDs used in the CDC Wonder XML API
DB_UCD_1999_2020 = "D76"  # Underlying Cause of Death, 1999-2020
DB_UCD_2018_2023 = "D158"  # Underlying Cause of Death, 2018-2023, Single Race

# Year -> database routing
YEAR_TO_DB: dict[int, str] = {
    2015: DB_UCD_1999_2020,
    2016: DB_UCD_1999_2020,
    2017: DB_UCD_1999_2020,
    2018: DB_UCD_2018_2023,
    2019: DB_UCD_2018_2023,
    2020: DB_UCD_2018_2023,
    2021: DB_UCD_2018_2023,
    2022: DB_UCD_2018_2023,
    2023: DB_UCD_2018_2023,
}

YEARS: list[int] = sorted(YEAR_TO_DB.keys())

# ICD-10 groups. Each entry maps a group name to a list of ICD-10
# chapter/code specifications accepted by the CDC Wonder API.
# "all_cause_nonaccidental" is synthesized as A00-R99 (excluding
# external causes S00-Y89), matching BenMAP/HRAPIE convention.
ICD_GROUPS: dict[str, list[str]] = {
    "all_cause": ["A00-Y89"],
    "all_cause_nonaccidental": ["A00-R99"],
    "cvd": ["I00-I99"],
    "ihd": ["I20-I25"],
    "stroke": ["I60-I69"],
    "respiratory": ["J00-J99"],
    "copd": ["J40-J44"],
    "lung_cancer": ["C33-C34"],
    "lri": ["J09-J22"],
}

# Age buckets and the 10-year CDC Wonder age-group codes they cover.
# CDC Wonder's 10-year age group codes: "1" = <1yr, "1-4", "5-14",
# "15-24", "25-34", "35-44", "45-54", "55-64", "65-74", "75-84", "85+".
AGE_BUCKETS: dict[str, list[str]] = {
    "all": [
        "1", "1-4", "5-14", "15-24",
        "25-34", "35-44", "45-54", "55-64",
        "65-74", "75-84", "85+",
    ],
    "25plus": [
        "25-34", "35-44", "45-54", "55-64",
        "65-74", "75-84", "85+",
    ],
    "65plus": ["65-74", "75-84", "85+"],
}

# On-disk layout
RAW_DIR = Path("data/raw/cdc_wonder")
PROCESSED_DIR = Path("data/processed/incidence/us")
NATIONAL_PARQUET = PROCESSED_DIR / "cdc_wonder_mortality_national.parquet"

# HTTP
CDC_WONDER_URL = "https://wonder.cdc.gov/controller/datarequest/{db}"
REQUEST_DELAY_SECONDS = 16.0  # CDC Wonder requires >= 15s between API requests
MAX_RETRIES = 5
