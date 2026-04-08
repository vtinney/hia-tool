#!/usr/bin/env python3
"""ETL: Fetch American Community Survey (ACS) 5-year estimates by census tract.

Downloads race/ethnicity (B03002), median household income (B19013), and
poverty status (C17002) for all US census tracts (including Puerto Rico) for
a given ACS 5-year vintage, joins them to TIGER/Line cartographic-boundary
tract geometries, and writes a Parquet file with one row per tract.

Usage
-----
    python -m backend.etl.process_acs \
        --vintage 2022 \
        --output data/processed/demographics/us/2022.parquet

    # Process all vintages 2015-2024 in one run
    python -m backend.etl.process_acs --all

Requires CENSUS_API_KEY in the environment (or .env file).
"""

from __future__ import annotations

import logging

logger = logging.getLogger("process_acs")


# ────────────────────────────────────────────────────────────────────
#  Constants
# ────────────────────────────────────────────────────────────────────

# ACS 5-year vintages we support. End year of the 5-year window.
SUPPORTED_VINTAGES: tuple[int, ...] = (
    2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024,
)

# Tract boundary era. Vintages 2015-2019 use 2010 tract geography;
# vintages 2020+ use 2020 tract geography.
def boundary_year_for_vintage(vintage: int) -> int:
    """Return the tract-boundary decennial year for an ACS vintage."""
    if vintage < 2020:
        return 2010
    return 2020


# Census sentinel values for "not available" / "not applicable"
# (see https://www.census.gov/data/developers/data-sets/acs-5year/data-notes.html)
CENSUS_SENTINELS: frozenset[int] = frozenset({
    -666666666,
    -999999999,
    -888888888,
    -222222222,
    -333333333,
    -555555555,
})

# ACS variable codes (Estimate columns — suffix "E")
ACS_VARIABLES: dict[str, str] = {
    # B03002 — Hispanic or Latino Origin by Race
    "B03002_001E": "total_pop",
    "B03002_003E": "nh_white",
    "B03002_004E": "nh_black",
    "B03002_005E": "nh_aian",
    "B03002_006E": "nh_asian",
    "B03002_007E": "nh_nhpi",
    "B03002_008E": "nh_other_alone",     # "Some other race alone"
    "B03002_009E": "nh_two_or_more",     # "Two or more races"
    "B03002_012E": "hispanic",
    # B19013 — Median household income
    "B19013_001E": "median_hh_income",
    # C17002 — Ratio of income to poverty level
    "C17002_001E": "pop_poverty_universe",
    "C17002_002E": "pop_under_050_pov",
    "C17002_003E": "pop_050_099_pov",
    "C17002_004E": "pop_100_124_pov",
    "C17002_005E": "pop_125_149_pov",
    "C17002_006E": "pop_150_184_pov",
    "C17002_007E": "pop_185_199_pov",
}

# State FIPS codes: 50 states + DC (11) + Puerto Rico (72).
# Excludes territories beyond PR (American Samoa, Guam, etc.) which ACS does
# not fully cover at the tract level.
STATE_FIPS: tuple[str, ...] = (
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
    "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
    "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
    "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
    "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
    "56", "72",
)
