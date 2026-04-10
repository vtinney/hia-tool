"""Build the GBD → Natural Earth location crosswalk.

Three-pass matcher:
1. ISO3 match on ``ihme_loc_id`` for country-level rows.
2. Exact name match for subnational rows, scoped to the parent country.
3. Fuzzy name match (rapidfuzz token-set ratio) for the remainder.

Outputs a CSV with one row per unique GBD location. Rows that fail to
meet the auto-accept threshold are flagged ``unmatched`` or ``fuzzy``
with ``confidence < 98``, and ``build_crosswalk`` raises
``CrosswalkError`` to abort the ETL pipeline. The engineer opens the
CSV, sets the correct columns manually, and re-runs with the clean
crosswalk in place.
"""

from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd
from rapidfuzz import fuzz, process

from backend.etl.gbd_pollution.constants import (
    FUZZY_AUTO_ACCEPT_THRESHOLD,
    FUZZY_SUGGEST_FLOOR,
)

logger = logging.getLogger("gbd_pollution.crosswalk")


CROSSWALK_COLUMNS = [
    "gbd_location_id", "gbd_name", "ihme_loc_id", "location_level",
    "ne_country_iso3", "ne_country_uid", "ne_state_uid",
    "match_method", "confidence", "notes",
]


class CrosswalkError(RuntimeError):
    """Raised when the crosswalk contains rows requiring manual review."""


def _empty_row(location: pd.Series) -> dict:
    return {
        "gbd_location_id": int(location["gbd_location_id"]),
        "gbd_name": location.get("location_name"),
        "ihme_loc_id": location.get("ihme_loc_id"),
        "location_level": int(location["location_level"]),
        "ne_country_iso3": None,
        "ne_country_uid": None,
        "ne_state_uid": None,
        "match_method": "unmatched",
        "confidence": 0,
        "notes": "",
    }


def _try_iso3_match(
    row: dict, location: pd.Series, countries: gpd.GeoDataFrame,
) -> bool:
    ihme = location.get("ihme_loc_id")
    if not isinstance(ihme, str) or not ihme or len(ihme) != 3:
        return False
    hit = countries[countries["ADM0_A3"] == ihme]
    if hit.empty:
        return False
    c = hit.iloc[0]
    row["ne_country_iso3"] = c.get("ISO_A3") if c.get("ISO_A3") != "-99" else ihme
    row["ne_country_uid"] = c["ADM0_A3"]
    row["match_method"] = "iso3"
    row["confidence"] = 100
    return True


def _try_exact_name_state_match(
    row: dict, location: pd.Series, states: gpd.GeoDataFrame,
) -> bool:
    parent = location.get("parent_iso3")
    name = location.get("location_name")
    if not parent or not isinstance(name, str):
        return False
    candidates = states[states["adm0_a3"] == parent]
    if candidates.empty:
        return False
    hit = candidates[candidates["name"] == name]
    if hit.empty:
        return False
    s = hit.iloc[0]
    row["ne_country_uid"] = parent
    row["ne_state_uid"] = s["adm1_code"]
    row["match_method"] = "exact_name"
    row["confidence"] = 100
    return True


def _try_fuzzy_state_match(
    row: dict, location: pd.Series, states: gpd.GeoDataFrame,
) -> bool:
    parent = location.get("parent_iso3")
    name = location.get("location_name")
    if not parent or not isinstance(name, str):
        return False
    candidates = states[states["adm0_a3"] == parent]
    if candidates.empty:
        return False
    choices = candidates["name_en"].fillna(candidates["name"]).tolist()
    best = process.extractOne(name, choices, scorer=fuzz.token_set_ratio)
    if best is None:
        return False
    matched_name, score, idx = best
    if score < FUZZY_SUGGEST_FLOOR:
        return False
    s = candidates.iloc[idx]
    row["ne_country_uid"] = parent
    row["ne_state_uid"] = s["adm1_code"]
    row["match_method"] = "fuzzy"
    row["confidence"] = int(score)
    row["notes"] = f"fuzzy match against {matched_name!r}"
    return True


def build_crosswalk(
    *,
    locations: pd.DataFrame,
    ne_countries_path: Path,
    ne_states_path: Path,
    output_csv: Path,
) -> None:
    """Build the crosswalk and raise if manual review is required."""
    countries = gpd.read_file(ne_countries_path)
    states = gpd.read_file(ne_states_path)

    rows: list[dict] = []
    for _, location in locations.iterrows():
        row = _empty_row(location)
        level = row["location_level"]

        if level == 3:
            _try_iso3_match(row, location, countries)
        elif level >= 4:
            if not _try_exact_name_state_match(row, location, states):
                _try_fuzzy_state_match(row, location, states)
        # level < 3 rows stay "unmatched" (super-regions, global)

        rows.append(row)

    df = pd.DataFrame(rows, columns=CROSSWALK_COLUMNS)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)

    needs_review = df[
        (df["match_method"] == "unmatched")
        | ((df["match_method"] == "fuzzy")
           & (df["confidence"] < FUZZY_AUTO_ACCEPT_THRESHOLD))
    ]
    if not needs_review.empty:
        logger.warning(
            "%d crosswalk rows require manual review (written to %s)",
            len(needs_review), output_csv,
        )
        raise CrosswalkError(
            f"{len(needs_review)} crosswalk rows need manual review. "
            f"Open {output_csv}, correct them (set match_method='manual', "
            f"confidence=100, and fill in ne_* columns), then re-run."
        )
