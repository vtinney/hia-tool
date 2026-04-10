"""Tests for the GBD → Natural Earth location crosswalk builder."""

from pathlib import Path

import pandas as pd
import pytest

from backend.etl.gbd_pollution.crosswalk import (
    CrosswalkError,
    build_crosswalk,
)

FIXTURES = Path(__file__).parent / "fixtures" / "gbd_pollution"


def _location_frame() -> pd.DataFrame:
    """Synthetic unique-locations frame as produced by the tabular step."""
    return pd.DataFrame([
        # Country, clean ISO3 match via ihme_loc_id
        {"gbd_location_id": 6, "ihme_loc_id": "CHN",
         "location_name": "China", "location_level": 3},
        {"gbd_location_id": 11, "ihme_loc_id": "IDN",
         "location_name": "Indonesia", "location_level": 3},
        {"gbd_location_id": 135, "ihme_loc_id": "BRA",
         "location_name": "Brazil", "location_level": 3},
        # Subnational, exact name match within parent
        {"gbd_location_id": 4770, "ihme_loc_id": None,
         "location_name": "Bahia", "location_level": 4,
         "parent_iso3": "BRA"},
        # Subnational, fuzzy near-match (accented vs unaccented)
        {"gbd_location_id": 4771, "ihme_loc_id": None,
         "location_name": "Sao Paulo", "location_level": 4,
         "parent_iso3": "BRA"},
        # Country-level aggregate (super-region) — intentionally unmappable
        {"gbd_location_id": 1, "ihme_loc_id": "G",
         "location_name": "Global", "location_level": 0},
    ])


def test_build_crosswalk_iso3_matches(tmp_path: Path):
    out_csv = tmp_path / "gbd_to_ne.csv"
    # This is expected to raise because "Global" (level 0) has no NE match.
    with pytest.raises(CrosswalkError):
        build_crosswalk(
            locations=_location_frame(),
            ne_countries_path=FIXTURES / "tiny_ne_countries.geojson",
            ne_states_path=FIXTURES / "tiny_ne_states.geojson",
            output_csv=out_csv,
        )
    # The CSV should still be written so the engineer can review it.
    assert out_csv.exists()
    df = pd.read_csv(out_csv)

    china = df[df["gbd_location_id"] == 6].iloc[0]
    assert china["match_method"] == "iso3"
    assert china["confidence"] == 100
    assert china["ne_country_uid"] == "CHN"

    bahia = df[df["gbd_location_id"] == 4770].iloc[0]
    assert bahia["match_method"] == "exact_name"
    assert bahia["ne_state_uid"] == "BRA-1"
    assert bahia["ne_country_uid"] == "BRA"

    sao_paulo = df[df["gbd_location_id"] == 4771].iloc[0]
    assert sao_paulo["match_method"] == "fuzzy"
    assert sao_paulo["confidence"] >= 90  # high-similarity fuzzy
    assert sao_paulo["ne_state_uid"] == "BRA-2"

    glob = df[df["gbd_location_id"] == 1].iloc[0]
    assert glob["match_method"] == "unmatched"
    assert pd.isna(glob["ne_country_uid"])


def test_build_crosswalk_clean_when_only_resolvable_rows(tmp_path: Path):
    out_csv = tmp_path / "gbd_to_ne.csv"
    # Drop the Global row so everything else resolves cleanly (the fuzzy
    # match for São Paulo must still score >= the auto-accept threshold,
    # which our fixture guarantees for "Sao Paulo" vs "São Paulo").
    locs = _location_frame().iloc[:-1].copy()
    build_crosswalk(
        locations=locs,
        ne_countries_path=FIXTURES / "tiny_ne_countries.geojson",
        ne_states_path=FIXTURES / "tiny_ne_states.geojson",
        output_csv=out_csv,
    )
    df = pd.read_csv(out_csv)
    assert not (df["match_method"] == "unmatched").any()
    # All confidence values >= auto-accept threshold
    assert (df["confidence"] >= 98).all()
