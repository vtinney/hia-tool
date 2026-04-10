"""Tests for the GBD pollution tabular ingest step."""

from pathlib import Path

import pandas as pd

from backend.etl.gbd_pollution.tabular import (
    build_unique_locations,
    ingest_tabular,
)

FIXTURES = Path(__file__).parent / "fixtures" / "gbd_pollution"


def test_build_unique_locations_backfills_names():
    no2 = pd.DataFrame([
        {"pollutant": "no2", "gbd_location_id": 6, "ihme_loc_id": "CHN",
         "location_name": "China", "year": 2019,
         "mean": 1.0, "lower": 0.0, "upper": 2.0,
         "unit": "ppb", "release": "gbd_2023"},
    ])
    ozone = pd.DataFrame(columns=no2.columns)
    pm25 = pd.DataFrame([
        # PM2.5 has no name — must be back-filled from NO2
        {"pollutant": "pm25", "gbd_location_id": 6, "ihme_loc_id": None,
         "location_name": None, "year": 2019,
         "mean": 10.0, "lower": 5.0, "upper": 15.0,
         "unit": "ug_m3", "release": "gbd_2023"},
    ])

    uniq = build_unique_locations(no2=no2, ozone=ozone, pm25=pm25)
    assert len(uniq) == 1
    row = uniq.iloc[0]
    assert row["gbd_location_id"] == 6
    assert row["ihme_loc_id"] == "CHN"
    assert row["location_name"] == "China"
    assert row["location_level"] == 3  # inferred from 3-letter ihme code


def test_ingest_tabular_writes_joined_parquet(tmp_path: Path, monkeypatch):
    # Pre-write a crosswalk CSV matching our fixture locations
    crosswalk = tmp_path / "gbd_to_ne.csv"
    pd.DataFrame([{
        "gbd_location_id": 6, "gbd_name": "China", "ihme_loc_id": "CHN",
        "location_level": 3, "ne_country_iso3": "CHN",
        "ne_country_uid": "CHN", "ne_state_uid": None,
        "match_method": "iso3", "confidence": 100, "notes": "",
    }, {
        "gbd_location_id": 11, "gbd_name": "Indonesia", "ihme_loc_id": "IDN",
        "location_level": 3, "ne_country_iso3": "IDN",
        "ne_country_uid": "IDN", "ne_state_uid": None,
        "match_method": "iso3", "confidence": 100, "notes": "",
    }]).to_csv(crosswalk, index=False)

    out_parquet = tmp_path / "gbd_pollution.parquet"
    ingest_tabular(
        no2_csv=FIXTURES / "sample_no2.csv",
        ozone_csv=FIXTURES / "sample_ozone.csv",
        pm25_csv=FIXTURES / "sample_pm25.csv",
        crosswalk_csv=crosswalk,
        output_parquet=out_parquet,
    )

    df = pd.read_parquet(out_parquet)
    # Global rows (location_id=1) have no crosswalk match; they still
    # appear with NULL ne_* columns per the spec.
    assert (df["pollutant"].isin(["no2", "ozone", "pm25"])).all()
    # China rows (location_id=6) should have ne_country_uid resolved
    china = df[df["gbd_location_id"] == 6]
    assert (china["ne_country_uid"] == "CHN").all()
    # Global rows present with null NE uid
    glob = df[df["gbd_location_id"] == 1]
    assert glob["ne_country_uid"].isna().all()
    # Year filter held: no rows from 2014
    assert (df["year"] >= 2015).all()
