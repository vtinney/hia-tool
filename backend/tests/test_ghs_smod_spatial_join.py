"""Tests for the GHS SMOD → Natural Earth spatial join."""

from pathlib import Path

import pandas as pd

from backend.etl.gbd_pollution.ghs_join import build_ghs_to_ne_join

FIXTURES = Path(__file__).parent / "fixtures" / "gbd_pollution"


def test_ghs_join_assigns_country_and_state(tmp_path: Path):
    out = tmp_path / "ghs_smod_to_ne.parquet"
    build_ghs_to_ne_join(
        ghs_smod_path=FIXTURES / "tiny_ghs_smod.geojson",
        ne_countries_path=FIXTURES / "tiny_ne_countries.geojson",
        ne_states_path=FIXTURES / "tiny_ne_states.geojson",
        output_parquet=out,
    )
    df = pd.read_parquet(out)
    assert set(df.columns) >= {"ghs_uid", "ne_country_uid", "ne_state_uid"}

    # City 1001 is in China (no state in the fixture)
    row_1001 = df[df["ghs_uid"] == 1001].iloc[0]
    assert row_1001["ne_country_uid"] == "CHN"
    assert pd.isna(row_1001["ne_state_uid"])

    # City 1002 is in Brazil, inside Bahia
    row_1002 = df[df["ghs_uid"] == 1002].iloc[0]
    assert row_1002["ne_country_uid"] == "BRA"
    assert row_1002["ne_state_uid"] == "BRA-1"

    # City 1003 is in Indonesia
    row_1003 = df[df["ghs_uid"] == 1003].iloc[0]
    assert row_1003["ne_country_uid"] == "IDN"
