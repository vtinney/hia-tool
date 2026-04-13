"""Tests for the CDC Wonder consolidation step."""

from pathlib import Path

import pandas as pd

from backend.etl.cdc_wonder.consolidate import consolidate


def _write_tsv(path: Path, rows: list[tuple[str, str, tuple[str, str]]]) -> None:
    """Write a minimal CDC Wonder TSV fixture with the given county rows."""
    header = '"Notes"\t"County"\t"County Code"\t"Deaths"\t"Population"\t"Crude Rate"'
    body = "\n".join(
        f'\t"{name}"\t"{fips}"\t"{deaths}"\t"{pop}"\t"0"'
        for fips, name, (deaths, pop) in rows
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{header}\n{body}\n\"---\"\n")


def test_consolidate_writes_long_county_parquet(tmp_path: Path):
    raw_root = tmp_path / "raw"
    out_county = tmp_path / "county.parquet"
    out_state = tmp_path / "state.parquet"

    _write_tsv(
        raw_root / "D158" / "2019" / "cvd_25plus.tsv",
        [("01001", "Autauga, AL", ("10", "40000"))],
    )
    _write_tsv(
        raw_root / "D158" / "2019" / "ihd_25plus.tsv",
        [("01001", "Autauga, AL", ("5", "40000"))],
    )

    consolidate(
        raw_root=raw_root,
        county_parquet=out_county,
        state_parquet=out_state,
        master_fips=["01001", "01003"],
    )

    df = pd.read_parquet(out_county)

    assert set(df.columns) == {
        "fips", "state_fips", "year", "icd_group",
        "age_bucket", "deaths", "population", "rate_per_person_year",
    }
    # 2 counties x 2 icd groups x 1 year x 1 age bucket = 4 rows
    assert len(df) == 4

    filled = df[df["fips"] == "01003"]
    assert (filled["deaths"] == 0).all()
    assert (filled["rate_per_person_year"] == 0).all()

    cvd_01001 = df[(df["fips"] == "01001") & (df["icd_group"] == "cvd")].iloc[0]
    assert cvd_01001["deaths"] == 10
    assert cvd_01001["population"] == 40000
    assert abs(cvd_01001["rate_per_person_year"] - 10 / 40000) < 1e-6


def test_consolidate_state_rollup(tmp_path: Path):
    raw_root = tmp_path / "raw"
    out_county = tmp_path / "county.parquet"
    out_state = tmp_path / "state.parquet"

    _write_tsv(
        raw_root / "D158" / "2019" / "cvd_25plus.tsv",
        [
            ("01001", "Autauga, AL", ("10", "40000")),
            ("01003", "Baldwin, AL", ("20", "60000")),
        ],
    )

    consolidate(
        raw_root=raw_root,
        county_parquet=out_county,
        state_parquet=out_state,
        master_fips=["01001", "01003"],
    )

    state = pd.read_parquet(out_state)
    row = state[
        (state["state_fips"] == "01") & (state["icd_group"] == "cvd")
    ].iloc[0]
    assert row["deaths"] == 30
    assert row["population"] == 100000
    assert abs(row["rate_per_person_year"] - 30 / 100000) < 1e-6
