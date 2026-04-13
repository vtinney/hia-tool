"""Tests for the CDC Wonder consolidation step."""

from pathlib import Path

import pandas as pd

from backend.etl.cdc_wonder.consolidate import consolidate


def _write_xml(path: Path, rows: list[tuple[str, int, int]]) -> None:
    """Write a minimal CDC Wonder XML response fixture."""
    r_elements = "\n".join(
        f'<r><c l="{label}"/><c v="{deaths}"/><c v="{pop}"/><c v="0"/></r>'
        for label, deaths, pop in rows
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<?xml version="1.0"?><page><data-table>{r_elements}</data-table></page>'
    )


def test_consolidate_sums_age_groups_into_buckets(tmp_path: Path):
    raw_root = tmp_path / "raw"
    out = tmp_path / "national.parquet"

    _write_xml(
        raw_root / "D158" / "2019" / "cvd.xml",
        [
            ("25-34 years", 100, 1000000),
            ("35-44 years", 200, 1000000),
            ("65-74 years", 500, 500000),
            ("75-84 years", 400, 300000),
            ("85+ years", 300, 100000),
        ],
    )

    consolidate(raw_root=raw_root, output_parquet=out)

    df = pd.read_parquet(out)
    assert set(df.columns) == {
        "year", "icd_group", "age_bucket",
        "deaths", "population", "rate_per_person_year",
    }

    # 25plus bucket: all 5 age groups (all are 25+)
    row_25 = df[(df["icd_group"] == "cvd") & (df["age_bucket"] == "25plus")].iloc[0]
    assert row_25["deaths"] == 1500
    assert row_25["population"] == 2900000

    # 65plus bucket: 65-74 + 75-84 + 85+
    row_65 = df[(df["icd_group"] == "cvd") & (df["age_bucket"] == "65plus")].iloc[0]
    assert row_65["deaths"] == 1200
    assert row_65["population"] == 900000

    # Rate is deaths / population
    assert abs(row_65["rate_per_person_year"] - 1200 / 900000) < 1e-6


def test_consolidate_multiple_years_and_icd(tmp_path: Path):
    raw_root = tmp_path / "raw"
    out = tmp_path / "national.parquet"

    _write_xml(
        raw_root / "D158" / "2019" / "cvd.xml",
        [("25-34 years", 100, 1000000)],
    )
    _write_xml(
        raw_root / "D158" / "2019" / "respiratory.xml",
        [("25-34 years", 50, 1000000)],
    )
    _write_xml(
        raw_root / "D158" / "2020" / "cvd.xml",
        [("25-34 years", 120, 1000000)],
    )

    consolidate(raw_root=raw_root, output_parquet=out)

    df = pd.read_parquet(out)
    # Each creates "all" and "25plus" buckets = 2 per combo = 6 rows
    assert len(df) == 6
    assert set(df["year"].unique()) == {2019, 2020}
    assert set(df["icd_group"].unique()) == {"cvd", "respiratory"}
