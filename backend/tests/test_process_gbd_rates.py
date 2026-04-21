"""Tests for backend.etl.process_gbd_rates."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pandas as pd
import pytest

from backend.etl.process_gbd_rates import process_gbd_rates

# ── Fixtures ───────────────────────────────────────────────────────

SAMPLE_IHD_CSV = textwrap.dedent("""\
    population_group_id,population_group_name,measure_id,measure_name,location_id,location_name,sex_id,sex_name,age_id,age_name,cause_id,cause_name,metric_id,metric_name,year,val,upper,lower
    1,All Population,1,Deaths,6,China,3,Both,22,All ages,493,Ischemic heart disease,1,Number,2019,1200000.0,1250000.0,1150000.0
    1,All Population,1,Deaths,6,China,3,Both,22,All ages,493,Ischemic heart disease,3,Rate,2019,85.5,90.0,81.0
    1,All Population,1,Deaths,102,United States of America,3,Both,22,All ages,493,Ischemic heart disease,3,Rate,2020,120.3,125.0,115.0
""")

SAMPLE_ASTHMA_CSV = textwrap.dedent("""\
    population_group_id,population_group_name,measure_id,measure_name,location_id,location_name,sex_id,sex_name,age_id,age_name,cause_id,cause_name,metric_id,metric_name,year,val,upper,lower
    1,All Population,1,Deaths,6,China,3,Both,158,<20 years,515,Asthma,3,Rate,2019,0.45,0.50,0.40
    1,All Population,1,Deaths,6,China,3,Both,158,<20 years,515,Asthma,1,Number,2019,500.0,550.0,450.0
""")

SAMPLE_STROKE_CSV = textwrap.dedent("""\
    population_group_id,population_group_name,measure_id,measure_name,location_id,location_name,sex_id,sex_name,age_id,age_name,cause_id,cause_name,metric_id,metric_name,year,val,upper,lower
    1,All Population,1,Deaths,6,China,3,Both,22,All ages,494,Stroke,3,Rate,2019,150.2,155.0,145.0
""")


@pytest.fixture
def raw_dir(tmp_path: Path) -> Path:
    """Create a temporary raw directory with sample CSVs."""
    rates_dir = tmp_path / "data" / "raw" / "gbd" / "rates"
    rates_dir.mkdir(parents=True)
    (rates_dir / "ihd.csv").write_text(SAMPLE_IHD_CSV)
    (rates_dir / "asthma.csv").write_text(SAMPLE_ASTHMA_CSV)
    (rates_dir / "stroke.csv").write_text(SAMPLE_STROKE_CSV)
    return rates_dir


@pytest.fixture
def output_path(tmp_path: Path) -> Path:
    """Return the output parquet path."""
    out = tmp_path / "data" / "processed" / "incidence"
    out.mkdir(parents=True)
    return out / "gbd_rates.parquet"


@pytest.fixture
def crosswalk_path(tmp_path: Path) -> Path:
    """Create a fake crosswalk CSV."""
    xwalk_dir = tmp_path / "data" / "processed" / "boundaries"
    xwalk_dir.mkdir(parents=True)
    xwalk = xwalk_dir / "gbd_to_ne.csv"
    xwalk.write_text(textwrap.dedent("""\
        gbd_location_id,gbd_name,ihme_loc_id,location_level,ne_country_iso3,ne_country_uid,ne_state_uid,match_method,confidence,notes
        6,China,CHN,3,CHN,CHN,,iso3,100,
        102,United States of America,USA,3,USA,USA,,iso3,100,
    """))
    return xwalk


# ── Tests ──────────────────────────────────────────────────────────


class TestRateFiltering:
    """Only metric_id=3 (Rate) rows should be kept."""

    def test_number_rows_dropped(self, raw_dir: Path, output_path: Path):
        process_gbd_rates(raw_dir=raw_dir, output_path=output_path)
        df = pd.read_parquet(output_path)
        # IHD CSV has 1 Number row and 2 Rate rows; asthma has 1 Rate + 1 Number
        # stroke has 1 Rate. Total rate rows = 2 + 1 + 1 = 4
        assert len(df) == 4


class TestNormalization:
    """Rates should be divided by 100,000 to get per-person-year."""

    def test_ihd_china_rate(self, raw_dir: Path, output_path: Path):
        process_gbd_rates(raw_dir=raw_dir, output_path=output_path)
        df = pd.read_parquet(output_path)
        row = df[(df["cause"] == "ihd") & (df["gbd_location_id"] == 6)]
        assert len(row) == 1
        assert pytest.approx(row.iloc[0]["rate"], rel=1e-6) == 85.5 / 100_000


class TestCauseSlugMapping:
    """Filenames should map to correct cause slugs."""

    def test_slugs_present(self, raw_dir: Path, output_path: Path):
        process_gbd_rates(raw_dir=raw_dir, output_path=output_path)
        df = pd.read_parquet(output_path)
        slugs = set(df["cause"].unique())
        assert slugs == {"ihd", "asthma", "stroke"}


class TestAgeGroupNormalization:
    """Age names should be normalized to standard slugs."""

    def test_all_ages(self, raw_dir: Path, output_path: Path):
        process_gbd_rates(raw_dir=raw_dir, output_path=output_path)
        df = pd.read_parquet(output_path)
        ihd_row = df[df["cause"] == "ihd"].iloc[0]
        assert ihd_row["age_group"] == "all_ages"

    def test_under_20(self, raw_dir: Path, output_path: Path):
        process_gbd_rates(raw_dir=raw_dir, output_path=output_path)
        df = pd.read_parquet(output_path)
        asthma_row = df[df["cause"] == "asthma"].iloc[0]
        assert asthma_row["age_group"] == "under_20"


class TestCrosswalkJoin:
    """NE columns should be populated when crosswalk exists."""

    def test_with_crosswalk(
        self, raw_dir: Path, output_path: Path, crosswalk_path: Path
    ):
        process_gbd_rates(
            raw_dir=raw_dir,
            output_path=output_path,
            crosswalk_path=crosswalk_path,
        )
        df = pd.read_parquet(output_path)
        china = df[(df["cause"] == "ihd") & (df["gbd_location_id"] == 6)]
        assert china.iloc[0]["ne_country_iso3"] == "CHN"
        assert china.iloc[0]["ne_country_uid"] == "CHN"

    def test_without_crosswalk(self, raw_dir: Path, output_path: Path):
        process_gbd_rates(raw_dir=raw_dir, output_path=output_path)
        df = pd.read_parquet(output_path)
        assert df["ne_country_iso3"].isna().all()
        assert df["ne_country_uid"].isna().all()
        assert df["ne_state_uid"].isna().all()


class TestOutputSchema:
    """Output parquet should have the exact expected columns."""

    def test_columns(self, raw_dir: Path, output_path: Path):
        process_gbd_rates(raw_dir=raw_dir, output_path=output_path)
        df = pd.read_parquet(output_path)
        expected = {
            "cause", "gbd_location_id", "location_name", "year",
            "age_group", "measure", "sex",
            "rate", "rate_lower", "rate_upper",
            "ne_country_iso3", "ne_country_uid", "ne_state_uid",
        }
        assert set(df.columns) == expected
