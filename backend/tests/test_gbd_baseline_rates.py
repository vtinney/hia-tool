"""Tests for the GBD baseline rate lookup in baseline_rates.py."""

from __future__ import annotations

import textwrap
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from backend.services import baseline_rates


@pytest.fixture(autouse=True)
def _clear_cache():
    """Reset the GBD cache before each test."""
    baseline_rates._clear_gbd_cache()
    yield
    baseline_rates._clear_gbd_cache()


@pytest.fixture
def gbd_parquet(tmp_path: Path) -> Path:
    """Create a small GBD rates parquet for testing."""
    df = pd.DataFrame({
        "cause": ["ihd", "ihd", "asthma", "stroke"],
        "gbd_location_id": [6, 102, 6, 6],
        "location_name": ["China", "United States of America", "China", "China"],
        "year": pd.array([2019, 2020, 2019, 2019], dtype="int16"),
        "rate": [85.5 / 100_000, 120.3 / 100_000, 0.45 / 100_000, 150.2 / 100_000],
        "age_group": ["all_ages", "all_ages", "under_20", "all_ages"],
        "ne_country_iso3": ["CHN", "USA", "CHN", "CHN"],
        "ne_country_uid": ["CHN", "USA", "CHN", "CHN"],
        "ne_state_uid": [None, None, None, None],
    })
    path = tmp_path / "gbd_rates.parquet"
    df.to_parquet(path, engine="pyarrow", index=False)
    return path


class TestGbdBaselineRate:
    """Test get_gbd_baseline_rate lookups."""

    def test_lookup_by_location_id(self, gbd_parquet: Path):
        with patch.object(baseline_rates, "_GBD_PARQUET_PATH", gbd_parquet):
            rate = baseline_rates.get_gbd_baseline_rate("ihd", 2019, gbd_location_id=6)
        assert rate is not None
        assert pytest.approx(rate, rel=1e-6) == 85.5 / 100_000

    def test_lookup_by_location_name(self, gbd_parquet: Path):
        with patch.object(baseline_rates, "_GBD_PARQUET_PATH", gbd_parquet):
            rate = baseline_rates.get_gbd_baseline_rate(
                "ihd", 2019, location_name="China"
            )
        assert rate is not None
        assert pytest.approx(rate, rel=1e-6) == 85.5 / 100_000

    def test_lookup_by_ne_country_uid(self, gbd_parquet: Path):
        with patch.object(baseline_rates, "_GBD_PARQUET_PATH", gbd_parquet):
            rate = baseline_rates.get_gbd_baseline_rate(
                "ihd", 2020, ne_country_uid="USA"
            )
        assert rate is not None
        assert pytest.approx(rate, rel=1e-6) == 120.3 / 100_000

    def test_not_found_returns_none(self, gbd_parquet: Path):
        with patch.object(baseline_rates, "_GBD_PARQUET_PATH", gbd_parquet):
            rate = baseline_rates.get_gbd_baseline_rate("copd", 2019, gbd_location_id=6)
        assert rate is None

    def test_missing_parquet_returns_none(self, tmp_path: Path):
        fake = tmp_path / "nonexistent.parquet"
        with patch.object(baseline_rates, "_GBD_PARQUET_PATH", fake):
            rate = baseline_rates.get_gbd_baseline_rate("ihd", 2019, gbd_location_id=6)
        assert rate is None
