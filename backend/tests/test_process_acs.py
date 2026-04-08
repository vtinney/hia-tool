"""Tests for backend/etl/process_acs.py."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from backend.etl import process_acs


# ────────────────────────────────────────────────────────────────────
#  clean_sentinels
# ────────────────────────────────────────────────────────────────────


def test_clean_sentinels_replaces_known_sentinels_with_nan():
    df = pd.DataFrame({
        "median_hh_income": [50000, -666666666, 75000, -999999999],
        "total_pop": [1200, 3400, 0, 5000],
    })

    cleaned = process_acs.clean_sentinels(df, ["median_hh_income", "total_pop"])

    assert cleaned["median_hh_income"].tolist()[0] == 50000
    assert math.isnan(cleaned["median_hh_income"].tolist()[1])
    assert cleaned["median_hh_income"].tolist()[2] == 75000
    assert math.isnan(cleaned["median_hh_income"].tolist()[3])
    # total_pop had no sentinels — unchanged
    assert cleaned["total_pop"].tolist() == [1200, 3400, 0, 5000]


def test_clean_sentinels_leaves_untouched_columns_alone():
    df = pd.DataFrame({
        "a": [-666666666, 1, 2],
        "b": [-666666666, 1, 2],
    })
    cleaned = process_acs.clean_sentinels(df, ["a"])
    assert math.isnan(cleaned["a"].iloc[0])
    # "b" was not in the target list — still has the sentinel
    assert cleaned["b"].iloc[0] == -666666666


def test_clean_sentinels_returns_new_dataframe_not_mutating_input():
    df = pd.DataFrame({"x": [-666666666, 100]})
    original = df.copy()
    _ = process_acs.clean_sentinels(df, ["x"])
    pd.testing.assert_frame_equal(df, original)


# ────────────────────────────────────────────────────────────────────
#  add_derived_columns
# ────────────────────────────────────────────────────────────────────


def test_add_derived_columns_computes_percentages():
    df = pd.DataFrame({
        "total_pop": [1000, 2000],
        "nh_white": [600, 1000],
        "nh_black": [200, 500],
        "nh_aian": [0, 0],
        "nh_asian": [50, 200],
        "nh_nhpi": [0, 0],
        "nh_other_alone": [0, 0],
        "nh_two_or_more": [0, 0],
        "hispanic": [150, 300],
        "pop_poverty_universe": [950, 1900],
        "pop_under_050_pov": [50, 100],
        "pop_050_099_pov": [50, 100],
        "pop_100_124_pov": [50, 100],
        "pop_125_149_pov": [0, 0],
        "pop_150_184_pov": [0, 0],
        "pop_185_199_pov": [0, 0],
    })

    result = process_acs.add_derived_columns(df)

    assert result.loc[0, "pct_nh_white"] == pytest.approx(0.6)
    assert result.loc[0, "pct_nh_black"] == pytest.approx(0.2)
    assert result.loc[0, "pct_hispanic"] == pytest.approx(0.15)
    assert result.loc[0, "pct_minority"] == pytest.approx(0.4)
    # nh_other aggregates alone + two_or_more
    assert result.loc[0, "nh_other"] == 0
    # 100 of 950 below poverty
    assert result.loc[0, "pop_below_100_pov"] == 100
    assert result.loc[0, "pct_below_100_pov"] == pytest.approx(100 / 950)
    # 150 of 950 below 2x poverty (under_050 + 050_099 + 100_124)
    assert result.loc[0, "pop_below_200_pov"] == 150
    assert result.loc[0, "pct_below_200_pov"] == pytest.approx(150 / 950)


def test_add_derived_columns_handles_zero_denominator():
    df = pd.DataFrame({
        "total_pop": [0],
        "nh_white": [0], "nh_black": [0], "nh_aian": [0], "nh_asian": [0],
        "nh_nhpi": [0], "nh_other_alone": [0], "nh_two_or_more": [0],
        "hispanic": [0],
        "pop_poverty_universe": [0],
        "pop_under_050_pov": [0], "pop_050_099_pov": [0],
        "pop_100_124_pov": [0], "pop_125_149_pov": [0],
        "pop_150_184_pov": [0], "pop_185_199_pov": [0],
    })
    result = process_acs.add_derived_columns(df)
    # Division by zero → NaN, not an exception
    assert math.isnan(result.loc[0, "pct_nh_white"])
    assert math.isnan(result.loc[0, "pct_below_100_pov"])


# ────────────────────────────────────────────────────────────────────
#  fetch_acs_tables
# ────────────────────────────────────────────────────────────────────


def test_fetch_acs_tables_calls_fetcher_per_state_and_concatenates():
    """The fetcher is called once per state; results are concatenated."""
    calls: list[tuple[int, str]] = []

    def fake_fetch(vintage: int, state_fips: str) -> pd.DataFrame:
        calls.append((vintage, state_fips))
        return pd.DataFrame({
            "state": [state_fips],
            "county": ["001"],
            "tract": ["000100"],
            "B03002_001E": [1000],
            "B03002_003E": [500],
            "B03002_004E": [200],
            "B03002_005E": [0],
            "B03002_006E": [50],
            "B03002_007E": [0],
            "B03002_008E": [0],
            "B03002_009E": [0],
            "B03002_012E": [250],
            "B19013_001E": [55000],
            "C17002_001E": [950],
            "C17002_002E": [50],
            "C17002_003E": [50],
            "C17002_004E": [50],
            "C17002_005E": [0],
            "C17002_006E": [0],
            "C17002_007E": [0],
        })

    result = process_acs.fetch_acs_tables(
        vintage=2022,
        state_fips_list=("06", "36"),  # California, New York
        fetch_fn=fake_fetch,
    )

    assert len(calls) == 2
    assert calls[0] == (2022, "06")
    assert calls[1] == (2022, "36")
    assert len(result) == 2
    assert set(result["state"]) == {"06", "36"}


def test_fetch_acs_tables_retries_on_transient_failure():
    """Transient failures should be retried up to 3 times."""
    attempts = {"count": 0}

    def flaky_fetch(vintage: int, state_fips: str) -> pd.DataFrame:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ConnectionError("transient network error")
        return pd.DataFrame({
            "state": [state_fips], "county": ["001"], "tract": ["000100"],
            "B03002_001E": [100], "B03002_003E": [50], "B03002_004E": [20],
            "B03002_005E": [0], "B03002_006E": [5], "B03002_007E": [0],
            "B03002_008E": [0], "B03002_009E": [0], "B03002_012E": [25],
            "B19013_001E": [50000], "C17002_001E": [95],
            "C17002_002E": [5], "C17002_003E": [5], "C17002_004E": [5],
            "C17002_005E": [0], "C17002_006E": [0], "C17002_007E": [0],
        })

    result = process_acs.fetch_acs_tables(
        vintage=2022, state_fips_list=("06",), fetch_fn=flaky_fetch, retry_sleep=0,
    )
    assert attempts["count"] == 3
    assert len(result) == 1


def test_fetch_acs_tables_aborts_after_exhausting_retries():
    """After 3 failed attempts for one state, the whole call should raise."""
    def always_fail(vintage: int, state_fips: str) -> pd.DataFrame:
        raise ConnectionError("permanent failure")

    with pytest.raises(ConnectionError):
        process_acs.fetch_acs_tables(
            vintage=2022, state_fips_list=("06",), fetch_fn=always_fail, retry_sleep=0,
        )


# ────────────────────────────────────────────────────────────────────
#  fetch_tract_geometry
# ────────────────────────────────────────────────────────────────────


def test_fetch_tract_geometry_reprojects_to_4326_and_builds_geoid():
    """The fetcher must return EPSG:4326 geometries with an 11-char GEOID column."""
    import geopandas as gpd
    from shapely.geometry import Polygon

    # Build a fake TIGER response in a non-4326 CRS
    fake_gdf = gpd.GeoDataFrame(
        {
            "STATEFP": ["06", "06"],
            "COUNTYFP": ["001", "075"],
            "TRACTCE": ["400100", "020100"],
            "geometry": [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
            ],
        },
        crs="EPSG:4269",  # NAD83 — TIGER default
    )

    def fake_pygris_fetch(year: int, cb: bool):
        assert cb is True
        return fake_gdf

    result = process_acs.fetch_tract_geometry(
        vintage=2022, fetch_fn=fake_pygris_fetch,
    )

    assert result.crs == "EPSG:4326"
    assert "geoid" in result.columns
    # 11-char GEOID = state (2) + county (3) + tract (6)
    assert result["geoid"].tolist() == ["06001400100", "06075020100"]
    assert result["state_fips"].tolist() == ["06", "06"]
    assert result["county_fips"].tolist() == ["001", "075"]
    assert result["tract_code"].tolist() == ["400100", "020100"]


# ────────────────────────────────────────────────────────────────────
#  build_demographics_frame
# ────────────────────────────────────────────────────────────────────


def test_build_demographics_frame_joins_renames_and_tags_vintage():
    import geopandas as gpd
    from shapely.geometry import Polygon

    acs_raw = pd.DataFrame({
        "state": ["06", "06"],
        "county": ["001", "075"],
        "tract": ["400100", "020100"],
        "B03002_001E": [1000, 2000],
        "B03002_003E": [600, 1000],
        "B03002_004E": [200, 500],
        "B03002_005E": [0, 0],
        "B03002_006E": [50, 200],
        "B03002_007E": [0, 0],
        "B03002_008E": [0, 0],
        "B03002_009E": [0, 0],
        "B03002_012E": [150, 300],
        "B19013_001E": [55000, -666666666],  # second tract has sentinel
        "C17002_001E": [950, 1900],
        "C17002_002E": [50, 100],
        "C17002_003E": [50, 100],
        "C17002_004E": [50, 100],
        "C17002_005E": [0, 0],
        "C17002_006E": [0, 0],
        "C17002_007E": [0, 0],
    })

    geom = gpd.GeoDataFrame(
        {
            "geoid": ["06001400100", "06075020100"],
            "state_fips": ["06", "06"],
            "county_fips": ["001", "075"],
            "tract_code": ["400100", "020100"],
            "geometry": [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
            ],
        },
        crs="EPSG:4326",
    )

    result = process_acs.build_demographics_frame(
        acs_raw=acs_raw, geometry=geom, vintage=2022,
    )

    # 2 rows, joined successfully
    assert len(result) == 2
    # Friendly column names
    assert "total_pop" in result.columns
    assert "median_hh_income" in result.columns
    # Vintage + boundary tagging
    assert (result["vintage"] == 2022).all()
    assert (result["boundary_year"] == 2020).all()
    # Sentinel became NaN
    assert result.loc[result["geoid"] == "06075020100", "median_hh_income"].isna().iloc[0]
    # Derived columns exist
    assert "pct_nh_white" in result.columns
    assert result.loc[result["geoid"] == "06001400100", "pct_nh_white"].iloc[0] == pytest.approx(0.6)
    # Geometry preserved
    assert "geometry" in result.columns
    # GEOID parts
    assert result["geoid"].tolist() == ["06001400100", "06075020100"]


def test_build_demographics_frame_uses_2010_boundary_year_for_2019_vintage():
    import geopandas as gpd
    from shapely.geometry import Polygon

    acs_raw = pd.DataFrame({
        "state": ["06"], "county": ["001"], "tract": ["400100"],
        "B03002_001E": [100], "B03002_003E": [50], "B03002_004E": [20],
        "B03002_005E": [0], "B03002_006E": [5], "B03002_007E": [0],
        "B03002_008E": [0], "B03002_009E": [0], "B03002_012E": [25],
        "B19013_001E": [50000], "C17002_001E": [95],
        "C17002_002E": [5], "C17002_003E": [5], "C17002_004E": [5],
        "C17002_005E": [0], "C17002_006E": [0], "C17002_007E": [0],
    })
    geom = gpd.GeoDataFrame(
        {"geoid": ["06001400100"], "state_fips": ["06"], "county_fips": ["001"],
         "tract_code": ["400100"],
         "geometry": [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])]},
        crs="EPSG:4326",
    )
    result = process_acs.build_demographics_frame(acs_raw=acs_raw, geometry=geom, vintage=2019)
    assert result["boundary_year"].iloc[0] == 2010


# ────────────────────────────────────────────────────────────────────
#  write_parquet_atomic
# ────────────────────────────────────────────────────────────────────


def test_write_parquet_atomic_writes_wkt_geometry(tmp_path):
    import geopandas as gpd
    from shapely.geometry import Polygon

    gdf = gpd.GeoDataFrame(
        {
            "geoid": ["06001400100"],
            "vintage": [2022],
            "total_pop": [1000],
            "geometry": [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        },
        crs="EPSG:4326",
    )

    out_path = tmp_path / "sub" / "dir" / "2022.parquet"
    process_acs.write_parquet_atomic(gdf, out_path)

    assert out_path.exists()
    roundtrip = pd.read_parquet(out_path)
    assert roundtrip["geometry"].iloc[0].startswith("POLYGON")
    assert roundtrip["geoid"].iloc[0] == "06001400100"
    assert roundtrip["total_pop"].iloc[0] == 1000


def test_write_parquet_atomic_does_not_leave_tmp_file_on_success(tmp_path):
    import geopandas as gpd
    from shapely.geometry import Polygon

    gdf = gpd.GeoDataFrame(
        {"geoid": ["1"], "geometry": [Polygon([(0, 0), (1, 0), (1, 1)])]},
        crs="EPSG:4326",
    )
    out_path = tmp_path / "out.parquet"
    process_acs.write_parquet_atomic(gdf, out_path)

    tmp_files = list(tmp_path.glob("*.tmp"))
    assert tmp_files == []
