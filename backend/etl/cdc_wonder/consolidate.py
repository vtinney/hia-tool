"""Consolidate cached CDC Wonder TSVs into tidy parquet files."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from backend.etl.cdc_wonder.parser import parse_response

logger = logging.getLogger("cdc_wonder.consolidate")


def _iter_cached(raw_root: Path):
    """Yield (database, year, icd_group, age_bucket, tsv_text) tuples."""
    if not raw_root.exists():
        return
    for db_dir in sorted(raw_root.iterdir()):
        if not db_dir.is_dir():
            continue
        for year_dir in sorted(db_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            try:
                year = int(year_dir.name)
            except ValueError:
                continue
            for tsv_path in sorted(year_dir.glob("*.tsv")):
                stem = tsv_path.stem
                if "_" not in stem:
                    continue
                icd_group, age_bucket = stem.rsplit("_", 1)
                yield db_dir.name, year, icd_group, age_bucket, tsv_path.read_text()


def consolidate(
    *,
    raw_root: Path,
    county_parquet: Path,
    state_parquet: Path,
    master_fips: list[str],
) -> None:
    """Build the tidy county and state parquets from cached raw TSVs."""
    rows: list[pd.DataFrame] = []
    for database, year, icd_group, age_bucket, text in _iter_cached(raw_root):
        parsed = parse_response(text)
        if parsed.empty:
            continue
        parsed["year"] = year
        parsed["icd_group"] = icd_group
        parsed["age_bucket"] = age_bucket
        rows.append(parsed)

    if not rows:
        raise RuntimeError(
            f"No cached CDC Wonder TSVs found under {raw_root}. "
            "Run the fetch step first."
        )

    df = pd.concat(rows, ignore_index=True)

    combos = df[["year", "icd_group", "age_bucket"]].drop_duplicates()
    master = pd.DataFrame({"fips": master_fips})
    master["key"] = 1
    combos["key"] = 1
    grid = master.merge(combos, on="key").drop(columns="key")

    merged = grid.merge(
        df, on=["fips", "year", "icd_group", "age_bucket"], how="left"
    )
    merged["deaths"] = merged["deaths"].fillna(0).astype("int32")
    merged["population"] = merged["population"].fillna(0).astype("int32")
    merged["state_fips"] = merged["fips"].str[:2]
    merged["year"] = merged["year"].astype("int16")
    merged["icd_group"] = merged["icd_group"].astype("category")
    merged["age_bucket"] = merged["age_bucket"].astype("category")

    rate = merged["deaths"].astype("float64") / merged["population"].replace(0, pd.NA)
    merged["rate_per_person_year"] = rate.fillna(0).astype("float32")

    out = merged[[
        "fips", "state_fips", "year", "icd_group",
        "age_bucket", "deaths", "population", "rate_per_person_year",
    ]]

    county_parquet.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(county_parquet, index=False)
    logger.info("wrote %d county rows to %s", len(out), county_parquet)

    state = (
        out.groupby(
            ["state_fips", "year", "icd_group", "age_bucket"],
            observed=True,
        )
        .agg(deaths=("deaths", "sum"), population=("population", "sum"))
        .reset_index()
    )
    state_rate = state["deaths"].astype("float64") / state["population"].replace(0, pd.NA)
    state["rate_per_person_year"] = state_rate.fillna(0).astype("float32")
    state["deaths"] = state["deaths"].astype("int64")
    state["population"] = state["population"].astype("int64")

    state_parquet.parent.mkdir(parents=True, exist_ok=True)
    state.to_parquet(state_parquet, index=False)
    logger.info("wrote %d state rows to %s", len(state), state_parquet)
