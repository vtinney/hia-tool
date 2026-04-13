"""Consolidate cached CDC Wonder XML responses into a tidy parquet file.

Since the CDC Wonder API only provides national-level data (county/state
grouping is unavailable via the API), each cached response contains
age-group-level rows for a single (year, ICD group) combination. This
module reads all cached responses, sums deaths and population across
the age groups that belong to each age bucket (all, 25plus, 65plus),
and writes one national-level parquet.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from backend.etl.cdc_wonder.parser import parse_response

logger = logging.getLogger("cdc_wonder.consolidate")

# Map CDC Wonder age-group labels to our bucket membership.
# Each response row has a label like "25-34 years".
_AGE_LABEL_TO_BUCKETS: dict[str, list[str]] = {
    "< 1 year": ["all"],
    "1-4 years": ["all"],
    "5-14 years": ["all"],
    "15-24 years": ["all"],
    "25-34 years": ["all", "25plus"],
    "35-44 years": ["all", "25plus"],
    "45-54 years": ["all", "25plus"],
    "55-64 years": ["all", "25plus"],
    "65-74 years": ["all", "25plus", "65plus"],
    "75-84 years": ["all", "25plus", "65plus"],
    "85+ years": ["all", "25plus", "65plus"],
}


def _iter_cached(raw_root: Path):
    """Yield (year, icd_group, xml_text) tuples from cached responses."""
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
            for xml_path in sorted(year_dir.glob("*.xml")):
                icd_group = xml_path.stem  # e.g. "cvd"
                yield year, icd_group, xml_path.read_text()


def consolidate(
    *,
    raw_root: Path,
    output_parquet: Path,
) -> None:
    """Build the national-level mortality rate parquet from cached XML responses.

    Parameters
    ----------
    raw_root : Path
        Directory containing ``{database}/{year}/{icd_group}.xml`` files.
    output_parquet : Path
        Output parquet destination.
    """
    all_rows: list[dict] = []

    for year, icd_group, text in _iter_cached(raw_root):
        parsed = parse_response(text)
        if parsed.empty:
            logger.warning("empty response for year=%d icd=%s", year, icd_group)
            continue

        # For each age group row, add to the appropriate buckets
        for _, row in parsed.iterrows():
            label = row["age_group"]
            buckets = _AGE_LABEL_TO_BUCKETS.get(label, [])
            for bucket in buckets:
                all_rows.append({
                    "year": year,
                    "icd_group": icd_group,
                    "age_bucket": bucket,
                    "deaths": row["deaths"],
                    "population": row["population"],
                })

    if not all_rows:
        raise RuntimeError(
            f"No cached CDC Wonder XML responses found under {raw_root}. "
            "Run the fetch step first."
        )

    df = pd.DataFrame(all_rows)

    # Sum across age groups within each bucket
    agg = (
        df.groupby(["year", "icd_group", "age_bucket"])
        .agg(deaths=("deaths", "sum"), population=("population", "sum"))
        .reset_index()
    )

    # Compute rate
    rate = agg["deaths"].astype("float64") / agg["population"].replace(0, pd.NA)
    agg["rate_per_person_year"] = rate.fillna(0).astype("float64")

    agg["year"] = agg["year"].astype("int16")
    agg["icd_group"] = agg["icd_group"].astype("category")
    agg["age_bucket"] = agg["age_bucket"].astype("category")

    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    agg.to_parquet(output_parquet, index=False)
    logger.info(
        "wrote %d national rows to %s", len(agg), output_parquet,
    )
