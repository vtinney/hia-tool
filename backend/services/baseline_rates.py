"""Per-county baseline mortality rate lookup for the HIA engine.

Loads the processed CDC Wonder parquet once (lazily), maps CRF endpoint
strings to (ICD group, age bucket) pairs, and returns y0 values for a
single county or a list of counties.

Returns None to signal "no US-specific rate available" — the caller
then falls back to the CRF's globally-published defaultRate.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

CRF_ENDPOINT_TO_BASELINE: dict[str, tuple[str, str]] = {
    "All-cause mortality": ("all_cause_nonaccidental", "25plus"),
    "All-cause mortality (non-accidental)": ("all_cause_nonaccidental", "25plus"),
    "All-cause mortality (short-term)": ("all_cause", "all"),
    "Cardiovascular mortality": ("cvd", "25plus"),
    "Cardiovascular mortality (short-term)": ("cvd", "all"),
    "Ischemic heart disease": ("ihd", "25plus"),
    "Stroke (cerebrovascular)": ("stroke", "25plus"),
    "Respiratory mortality": ("respiratory", "25plus"),
    "Respiratory mortality (short-term)": ("respiratory", "all"),
    "COPD mortality": ("copd", "25plus"),
    "Lung cancer": ("lung_cancer", "25plus"),
    "Lower respiratory infection": ("lri", "all"),
}

_PARQUET_PATH = Path("data/processed/incidence/us/cdc_wonder_mortality.parquet")

_rate_cache: dict[tuple[str, str, int], pd.Series] = {}
_full_df: pd.DataFrame | None = None


def _clear_cache() -> None:
    """Reset the in-memory cache. Used by tests."""
    global _full_df
    _full_df = None
    _rate_cache.clear()


def _load_frame() -> pd.DataFrame | None:
    global _full_df
    if _full_df is not None:
        return _full_df
    if not _PARQUET_PATH.exists():
        return None
    _full_df = pd.read_parquet(_PARQUET_PATH)
    return _full_df


def _rate_series(icd_group: str, age_bucket: str, year: int) -> pd.Series | None:
    key = (icd_group, age_bucket, year)
    if key in _rate_cache:
        return _rate_cache[key]
    df = _load_frame()
    if df is None:
        return None
    subset = df[
        (df["icd_group"] == icd_group)
        & (df["age_bucket"] == age_bucket)
        & (df["year"] == year)
    ]
    series = pd.Series(
        subset["rate_per_person_year"].to_numpy(dtype=np.float64),
        index=subset["fips"].to_numpy(),
    )
    _rate_cache[key] = series
    return series


def get_baseline_rate(
    crf_endpoint: str,
    year: int,
    fips: str | Iterable[str] | None,
) -> float | np.ndarray | None:
    """Look up the US county baseline mortality rate for a CRF.

    Returns float for scalar fips, ndarray for list of fips, None if
    endpoint is unmapped or fips is None.
    """
    mapping = CRF_ENDPOINT_TO_BASELINE.get(crf_endpoint)
    if mapping is None:
        return None
    if fips is None:
        return None

    icd_group, age_bucket = mapping
    series = _rate_series(icd_group, age_bucket, year)
    if series is None:
        return None

    if isinstance(fips, str):
        return float(series.get(fips, 0.0))

    fips_list = list(fips)
    out = np.zeros(len(fips_list), dtype=np.float64)
    for i, f in enumerate(fips_list):
        if f in series.index:
            out[i] = float(series.loc[f])
    return out
