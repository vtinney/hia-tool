"""National baseline mortality rate lookup for the HIA engine.

Loads the processed CDC Wonder national parquet once (lazily), maps CRF
endpoint strings to (ICD group, age bucket) pairs, and returns the
national-level y0 rate.

The CDC Wonder API provides national-level data only (county/state
grouping is unavailable via the API). The returned rate is the same
for all US locations in a given year.

Returns None to signal "no US-specific rate available" — the caller
then falls back to the CRF's globally-published defaultRate.
"""

from __future__ import annotations

from pathlib import Path

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

_PARQUET_PATH = Path("data/processed/incidence/us/cdc_wonder_mortality_national.parquet")

_rate_cache: dict[tuple[str, str, int], float] = {}
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


def _lookup_rate(icd_group: str, age_bucket: str, year: int) -> float | None:
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
    if subset.empty:
        return None
    rate = float(subset["rate_per_person_year"].iloc[0])
    _rate_cache[key] = rate
    return rate


def get_baseline_rate(
    crf_endpoint: str,
    year: int,
    country_code: str | None = None,
) -> float | None:
    """Look up the US national baseline mortality rate for a CRF.

    Parameters
    ----------
    crf_endpoint : str
        The endpoint string from the CRF library.
    year : int
        Analysis year. Must be in 2015..2023 for a lookup to succeed.
    country_code : str or None
        Pass "US" for US-specific rates. Non-US or None returns None.

    Returns
    -------
    float
        National baseline rate (deaths per person per year).
    None
        The caller should fall back to the CRF's global defaultRate.
    """
    if country_code != "US":
        return None

    mapping = CRF_ENDPOINT_TO_BASELINE.get(crf_endpoint)
    if mapping is None:
        return None

    icd_group, age_bucket = mapping
    return _lookup_rate(icd_group, age_bucket, year)
