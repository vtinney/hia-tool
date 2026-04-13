"""Baseline rate lookups for the HIA tool.

Provides lazy-loaded, cached access to baseline incidence/mortality
rates from different data sources (CDC Wonder for US counties, GBD for
global/national rates).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

# ────────────────────────────────────────────────────────────────────
#  GBD baseline rate lookup
# ────────────────────────────────────────────────────────────────────

_GBD_PARQUET_PATH = Path("data/processed/incidence/gbd_rates.parquet")

_gbd_df: pd.DataFrame | None = None


def _clear_gbd_cache() -> None:
    """Reset the GBD in-memory cache. Used by tests."""
    global _gbd_df
    _gbd_df = None


def _load_gbd_frame() -> pd.DataFrame | None:
    global _gbd_df
    if _gbd_df is not None:
        return _gbd_df
    if not _GBD_PARQUET_PATH.exists():
        return None
    _gbd_df = pd.read_parquet(_GBD_PARQUET_PATH)
    return _gbd_df


def get_gbd_baseline_rate(
    cause_slug: str,
    year: int,
    *,
    gbd_location_id: int | None = None,
    location_name: str | None = None,
    ne_country_uid: str | None = None,
) -> float | None:
    """Look up a GBD baseline rate.

    Parameters
    ----------
    cause_slug : str
        App cause slug (e.g., "ihd", "stroke", "asthma").
    year : int
        Analysis year (2015-2023).
    gbd_location_id : int, optional
        GBD integer location ID -- most precise lookup.
    location_name : str, optional
        GBD English location name -- fallback if no ID.
    ne_country_uid : str, optional
        Natural Earth country UID -- fallback if no name.

    Returns
    -------
    float or None
        Per-person-year rate, or None if not found.
    """
    df = _load_gbd_frame()
    if df is None:
        return None

    mask = (df["cause"] == cause_slug) & (df["year"] == year)

    if gbd_location_id is not None:
        mask = mask & (df["gbd_location_id"] == gbd_location_id)
    elif location_name is not None:
        mask = mask & (df["location_name"] == location_name)
    elif ne_country_uid is not None:
        mask = mask & (df["ne_country_uid"] == ne_country_uid)
    else:
        return None

    subset = df[mask]
    if subset.empty:
        return None

    return float(subset.iloc[0]["rate"])
