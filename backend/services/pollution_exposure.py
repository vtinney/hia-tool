"""Per-location concentration lookup for the HIA engine wizard.

Loads the processed GBD pollution parquet lazily, plus the GHS SMOD →
NE spatial-join parquet and the PM2.5 raster catalog. Returns
concentration estimates for a country, state, or GHS urban center.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

# ── On-disk locations (overridable in tests) ────────────────────
_POLLUTION_ROOT = Path("data/processed/pollution")
_POLLUTION_PARQUET = _POLLUTION_ROOT / "gbd_pollution.parquet"
_GHS_PARQUET = Path("data/processed/boundaries/ghs_smod_to_ne.parquet")
_RASTER_CATALOG = _POLLUTION_ROOT / "pm25_gbd2023" / "catalog.parquet"

# ── Module-level cache ──────────────────────────────────────────
_pollution_df: pd.DataFrame | None = None
_ghs_df: pd.DataFrame | None = None
_raster_catalog_df: pd.DataFrame | None = None

OZONE_LATEST_AVAILABLE_YEAR = 2021


def _clear_cache() -> None:
    """Reset the in-memory cache. Used by tests."""
    global _pollution_df, _ghs_df, _raster_catalog_df
    _pollution_df = None
    _ghs_df = None
    _raster_catalog_df = None


def _load_pollution() -> pd.DataFrame | None:
    global _pollution_df
    if _pollution_df is not None:
        return _pollution_df
    if not _POLLUTION_PARQUET.exists():
        return None
    _pollution_df = pd.read_parquet(_POLLUTION_PARQUET)
    return _pollution_df


def _load_ghs() -> pd.DataFrame | None:
    global _ghs_df
    if _ghs_df is not None:
        return _ghs_df
    if not _GHS_PARQUET.exists():
        return None
    _ghs_df = pd.read_parquet(_GHS_PARQUET)
    return _ghs_df


def _load_raster_catalog() -> pd.DataFrame | None:
    global _raster_catalog_df
    if _raster_catalog_df is not None:
        return _raster_catalog_df
    if not _RASTER_CATALOG.exists():
        return None
    _raster_catalog_df = pd.read_parquet(_RASTER_CATALOG)
    return _raster_catalog_df


def _adjust_year_for_pollutant(pollutant: str, year: int) -> int:
    """Apply pollutant-specific year fallback rules."""
    if pollutant == "ozone" and year > OZONE_LATEST_AVAILABLE_YEAR:
        return OZONE_LATEST_AVAILABLE_YEAR
    return year


def _lookup_row(
    df: pd.DataFrame,
    pollutant: str,
    year: int,
    *,
    ne_state_uid: str | None,
    ne_country_uid: str | None,
) -> pd.Series | None:
    subset = df[(df["pollutant"] == pollutant) & (df["year"] == year)]
    if subset.empty:
        return None

    if ne_state_uid is not None:
        hit = subset[subset["ne_state_uid"] == ne_state_uid]
        if not hit.empty:
            return hit.iloc[0]

    if ne_country_uid is not None:
        hit = subset[
            (subset["ne_country_uid"] == ne_country_uid)
            & (subset["ne_state_uid"].isna())
        ]
        if not hit.empty:
            return hit.iloc[0]

    return None


def get_default_concentration(
    pollutant: str,
    year: int,
    *,
    ne_country_uid: str | None = None,
    ne_state_uid: str | None = None,
    ghs_uid: int | None = None,
) -> dict | None:
    """Look up a default concentration for a location × year.

    Resolution order:
    1. If ``ne_state_uid`` is given, try state-level first.
    2. Else if ``ghs_uid`` is given, resolve via the GHS → NE spatial
       join and retry with the resolved country/state.
    3. Else if ``ne_country_uid`` is given, try country-level.
    4. Ozone queries past 2021 transparently fall back to 2021 and set
       ``year_used`` in the result.
    5. Return ``None`` if nothing resolves.
    """
    df = _load_pollution()
    if df is None:
        return None

    year_used = _adjust_year_for_pollutant(pollutant, year)

    # GHS resolution: expand into country/state via spatial join parquet.
    if ghs_uid is not None and ne_state_uid is None and ne_country_uid is None:
        ghs = _load_ghs()
        if ghs is None:
            return None
        hit = ghs[ghs["ghs_uid"] == ghs_uid]
        if not hit.empty:
            r = hit.iloc[0]
            ne_country_uid = r["ne_country_uid"] if pd.notna(r["ne_country_uid"]) else None
            ne_state_uid = r["ne_state_uid"] if pd.notna(r["ne_state_uid"]) else None

    row = _lookup_row(
        df, pollutant, year_used,
        ne_state_uid=ne_state_uid,
        ne_country_uid=ne_country_uid,
    )
    if row is None:
        return None

    return {
        "mean": float(row["mean"]),
        "lower": float(row["lower"]),
        "upper": float(row["upper"]),
        "unit": str(row["unit"]),
        "source": f"IHME {str(row['release']).upper().replace('_', ' ')}",
        "year_used": int(year_used),
    }


def get_default_raster_path(pollutant: str, year: int) -> Path | None:
    """Return the filesystem path to the default raster for a pollutant.

    Only PM2.5 is supported in v1. Other pollutants return ``None``.
    """
    if pollutant != "pm25":
        return None
    catalog = _load_raster_catalog()
    if catalog is None or catalog.empty:
        return None
    hit = catalog[catalog["year"] == year]
    if hit.empty:
        return None
    rel = str(hit.iloc[0]["relative_path"])
    abs_path = _POLLUTION_ROOT / rel
    if not abs_path.exists():
        return None
    return abs_path
