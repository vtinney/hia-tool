"""Parsers for the three GBD air pollution source CSVs.

Each parser reads one source file and returns a normalized DataFrame
with the common column set used by the downstream tabular ingest step.
The three source files have incompatible schemas — see the plan's
"Real source-data reference" section for details.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from backend.etl.gbd_pollution.constants import (
    POLLUTANT_NO2,
    POLLUTANT_OZONE,
    POLLUTANT_PM25,
    RELEASE_GBD_2021,
    RELEASE_GBD_2023,
    UNIT_MAP,
    YEAR_MAX,
    YEAR_MIN,
)

_NORMALIZED_ORDER = [
    "pollutant", "gbd_location_id", "ihme_loc_id", "location_name",
    "year", "mean", "lower", "upper", "unit", "release",
]


def _filter_years(df: pd.DataFrame, year_col: str) -> pd.DataFrame:
    mask = (df[year_col] >= YEAR_MIN) & (df[year_col] <= YEAR_MAX)
    return df[mask].copy()


def _normalize_unit(raw: str) -> str:
    s = raw.strip() if isinstance(raw, str) else ""
    if s not in UNIT_MAP:
        raise ValueError(f"Unknown unit string from GBD source: {raw!r}")
    return UNIT_MAP[s]


def parse_no2_csv(path: Path) -> pd.DataFrame:
    """Parse the GBD 2023 NO2 source CSV.

    The NO2 file has two ``location_name`` columns (``.x`` and ``.y``
    from an R merge) and a usable ``ihme_loc_id``.
    """
    raw = pd.read_csv(path)
    filtered = _filter_years(raw, "year_id")

    out = pd.DataFrame({
        "pollutant": POLLUTANT_NO2,
        "gbd_location_id": filtered["location_id"].astype("int32"),
        "ihme_loc_id": filtered["ihme_loc_id"].astype("string"),
        "location_name": filtered["location_name.x"].astype("string"),
        "year": filtered["year_id"].astype("int16"),
        "mean": filtered["mean"].astype("float32"),
        "lower": filtered["lower"].astype("float32"),
        "upper": filtered["upper"].astype("float32"),
        "unit": filtered["unit"].map(_normalize_unit),
        "release": RELEASE_GBD_2023,
    })
    return out.reset_index(drop=True)[_NORMALIZED_ORDER]


def parse_ozone_csv(path: Path) -> pd.DataFrame:
    """Parse the GBD 2021 ozone source CSV.

    Ozone has no ``ihme_loc_id`` column — we leave it as NA and back-fill
    during crosswalk resolution. Has a single ``location_name`` column.
    """
    raw = pd.read_csv(path)
    filtered = _filter_years(raw, "year_id")

    out = pd.DataFrame({
        "pollutant": POLLUTANT_OZONE,
        "gbd_location_id": filtered["location_id"].astype("int32"),
        "ihme_loc_id": pd.array([pd.NA] * len(filtered), dtype="string"),
        "location_name": filtered["location_name"].astype("string"),
        "year": filtered["year_id"].astype("int16"),
        "mean": filtered["mean"].astype("float32"),
        "lower": filtered["lower"].astype("float32"),
        "upper": filtered["upper"].astype("float32"),
        "unit": filtered["unit"].map(_normalize_unit),
        "release": RELEASE_GBD_2021,
    })
    return out.reset_index(drop=True)[_NORMALIZED_ORDER]


def parse_pm25_csv(path: Path) -> pd.DataFrame:
    """Parse the GBD 2023 PM2.5 summary CSV.

    The PM2.5 file has NO location_name and NO ihme_loc_id columns —
    only ``location_id``. Both missing fields are back-filled during
    crosswalk resolution. The ``measure_name`` column contains a source
    typo (``"Countinuous"``) which we do not touch.
    """
    raw = pd.read_csv(path)
    filtered = _filter_years(raw, "year_id")

    out = pd.DataFrame({
        "pollutant": POLLUTANT_PM25,
        "gbd_location_id": filtered["location_id"].astype("int32"),
        "ihme_loc_id": pd.array([pd.NA] * len(filtered), dtype="string"),
        "location_name": pd.array([pd.NA] * len(filtered), dtype="string"),
        "year": filtered["year_id"].astype("int16"),
        "mean": filtered["mean"].astype("float32"),
        "lower": filtered["lower"].astype("float32"),
        "upper": filtered["upper"].astype("float32"),
        "unit": filtered["unit"].map(_normalize_unit),
        "release": RELEASE_GBD_2023,
    })
    return out.reset_index(drop=True)[_NORMALIZED_ORDER]
