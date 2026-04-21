"""ETL: convert raw MR-BRT CSVs from IHME into a uniform Parquet layout.

Raw layout
    data/raw/mr_brt/IHME_GBD_2023_AIR_POLLUTION_..._{POLLUTANT}_RR_{ENDPOINT}_{MEAN|DRAWS}_*.CSV

Raw columns (MEAN):
    exposure, mean, median, lower, upper     — values are log(RR) for binary
                                                outcomes, raw effect size for
                                                continuous outcomes (birth
                                                weight, gestational age shift).

Output
    data/processed/mr_brt/{pollutant}/{endpoint}.parquet

Output columns:
    exposure, log_rr_mean, log_rr_lower, log_rr_upper,
    rr_mean, rr_lower, rr_upper, is_rr, source_file

``is_rr`` is True for binary-outcome endpoints where exp(mean) is a relative
risk the HIA engine can consume. For continuous outcomes the ``rr_*`` columns
are filled with ``exp(...)`` anyway (harmless) but ``is_rr=False`` flags them
so downstream code can skip or treat them differently.

Run with::

    python -m backend.etl.process_mr_brt
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

RAW_ROOT = Path("data/raw/mr_brt")
OUT_ROOT = Path("data/processed/mr_brt")

_POLLUTANT_MAP = {"NO2": "no2", "OZONE": "ozone", "PM": "pm25"}

# Endpoints whose MEAN column is NOT log(RR) — continuous outcomes. These
# still get an ``exp(...)`` column for schema uniformity, but is_rr=False.
_CONTINUOUS_ENDPOINTS = {"birth_weight", "gestational_age_shift"}

_FILENAME_RE = re.compile(
    r"IHME_GBD_\d{4}_AIR_POLLUTION_\d+_\d+_"
    r"(?P<pollutant>[A-Z0-9]+)_RR_(?P<endpoint>[A-Z_]+)_MEAN_",
    re.IGNORECASE,
)


def _parse_filename(name: str) -> tuple[str, str] | None:
    m = _FILENAME_RE.search(name)
    if not m:
        return None
    pollutant_raw = m.group("pollutant").upper()
    endpoint = m.group("endpoint").lower().strip("_")
    pollutant = _POLLUTANT_MAP.get(pollutant_raw)
    if pollutant is None:
        return None
    return pollutant, endpoint


def _process_one(src: Path, out: Path) -> None:
    df = pd.read_csv(src)
    df.columns = [c.lower() for c in df.columns]
    # 2022-vintage files use ``exposure``; 2025-vintage files use ``risk``.
    # Both refer to the concentration axis.
    if "exposure" not in df.columns and "risk" in df.columns:
        df = df.rename(columns={"risk": "exposure"})
    required = {"exposure", "mean", "lower", "upper"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{src.name}: missing columns {missing}")

    df = df.sort_values("exposure").reset_index(drop=True)

    endpoint = out.stem
    is_rr = endpoint not in _CONTINUOUS_ENDPOINTS

    result = pd.DataFrame({
        "exposure": df["exposure"].astype("float64"),
        "log_rr_mean": df["mean"].astype("float64"),
        "log_rr_lower": df["lower"].astype("float64"),
        "log_rr_upper": df["upper"].astype("float64"),
        "rr_mean": np.exp(df["mean"].astype("float64")),
        "rr_lower": np.exp(df["lower"].astype("float64")),
        "rr_upper": np.exp(df["upper"].astype("float64")),
        "is_rr": is_rr,
        "source_file": src.name,
    })

    out.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(out, index=False)
    logger.info(
        "Wrote %s (%d rows, exposure range %.3f–%.1f)",
        out, len(result),
        result["exposure"].min(), result["exposure"].max(),
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if not RAW_ROOT.exists():
        raise SystemExit(f"Raw directory not found: {RAW_ROOT.resolve()}")

    files = sorted(RAW_ROOT.glob("*MEAN*.CSV"))
    if not files:
        raise SystemExit(f"No MEAN CSVs found in {RAW_ROOT.resolve()}")

    processed = 0
    skipped: list[str] = []
    for src in files:
        parsed = _parse_filename(src.name)
        if parsed is None:
            skipped.append(src.name)
            continue
        pollutant, endpoint = parsed
        out = OUT_ROOT / pollutant / f"{endpoint}.parquet"
        _process_one(src, out)
        processed += 1

    logger.info("Done — processed %d file(s).", processed)
    if skipped:
        logger.warning("Skipped %d file(s): %s", len(skipped), skipped)


if __name__ == "__main__":
    main()
