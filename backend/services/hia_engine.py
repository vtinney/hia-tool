"""
HIA Engine — Health Impact Assessment computation module (Python / NumPy).

Mirrors the JavaScript engine at frontend/src/lib/hia-engine.js but uses
NumPy for vectorised Monte Carlo sampling and batch arithmetic.

Supported functional forms
--------------------------
1. **Log-linear** (EPA / HRAPIE)
2. **MR-BRT spline** (GBD 2023) — interpolated from IHME's tabulated
   RR curves under ``data/processed/mr_brt/``; falls back to log-linear
   when the spline file for a given CRF isn't on disk.
3. **GEMM SCHIF** (Burnett et al. 2018)
4. **Fusion-CanCHEC hybrid** (Weichenthal et al. 2022) — interpolated
   from the eSCHIF/Fusion hybrid RR table under
   ``data/processed/fusion/`` (produced by
   ``backend.etl.process_fusion``); falls back to log-linear when the
   table for a given CRF isn't on disk.

For gridded / spatially-resolved analyses the public functions accept
NumPy arrays of concentrations and populations so that every spatial
unit is computed in a single vectorised call.
"""

from __future__ import annotations

import logging
import os
import warnings
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────────────
#  MR-BRT spline lookup
# ────────────────────────────────────────────────────────────────────
#
# Map a CRF ID (as used in ``frontend/src/data/crf-library.json``) to the
# processed MR-BRT spline that backs it. The on-disk layout is produced
# by ``backend.etl.process_mr_brt`` — one parquet per (pollutant, endpoint)
# with columns ``exposure, rr_mean, rr_lower, rr_upper``.
#
# A CRF id missing from this map means no spline is wired for it — the
# engine falls back to log-linear and emits the existing warning.
_CRF_ID_TO_SPLINE = {
    "gbd_pm25_ihd":         ("pm25", "ischemic_heart_disease"),
    "gbd_pm25_stroke":      ("pm25", "stroke"),
    "gbd_pm25_lc":          ("pm25", "lung_cancer"),
    "gbd_pm25_copd":        ("pm25", "copd"),
    "gbd_pm25_lri":         ("pm25", "lower_respiratory_infections"),
    "gbd_pm25_dm2":         ("pm25", "diabetes"),
    "gbd_pm25_dementia":    ("pm25", "dementia"),
    "gbd_ozone_copd_mort":  ("ozone", "copd"),
    "gbd_no2_asthma_child": ("no2", "asthma"),
}

_MR_BRT_ROOT = Path(os.getenv("DATA_ROOT", "./data/processed")) / "mr_brt"
_FUSION_ROOT = Path(os.getenv("DATA_ROOT", "./data/processed")) / "fusion"

# Map a CRF ID to the tabulated Fusion-CanCHEC hybrid under ``_FUSION_ROOT``.
# Only all-cause/non-accidental mortality has published Fusion parameters
# today (Weichenthal et al. 2022 via the Vohra HealthBurden repo); CVD
# and lung-cancer Fusion CRFs continue to fall back to log-linear until
# source data lands.
_CRF_ID_TO_FUSION = {
    "fusion_pm25_acm": ("pm25", "all_cause_mortality"),
}


def _load_tabulated_rr(root: Path, pollutant: str, endpoint: str) -> np.ndarray | None:
    """Read a [exposure, rr_mean] table from ``root/pollutant/endpoint.parquet``.

    Returns ``None`` when the file doesn't exist or lacks the expected
    columns so callers can fall back gracefully.
    """
    path = root / pollutant / f"{endpoint}.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    if "exposure" not in df.columns or "rr_mean" not in df.columns:
        return None
    df = df.sort_values("exposure")
    return np.column_stack(
        [df["exposure"].to_numpy(), df["rr_mean"].to_numpy()]
    ).astype(np.float64)


@lru_cache(maxsize=32)
def _load_spline_table(pollutant: str, endpoint: str) -> np.ndarray | None:
    """Load a processed MR-BRT spline as a ``(N, 2)`` [exposure, RR] array."""
    return _load_tabulated_rr(_MR_BRT_ROOT, pollutant, endpoint)


@lru_cache(maxsize=32)
def _load_fusion_table(pollutant: str, endpoint: str) -> np.ndarray | None:
    """Load a processed Fusion-CanCHEC table as a ``(N, 2)`` [exposure, RR] array."""
    return _load_tabulated_rr(_FUSION_ROOT, pollutant, endpoint)


def _spline_for_crf(crf: dict[str, Any] | None) -> np.ndarray | None:
    """Look up the MR-BRT spline table for a CRF dict, if one is wired.

    Resolution order:
    1. Explicit ``spline_table`` on the CRF (caller-provided override).
    2. CRF ``id`` → entry in ``_CRF_ID_TO_SPLINE``.
    3. CRF ``(pollutant, endpoint)`` tuple — used as a loose match when
       only the pair is known.
    """
    if not crf:
        return None
    existing = crf.get("spline_table")
    if existing is not None:
        return np.asarray(existing, dtype=np.float64)
    pair = _CRF_ID_TO_SPLINE.get(crf.get("id"))
    if pair is None:
        pollutant = (crf.get("pollutant") or "").lower()
        endpoint = (crf.get("endpoint") or "").lower().replace(" ", "_")
        if pollutant and endpoint:
            pair = (pollutant, endpoint)
    if pair is None:
        return None
    return _load_spline_table(*pair)


def _fusion_table_for_crf(crf: dict[str, Any] | None) -> np.ndarray | None:
    """Look up the Fusion-CanCHEC hybrid table for a CRF dict, if one is wired.

    Resolution order mirrors ``_spline_for_crf`` but consults
    ``_CRF_ID_TO_FUSION`` and ``_FUSION_ROOT``.
    """
    if not crf:
        return None
    existing = crf.get("fusion_table")
    if existing is not None:
        return np.asarray(existing, dtype=np.float64)
    pair = _CRF_ID_TO_FUSION.get(crf.get("id"))
    if pair is None:
        pollutant = (crf.get("pollutant") or "").lower()
        endpoint = (crf.get("endpoint") or "").lower().replace(" ", "_")
        if pollutant and endpoint:
            pair = (pollutant, endpoint)
    if pair is None:
        return None
    return _load_fusion_table(*pair)

# ────────────────────────────────────────────────────────────────────
#  Helpers
# ────────────────────────────────────────────────────────────────────


def _beta_se(beta_low: float, beta_high: float) -> float:
    """Derive SE of beta from the 95 % CI bounds.

    Under a normal assumption, CI width = 2 × 1.96 × SE.

    Parameters
    ----------
    beta_low : float
        Lower 2.5th-percentile bound.
    beta_high : float
        Upper 97.5th-percentile bound.

    Returns
    -------
    float
        Estimated standard error of beta.
    """
    return (beta_high - beta_low) / (2 * 1.96)


def _summarise(samples: np.ndarray) -> dict[str, float]:
    """Compute mean and 95 % interval from an array of MC samples.

    Parameters
    ----------
    samples : np.ndarray
        1-D array of Monte Carlo draws.

    Returns
    -------
    dict
        Keys: ``mean``, ``lower95``, ``upper95``.
    """
    return {
        "mean": float(np.mean(samples)),
        "lower95": float(np.percentile(samples, 2.5)),
        "upper95": float(np.percentile(samples, 97.5)),
    }


def _summarise_spatial(samples: np.ndarray) -> list[dict[str, float]]:
    """Summarise MC draws across zones.

    Parameters
    ----------
    samples : np.ndarray
        Shape ``(n_iter, n_zones)`` — Monte Carlo draws per zone.

    Returns
    -------
    list of dict
        One ``{mean, lower95, upper95}`` dict per zone.
    """
    means = np.mean(samples, axis=0)
    lowers = np.percentile(samples, 2.5, axis=0)
    uppers = np.percentile(samples, 97.5, axis=0)
    return [
        {"mean": float(m), "lower95": float(l), "upper95": float(u)}
        for m, l, u in zip(means, lowers, uppers)
    ]


# ────────────────────────────────────────────────────────────────────
#  1. LOG-LINEAR
# ────────────────────────────────────────────────────────────────────


def log_linear(
    beta: np.ndarray,
    delta_c: float | np.ndarray,
    y0: float,
    pop: float | np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Log-linear concentration-response function (vectorised).

    Standard BenMAP / EPA form for chronic-exposure CRFs::

        ΔY = y₀ × Pop × (1 − 1/exp(β × ΔC))

    where ΔC = C_baseline − C_control (positive ⇒ reduction).

    Parameters
    ----------
    beta : np.ndarray
        Array of sampled log-RR values (one per MC iteration).
    delta_c : float or np.ndarray
        Change in concentration.  Scalar for single-area analyses;
        array for gridded analyses (broadcast with *beta*).
    y0 : float
        Baseline incidence rate (per person per year).
    pop : float or np.ndarray
        Exposed population.

    Returns
    -------
    cases : np.ndarray
        Attributable cases per iteration.
    paf : np.ndarray
        Population attributable fraction per iteration.
    """
    rr = np.exp(beta * delta_c)
    paf = 1.0 - 1.0 / rr
    cases = paf * y0 * pop
    return cases, paf


# ────────────────────────────────────────────────────────────────────
#  2. MR-BRT SPLINE
# ────────────────────────────────────────────────────────────────────

_mr_brt_warned = False


def _interpolate_rr(
    table: np.ndarray, c: float | np.ndarray
) -> float | np.ndarray:
    """Linear interpolation of RR from a sorted spline table.

    Parameters
    ----------
    table : np.ndarray
        Shape (N, 2) — columns are [concentration, RR], sorted ascending
        by concentration.
    c : float or np.ndarray
        Concentration(s) to look up.

    Returns
    -------
    float or np.ndarray
        Interpolated RR value(s).
    """
    concs = table[:, 0]
    rrs = table[:, 1]
    return np.interp(c, concs, rrs)


def mr_brt(
    beta: np.ndarray,
    c_base: float | np.ndarray,
    c_ctrl: float | np.ndarray,
    y0: float,
    pop: float | np.ndarray,
    spline_table: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """MR-BRT spline concentration-response function.

    When a spline table is provided::

        PAF = (RR(C_base) − RR(C_ctrl)) / RR(C_base)

    Otherwise falls back to log-linear with a one-time warning.

    Parameters
    ----------
    beta : np.ndarray
        Sampled log-RR values (used for fallback only).
    c_base, c_ctrl : float or np.ndarray
        Baseline and control concentrations.
    y0 : float
        Baseline incidence rate.
    pop : float or np.ndarray
        Exposed population.
    spline_table : np.ndarray or None
        Shape (N, 2) — [concentration, RR].  ``None`` triggers fallback.

    Returns
    -------
    cases, paf : np.ndarray
    """
    global _mr_brt_warned

    if spline_table is not None and len(spline_table) >= 2:
        rr_base = _interpolate_rr(spline_table, c_base)
        rr_ctrl = _interpolate_rr(spline_table, c_ctrl)
        paf = np.where(rr_base > 0, (rr_base - rr_ctrl) / rr_base, 0.0)
        # Broadcast paf to match beta shape for MC consistency
        paf = np.broadcast_to(paf, beta.shape).copy()
        cases = paf * y0 * pop
        return cases, paf

    if not _mr_brt_warned:
        logger.warning(
            "MR-BRT spline data not loaded — using log-linear approximation. "
            "Results will differ from GBD estimates."
        )
        _mr_brt_warned = True

    delta_c = np.asarray(c_base) - np.asarray(c_ctrl)
    return log_linear(beta, delta_c, y0, pop)


# ────────────────────────────────────────────────────────────────────
#  3. GEMM SCHIF
# ────────────────────────────────────────────────────────────────────


def _gemm_hr(
    theta: np.ndarray, z: float | np.ndarray, mu: float, tau: float
) -> np.ndarray:
    """Compute the GEMM hazard ratio.

    ::

        HR(z) = exp(θ × z / (1 + exp(−(z − μ) / τ)))

    Parameters
    ----------
    theta : np.ndarray
        Shape parameter (sampled β values).
    z : float or np.ndarray
        Exposure above TMREL, max(0, C − 2.4).
    mu, tau : float
        Inflection and scale parameters.

    Returns
    -------
    np.ndarray
        Hazard ratio per sample.
    """
    z = np.asarray(z, dtype=np.float64)
    sigmoid = 1.0 / (1.0 + np.exp(-(z - mu) / tau))
    return np.exp(theta * z * sigmoid)


def gemm(
    theta: np.ndarray,
    c_base: float | np.ndarray,
    c_ctrl: float | np.ndarray,
    y0: float,
    pop: float | np.ndarray,
    tmrel: float = 2.4,
    mu: float = 20.0,
    tau: float = 8.0,
) -> tuple[np.ndarray, np.ndarray]:
    """GEMM Shape-Constrained Health Impact Function (Burnett et al. 2018).

    ::

        HR(z) = exp( θ × z / (1 + exp(−(z − μ) / τ)) )
        z     = max(0, C − TMREL)
        PAF   = (HR(z_base) − HR(z_ctrl)) / HR(z_base)

    Parameters
    ----------
    theta : np.ndarray
        Sampled shape parameter values.
    c_base, c_ctrl : float or np.ndarray
        Baseline and control concentrations.
    y0 : float
        Baseline incidence rate.
    pop : float or np.ndarray
        Exposed population.
    tmrel : float
        Theoretical minimum risk exposure level (default 2.4 μg/m³).
    mu : float
        Inflection concentration (default 20).
    tau : float
        Scale parameter (default 8).

    Returns
    -------
    cases, paf : np.ndarray
    """
    z_base = np.maximum(0.0, np.asarray(c_base) - tmrel)
    z_ctrl = np.maximum(0.0, np.asarray(c_ctrl) - tmrel)

    hr_base = _gemm_hr(theta, z_base, mu, tau)
    hr_ctrl = _gemm_hr(theta, z_ctrl, mu, tau)

    paf = np.where(hr_base > 1.0, (hr_base - hr_ctrl) / hr_base, 0.0)
    cases = paf * y0 * pop
    return cases, paf


# ────────────────────────────────────────────────────────────────────
#  4. FUSION-CANCHEC HYBRID
# ────────────────────────────────────────────────────────────────────

_fusion_warned = False


def fusion(
    beta: np.ndarray,
    c_base: float | np.ndarray,
    c_ctrl: float | np.ndarray,
    y0: float,
    pop: float | np.ndarray,
    spline_table: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Fusion-CanCHEC hybrid concentration-response function.

    When a tabulated RR table is provided (output of
    ``backend.etl.process_fusion``, which stitches the eSCHIF CanCHEC
    curve below 9.8 μg/m³ onto the Fusion integral above 9.8 μg/m³)::

        PAF = (RR(C_base) − RR(C_ctrl)) / RR(C_base)

    Otherwise falls back to log-linear with a one-time warning — the
    table is preferred because the hybrid curve is flat near the TMREL
    and steepens substantially above, so a flat β · ΔC approximation
    systematically under- or over-estimates depending on the regime.

    Parameters
    ----------
    beta : np.ndarray
        Sampled log-RR values (used for fallback only).
    c_base, c_ctrl : float or np.ndarray
        Baseline and control concentrations.
    y0 : float
        Baseline incidence rate.
    pop : float or np.ndarray
        Exposed population.
    spline_table : np.ndarray or None
        Shape (N, 2) — [concentration, RR]. ``None`` triggers fallback.

    Returns
    -------
    cases, paf : np.ndarray
    """
    global _fusion_warned

    if spline_table is not None and len(spline_table) >= 2:
        rr_base = _interpolate_rr(spline_table, c_base)
        rr_ctrl = _interpolate_rr(spline_table, c_ctrl)
        paf = np.where(rr_base > 0, (rr_base - rr_ctrl) / rr_base, 0.0)
        # Broadcast paf to match beta shape for MC consistency
        paf = np.broadcast_to(paf, beta.shape).copy()
        cases = paf * y0 * pop
        return cases, paf

    if not _fusion_warned:
        logger.warning(
            "Fusion table not loaded — using log-linear approximation. "
            "Results will differ from Fusion-CanCHEC estimates."
        )
        _fusion_warned = True

    delta_c = np.asarray(c_base) - np.asarray(c_ctrl)
    return log_linear(beta, delta_c, y0, pop)


# ────────────────────────────────────────────────────────────────────
#  Dispatcher
# ────────────────────────────────────────────────────────────────────


def _compute_single_crf(
    form: str,
    beta: np.ndarray,
    c_base: float | np.ndarray,
    c_ctrl: float | np.ndarray,
    y0: float,
    pop: float | np.ndarray,
    crf: dict[str, Any] | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Route a CRF to the correct functional form.

    Parameters
    ----------
    form : str
        One of ``"log-linear"``, ``"mr-brt"``, ``"gemm-nlt"``,
        ``"fusion-hybrid"``.
    beta : np.ndarray
        Sampled betas (shape: ``(n_iter,)``).
    c_base, c_ctrl : float or np.ndarray
        Baseline and control concentrations.
    y0 : float
        Baseline incidence rate.
    pop : float or np.ndarray
        Exposed population.
    crf : dict or None
        Full CRF record — used for spline lookup on MR-BRT / fusion.
    """
    delta_c = np.asarray(c_base) - np.asarray(c_ctrl)

    if form == "log-linear":
        return log_linear(beta, delta_c, y0, pop)
    elif form == "mr-brt":
        spline = _spline_for_crf(crf)
        return mr_brt(beta, c_base, c_ctrl, y0, pop, spline_table=spline)
    elif form == "gemm-nlt":
        return gemm(beta, c_base, c_ctrl, y0, pop)
    elif form == "fusion-hybrid":
        table = _fusion_table_for_crf(crf)
        return fusion(beta, c_base, c_ctrl, y0, pop, spline_table=table)
    else:
        logger.warning(
            'Unknown functional form "%s", falling back to log-linear.', form
        )
        return log_linear(beta, delta_c, y0, pop)


# ────────────────────────────────────────────────────────────────────
#  Main entry point
# ────────────────────────────────────────────────────────────────────

_MORTALITY_KEYWORDS = ("mortality", "death", "deaths")


def compute_hia(config: dict[str, Any]) -> dict[str, Any]:
    """Run a full Health Impact Assessment computation.

    For each selected CRF the engine:

    1. Derives SE from the 95 % CI stored in the CRF record.
    2. Draws ``monte_carlo_iterations`` samples of β ~ N(β̂, SE²)
       in a single vectorised call.
    3. Computes attributable cases, PAF, and rate per 100 000 for
       every draw simultaneously.
    4. Summarises across draws (mean, 2.5th, 97.5th percentiles).

    Parameters
    ----------
    config : dict
        Required keys:

        - ``baselineConcentration`` (float or array): C_baseline.
        - ``controlConcentration`` (float or array): C_control.
        - ``baselineIncidence`` (float): y₀ per person per year.
        - ``population`` (float or array): Exposed population.
        - ``selectedCRFs`` (list[dict]): CRF objects from the library.
          Each must contain ``id``, ``source``, ``endpoint``, ``beta``,
          ``betaLow``, ``betaHigh``, ``functionalForm``.
        - ``monteCarloIterations`` (int, optional): Defaults to 1000.

    Returns
    -------
    dict
        ``results``: list of per-CRF dicts with ``crfId``, ``study``,
        ``endpoint``, ``attributableCases``, ``attributableFraction``,
        ``attributableRate`` (each with ``mean``, ``lower95``, ``upper95``).

        ``totalDeaths``: aggregated mortality estimate with ``mean``,
        ``lower95``, ``upper95``.
    """
    c_base = config["baselineConcentration"]
    c_ctrl = config["controlConcentration"]
    y0_global = config["baselineIncidence"]
    pop = config["population"]
    selected_crfs = config.get("selectedCRFs", [])
    n_iter = config.get("monteCarloIterations", 1000)

    if not selected_crfs:
        return {
            "results": [],
            "totalDeaths": {"mean": 0.0, "lower95": 0.0, "upper95": 0.0},
        }

    rng = np.random.default_rng()
    per_100k = 100_000

    results: list[dict[str, Any]] = []
    warnings: list[str] = []
    _seen_fallback_forms: set[str] = set()

    for crf in selected_crfs:
        se = _beta_se(crf["betaLow"], crf["betaHigh"])
        form = crf.get("functionalForm", "log-linear")
        y0 = crf.get("defaultRate", y0_global) or y0_global

        # Track which forms will use fallback. For MR-BRT, only warn when
        # no spline file is available for this specific CRF.
        if form == "mr-brt":
            spline_missing = _spline_for_crf(crf) is None
            if spline_missing and "mr-brt" not in _seen_fallback_forms:
                _seen_fallback_forms.add("mr-brt")
                warnings.append(
                    "MR-BRT spline data not loaded for some GBD CRFs — "
                    "those are using a log-linear approximation. Results "
                    "may differ from published GBD estimates."
                )
        elif form == "fusion-hybrid":
            fusion_missing = _fusion_table_for_crf(crf) is None
            if fusion_missing and "fusion-hybrid" not in _seen_fallback_forms:
                _seen_fallback_forms.add("fusion-hybrid")
                warnings.append(
                    "Fusion table not loaded for some Fusion CRFs — those are "
                    "using a log-linear approximation. Results may differ from "
                    "published Fusion-CanCHEC estimates."
                )

        # Vectorised MC sampling
        betas = rng.normal(loc=crf["beta"], scale=se, size=n_iter)

        cases, paf = _compute_single_crf(
            form, betas, c_base, c_ctrl, y0, pop, crf=crf,
        )

        # Rate per 100 000
        pop_arr = np.asarray(pop, dtype=np.float64)
        with np.errstate(invalid="ignore", divide="ignore"):
            rate = np.where(pop_arr > 0, (cases / pop_arr) * per_100k, 0.0)

        results.append(
            {
                "crfId": crf["id"],
                "study": crf.get("source", ""),
                "endpoint": crf.get("endpoint", ""),
                "attributableCases": _summarise(cases),
                "attributableFraction": _summarise(paf),
                "attributableRate": _summarise(rate),
            }
        )

    # Total deaths: sum across mortality endpoints
    mortality_results = [
        r
        for r in results
        if any(kw in r["endpoint"].lower() for kw in _MORTALITY_KEYWORDS)
    ]
    total_deaths = {
        "mean": sum(r["attributableCases"]["mean"] for r in mortality_results),
        "lower95": sum(
            r["attributableCases"]["lower95"] for r in mortality_results
        ),
        "upper95": sum(
            r["attributableCases"]["upper95"] for r in mortality_results
        ),
    }

    out: dict[str, Any] = {"results": results, "totalDeaths": total_deaths}
    if warnings:
        out["warnings"] = warnings
    return out
