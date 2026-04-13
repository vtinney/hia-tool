"""
HIA Engine — Health Impact Assessment computation module (Python / NumPy).

Mirrors the JavaScript engine at frontend/src/lib/hia-engine.js but uses
NumPy for vectorised Monte Carlo sampling and batch arithmetic.

Supported functional forms
--------------------------
1. **Log-linear** (EPA / HRAPIE)
2. **MR-BRT spline** (GBD 2023) — placeholder with log-linear fallback
3. **GEMM SCHIF** (Burnett et al. 2018)
4. **Fusion hybrid** (Weichenthal et al. 2022) — placeholder with
   trapezoidal integration scaffold

For gridded / spatially-resolved analyses the public functions accept
NumPy arrays of concentrations and populations so that every spatial
unit is computed in a single vectorised call.
"""

from __future__ import annotations

import logging
import warnings
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

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
#  4. FUSION — trapezoidal integration
# ────────────────────────────────────────────────────────────────────

_fusion_warned = False


def _trapezoidal_integrate(
    table: np.ndarray, a: float, b: float
) -> float:
    """Trapezoidal numerical integration of a tabulated function.

    Parameters
    ----------
    table : np.ndarray
        Shape (N, 2) — sorted [x, y] pairs.
    a, b : float
        Integration bounds.

    Returns
    -------
    float
        Approximate ∫_a^b f(x) dx.
    """
    if a >= b or len(table) < 2:
        return 0.0

    xs = table[:, 0]
    ys = table[:, 1]
    x_min, x_max = xs[0], xs[-1]
    lo = max(a, x_min)
    hi = min(b, x_max)
    if lo >= hi:
        return 0.0

    # Build integration points: lo, interior knots in (lo, hi), hi
    mask = (xs > lo) & (xs < hi)
    interior_x = xs[mask]
    interior_y = ys[mask]

    all_x = np.concatenate(([lo], interior_x, [hi]))
    all_y = np.concatenate(
        ([np.interp(lo, xs, ys)], interior_y, [np.interp(hi, xs, ys)])
    )

    return float(np.trapz(all_y, all_x))


def fusion(
    beta: np.ndarray,
    c_base: float | np.ndarray,
    c_ctrl: float | np.ndarray,
    y0: float,
    pop: float | np.ndarray,
    mr_table: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Fusion hybrid concentration-response function.

    Integrates a tabulated marginal risk function MR(c)::

        Excess risk = ∫_{c_ctrl}^{c_base} MR(c) dc
        PAF         = 1 − exp(−excess_risk)

    When no MR table is available, a synthetic table is built from the
    log-linear beta (MR(c) ≈ β for all c).

    Parameters
    ----------
    beta : np.ndarray
        Sampled log-RR values.
    c_base, c_ctrl : float or np.ndarray
        Baseline and control concentrations.
    y0 : float
        Baseline incidence rate.
    pop : float or np.ndarray
        Exposed population.
    mr_table : np.ndarray or None
        Shape (N, 2) — [concentration, marginal_risk].

    Returns
    -------
    cases, paf : np.ndarray
    """
    global _fusion_warned

    c_base_f = float(np.asarray(c_base).flat[0])
    c_ctrl_f = float(np.asarray(c_ctrl).flat[0])

    if mr_table is not None and len(mr_table) >= 2:
        excess_risk = _trapezoidal_integrate(mr_table, c_ctrl_f, c_base_f)
        paf = 1.0 - np.exp(-excess_risk)
        paf_arr = np.full_like(beta, paf)
        cases = paf_arr * y0 * pop
        return cases, paf_arr

    if not _fusion_warned:
        logger.warning(
            "Fusion marginal-risk table not loaded — using log-linear "
            "approximation. Results will differ from Fusion estimates."
        )
        _fusion_warned = True

    # Synthetic MR table: MR(c) ≈ sampled beta for each iteration
    lo = min(c_ctrl_f, c_base_f)
    hi = max(c_ctrl_f, c_base_f)
    n_iter = len(beta)

    # For each MC iteration, excess_risk ≈ beta_i × ΔC
    excess_risk = beta * (hi - lo)
    paf = 1.0 - np.exp(-excess_risk)
    cases = paf * y0 * pop
    return cases, paf


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

    Returns
    -------
    cases, paf : np.ndarray
    """
    delta_c = np.asarray(c_base) - np.asarray(c_ctrl)

    if form == "log-linear":
        return log_linear(beta, delta_c, y0, pop)
    elif form == "mr-brt":
        return mr_brt(beta, c_base, c_ctrl, y0, pop)
    elif form == "gemm-nlt":
        return gemm(beta, c_base, c_ctrl, y0, pop)
    elif form == "fusion-hybrid":
        return fusion(beta, c_base, c_ctrl, y0, pop)
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

        # Track which forms will use fallback
        if form == "mr-brt" and "mr-brt" not in _seen_fallback_forms:
            _seen_fallback_forms.add("mr-brt")
            warnings.append(
                "MR-BRT spline data not loaded — GBD CRFs are using a "
                "log-linear approximation. Results may differ from published "
                "GBD estimates."
            )
        elif form == "fusion-hybrid" and "fusion-hybrid" not in _seen_fallback_forms:
            _seen_fallback_forms.add("fusion-hybrid")
            warnings.append(
                "Fusion marginal-risk table not loaded — Fusion CRFs are "
                "using a log-linear approximation. Results may differ from "
                "published Fusion estimates."
            )

        # Vectorised MC sampling
        betas = rng.normal(loc=crf["beta"], scale=se, size=n_iter)

        cases, paf = _compute_single_crf(form, betas, c_base, c_ctrl, y0, pop)

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
