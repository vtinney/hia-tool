#!/usr/bin/env python3
"""ETL: Build the Fusion-CanCHEC hybrid RR tables for PM2.5.

Implements the Weichenthal et al. (2022) Fusion-CanCHEC hybrid risk
model as adapted in Vohra et al.'s HealthBurden reference R script
(https://github.com/karnvohra/HealthBurden). The hybrid stitches
an eSCHIF (CanCHEC) curve below 9.8 μg/m³ onto the Fusion integral
above 9.8 μg/m³, so the HIA engine can look up RR(c) the same way
it already does for MR-BRT.

Input
-----
- ``data/raw/fusion/Fusion_NonAccidental_Parameters.csv``
  1,000 Monte Carlo draws, columns ``[gamma, mu, rho]``.
- ``data/raw/fusion/eSCHIF_CanCHEC_Parameters.csv``
  1,000 Monte Carlo draws, columns ``[int, gamma, delta, theta, alpha, mu, v]``.

Output
------
``data/processed/fusion/pm25/all_cause_mortality.parquet`` with columns

    exposure, log_rr_mean, log_rr_lower, log_rr_upper,
    rr_mean, rr_lower, rr_upper

on a 0–120 μg/m³ grid at 0.1 μg/m³ resolution. Schema mirrors the
MR-BRT output so the engine's existing spline-lookup code can read it.

The two Weichenthal et al. CSVs cover only non-accidental deaths,
which is treated here as the best available proxy for all-cause
mortality. CVD- and LC-specific Fusion parameters aren't published
in the HealthBurden repo; those CRFs continue to fall back to
log-linear until source data lands.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

# Grid of PM2.5 concentrations the engine will interpolate over.
GRID_PM_MAX = 120.0
GRID_STEP = 0.1
FUSION_TMAX = 74.0  # T in the R code — integration cap for Fusion
HYBRID_BREAK = 9.8  # PM threshold where eSCHIF hands off to Fusion
ESCHIF_OFFSET = 2.5  # eSCHIF is defined in z = PM − 2.5 (i.e. from PM 2.5)

RAW_DIR = Path("data/raw/fusion")
OUTPUT_ROOT = Path("data/processed/fusion")


def _load_parameters() -> tuple[np.ndarray, np.ndarray]:
    """Return (fusion_params, eschif_params) as float arrays."""
    fus = pd.read_csv(RAW_DIR / "Fusion_NonAccidental_Parameters.csv")
    esch = pd.read_csv(RAW_DIR / "eSCHIF_CanCHEC_Parameters.csv")
    return fus.to_numpy(dtype=np.float64), esch.to_numpy(dtype=np.float64)


def _fusion_curve(params: np.ndarray, grid_pm: np.ndarray) -> np.ndarray:
    """Evaluate the Fusion log-RR curve at every grid point for every draw.

    Follows the R reference script step-by-step:

    - For ``s < mu_j``: integrand ``G(s) = 1``.
    - For ``mu_j ≤ s ≤ T``: ``G(s) = 1 / (1 + ((1-rho)/rho) · ((s-mu)/(T-mu))^lambda)``
      where ``lambda = (T - mu) / (T · (1 - rho))``.
    - Cumulative integral ``INT(x) = Σ G(s_k) · 0.1`` on the same grid
      (Riemann left-endpoint sum, matching the R code).
    - ``FUSION(x) = gamma · INT(x)`` for ``x < T``, and
      ``gamma · (INT(T) + T · log(x/T) · rho)`` for ``x ≥ T``.
    """
    n_draws = params.shape[0]
    n_pm = grid_pm.size
    # Sub-grid used for integration up to T. Indices align with grid_pm
    # because both start at 0 with step GRID_STEP.
    xx = np.arange(0.0, FUSION_TMAX + GRID_STEP, GRID_STEP)

    out = np.zeros((n_draws, n_pm), dtype=np.float64)
    # Index in xx where s == T (inclusive), used for the x >= T branch.
    idx_T = xx.size - 1

    gamma = params[:, 0]
    mu = params[:, 1]
    rho = params[:, 2]
    lamda = (FUSION_TMAX - mu) / (FUSION_TMAX * (1.0 - rho))

    for j in range(n_draws):
        s = xx
        below = s < mu[j]
        # Safe base for the power: 0 below mu, else (s-mu)/(T-mu)
        base = np.where(below, 0.0, (s - mu[j]) / (FUSION_TMAX - mu[j]))
        G = 1.0 / (1.0 + ((1.0 - rho[j]) / rho[j]) * np.power(base, lamda[j]))
        G[below] = 1.0  # guarantee the "s<mu → G=1" branch
        int_vals = np.cumsum(G) * GRID_STEP  # INT(s_k), length == xx.size

        # x < T: shared index with xx.
        below_T = grid_pm < FUSION_TMAX
        k_below = np.where(below_T)[0]
        out[j, k_below] = gamma[j] * int_vals[k_below]

        # x >= T: use the analytic tail.
        above_T = ~below_T
        k_above = np.where(above_T)[0]
        tail = int_vals[idx_T] + FUSION_TMAX * np.log(
            np.maximum(grid_pm[k_above], FUSION_TMAX) / FUSION_TMAX
        ) * rho[j]
        out[j, k_above] = gamma[j] * tail

    return out


def _eschif_curve(params: np.ndarray, grid_z: np.ndarray) -> np.ndarray:
    """Evaluate the eSCHIF log-RR curve over ``z = PM − 2.5`` for every draw.

    ``eSCHIF(z) = int + gamma · log(z/delta + 1)
                    + theta · log(z/alpha + 1) / (1 + exp(-(z-mu)/v))``
    """
    n_draws = params.shape[0]
    n_z = grid_z.size
    out = np.zeros((n_draws, n_z), dtype=np.float64)

    int_ = params[:, 0]
    gamma_ = params[:, 1]
    delta_ = params[:, 2]
    theta_ = params[:, 3]
    alpha_ = params[:, 4]
    mu_ = params[:, 5]
    v_ = params[:, 6]

    for j in range(n_draws):
        out[j, :] = (
            int_[j]
            + gamma_[j] * np.log(grid_z / delta_[j] + 1.0)
            + theta_[j] * np.log(grid_z / alpha_[j] + 1.0)
            / (1.0 + np.exp(-(grid_z - mu_[j]) / v_[j]))
        )
    return out


def _hybrid_log_rr(
    fusion_params: np.ndarray, eschif_params: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    """Stitch eSCHIF (PM < 9.8) and Fusion (PM ≥ 9.8) into a single log-RR table.

    Returns ``(grid_pm, hybrid_log_rr)`` where ``hybrid_log_rr`` has shape
    ``(n_draws, grid_pm.size)``.
    """
    grid_pm = np.arange(0.0, GRID_PM_MAX + GRID_STEP, GRID_STEP)
    grid_z = np.arange(0.0, 7.5 + GRID_STEP, GRID_STEP)  # PM 2.5..10

    fusion_curve = _fusion_curve(fusion_params, grid_pm)
    eschif_curve = _eschif_curve(eschif_params, grid_z)

    n_draws = fusion_curve.shape[0]
    # CanCHEC extension: 25 leading zeros (PM 0..2.4) + eSCHIF values
    # (PM 2.5..10.0). Final length = 101, matching the R code.
    leading = np.zeros((n_draws, int(round(ESCHIF_OFFSET / GRID_STEP))))
    chec = np.concatenate([leading, eschif_curve], axis=1)

    # Break index corresponds to PM = 9.8. Indices line up across grids
    # because all three step 0.1 from 0.
    break_idx = int(round(HYBRID_BREAK / GRID_STEP))  # 98
    offset_chec = chec[:, break_idx]
    offset_fusion = fusion_curve[:, break_idx]

    hybrid = np.empty_like(fusion_curve)

    # PM < 9.8: use eSCHIF (CHEC). CHEC only extends to PM=10; break_idx
    # (98) is < 101, so no out-of-range reads in this branch.
    hybrid[:, :break_idx] = chec[:, :break_idx]

    # PM >= 9.8: HYB = CHEC(9.8) + (FUSION(x) - FUSION(9.8))
    diff = fusion_curve[:, break_idx:] - offset_fusion[:, np.newaxis]
    hybrid[:, break_idx:] = offset_chec[:, np.newaxis] + diff

    return grid_pm, hybrid


def _summarize(hybrid: np.ndarray) -> dict[str, np.ndarray]:
    """Summarize the (n_draws, n_grid) log-RR matrix → mean + 95% CI arrays."""
    return {
        "log_rr_mean": hybrid.mean(axis=0),
        "log_rr_lower": np.percentile(hybrid, 2.5, axis=0),
        "log_rr_upper": np.percentile(hybrid, 97.5, axis=0),
    }


def main() -> None:
    fusion_params, eschif_params = _load_parameters()
    grid_pm, hybrid = _hybrid_log_rr(fusion_params, eschif_params)
    summary = _summarize(hybrid)

    df = pd.DataFrame({
        "exposure": grid_pm,
        "log_rr_mean": summary["log_rr_mean"],
        "log_rr_lower": summary["log_rr_lower"],
        "log_rr_upper": summary["log_rr_upper"],
        "rr_mean": np.exp(summary["log_rr_mean"]),
        "rr_lower": np.exp(summary["log_rr_lower"]),
        "rr_upper": np.exp(summary["log_rr_upper"]),
        "source_file": "Weichenthal2022_CanCHEC_Fusion_NonAccidental",
    })
    out_path = OUTPUT_ROOT / "pm25" / "all_cause_mortality.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, engine="pyarrow", index=False)
    print(f"wrote {out_path} — {len(df)} rows")
    # Quick sanity check
    for c in (2.4, 5.0, 8.0, 12.0, 20.0, 35.0, 50.0):
        idx = int(round(c / GRID_STEP))
        print(
            f"  PM2.5 = {c:5.1f} ug/m3 -> "
            f"RR = {df.rr_mean.iloc[idx]:.4f} "
            f"({df.rr_lower.iloc[idx]:.4f}-{df.rr_upper.iloc[idx]:.4f})"
        )


if __name__ == "__main__":
    main()
