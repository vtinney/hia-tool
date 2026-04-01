"""Tests for backend.services.hia_engine — mirrors the Vitest suite."""

import math

import numpy as np
import pytest

from backend.services.hia_engine import (
    compute_hia,
    log_linear,
    gemm,
    _gemm_hr,
    _beta_se,
    _summarise,
)

# ── Shared fixtures ─────────────────────────────────────────────────

KREWSKI_CRF = {
    "id": "test_krewski",
    "source": "Krewski et al. 2009",
    "endpoint": "All-cause mortality",
    "beta": 0.005827,
    "betaLow": 0.003922,
    "betaHigh": 0.007716,
    "functionalForm": "log-linear",
    "defaultRate": 0.008,
}

GEMM_CRF = {
    "id": "test_gemm_acm",
    "source": "Burnett et al. 2018 (GEMM)",
    "endpoint": "All-cause mortality (non-accidental)",
    "beta": 0.00700,
    "betaLow": 0.00520,
    "betaHigh": 0.00880,
    "functionalForm": "gemm-nlt",
    "defaultRate": 0.008,
}


# ── Test 1: Single-value PM2.5, Krewski CRF ────────────────────────


class TestLogLinearKrewski:
    """Log-linear with Krewski params: baseline=12, control=5, pop=1M."""

    baseline = 12.0
    control = 5.0
    delta_c = baseline - control  # 7
    pop = 1_000_000
    y0 = 0.008
    beta = 0.005827

    expected_paf = 1 - math.exp(-beta * delta_c)
    expected_deaths = expected_paf * y0 * pop  # ≈ 321

    def test_point_estimate(self):
        """Analytic point estimate via log_linear()."""
        betas = np.array([self.beta])
        cases, paf = log_linear(betas, self.delta_c, self.y0, self.pop)

        assert cases[0] == pytest.approx(self.expected_deaths, rel=0.01)
        assert paf[0] == pytest.approx(self.expected_paf, abs=1e-6)

    def test_compute_hia_mean(self):
        """MC mean via compute_hia() within 5% of analytic estimate."""
        result = compute_hia(
            {
                "baselineConcentration": self.baseline,
                "controlConcentration": self.control,
                "baselineIncidence": self.y0,
                "population": self.pop,
                "selectedCRFs": [KREWSKI_CRF],
                "monteCarloIterations": 5000,
            }
        )

        assert len(result["results"]) == 1
        r = result["results"][0]

        rel_error = (
            abs(r["attributableCases"]["mean"] - self.expected_deaths)
            / self.expected_deaths
        )
        assert rel_error < 0.05

        # Verify structure
        assert r["crfId"] == "test_krewski"
        assert r["study"] == "Krewski et al. 2009"
        assert r["endpoint"] == "All-cause mortality"
        for key in ("attributableCases", "attributableFraction", "attributableRate"):
            for stat in ("mean", "lower95", "upper95"):
                assert stat in r[key]

    def test_total_deaths(self):
        """totalDeaths sums mortality endpoints."""
        result = compute_hia(
            {
                "baselineConcentration": self.baseline,
                "controlConcentration": self.control,
                "baselineIncidence": self.y0,
                "population": self.pop,
                "selectedCRFs": [KREWSKI_CRF],
                "monteCarloIterations": 2000,
            }
        )

        td = result["totalDeaths"]
        r_mean = result["results"][0]["attributableCases"]["mean"]
        assert td["mean"] == pytest.approx(r_mean, abs=1)


# ── Test 2: GEMM NCD+LRI ───────────────────────────────────────────


class TestGEMM:
    """GEMM with baseline=50, control=2.4 (TMREL), pop=1M."""

    def test_paf_range(self):
        """PAF should be between 0.10 and 0.30 for C=50."""
        result = compute_hia(
            {
                "baselineConcentration": 50.0,
                "controlConcentration": 2.4,
                "baselineIncidence": 0.008,
                "population": 1_000_000,
                "selectedCRFs": [GEMM_CRF],
                "monteCarloIterations": 3000,
            }
        )

        paf = result["results"][0]["attributableFraction"]["mean"]
        assert 0.10 < paf < 0.30

    def test_gemm_hr_at_zero(self):
        """HR(z=0) should be 1.0."""
        hr = _gemm_hr(np.array([0.007]), z=0.0, mu=20.0, tau=8.0)
        assert hr[0] == pytest.approx(1.0, abs=1e-10)

    def test_gemm_hr_monotonic(self):
        """HR increases with z."""
        theta = np.array([0.007])
        hr10 = _gemm_hr(theta, z=10.0, mu=20.0, tau=8.0)[0]
        hr40 = _gemm_hr(theta, z=40.0, mu=20.0, tau=8.0)[0]
        assert hr10 > 1.0
        assert hr40 > hr10


# ── Test 3: Monte Carlo uncertainty propagation ─────────────────────


class TestMonteCarlo:
    """Sanity checks on MC uncertainty."""

    def test_ci_narrower_than_50pct(self):
        """95% CI width should be less than the mean (i.e., < ±50%)."""
        result = compute_hia(
            {
                "baselineConcentration": 12.0,
                "controlConcentration": 5.0,
                "baselineIncidence": 0.008,
                "population": 1_000_000,
                "selectedCRFs": [KREWSKI_CRF],
                "monteCarloIterations": 5000,
            }
        )

        r = result["results"][0]["attributableCases"]
        ci_width = r["upper95"] - r["lower95"]
        assert ci_width < r["mean"]
        assert r["lower95"] > 0
        assert r["upper95"] > r["lower95"]

    def test_beta_se(self):
        """SE derived from CI bounds matches analytic formula."""
        se = _beta_se(0.003922, 0.007716)
        expected = (0.007716 - 0.003922) / (2 * 1.96)
        assert se == pytest.approx(expected, abs=1e-7)


# ── Test 4: Edge cases ──────────────────────────────────────────────


class TestEdgeCases:
    """Boundary and degenerate inputs."""

    def test_zero_delta(self):
        """deltaC = 0 → ≈ 0 deaths."""
        result = compute_hia(
            {
                "baselineConcentration": 12.0,
                "controlConcentration": 12.0,
                "baselineIncidence": 0.008,
                "population": 1_000_000,
                "selectedCRFs": [KREWSKI_CRF],
                "monteCarloIterations": 500,
            }
        )

        mean = result["results"][0]["attributableCases"]["mean"]
        assert abs(mean) < 5

    def test_very_high_concentration_log_linear(self):
        """C = 200 μg/m³ computes without error (log-linear)."""
        result = compute_hia(
            {
                "baselineConcentration": 200.0,
                "controlConcentration": 5.0,
                "baselineIncidence": 0.008,
                "population": 1_000_000,
                "selectedCRFs": [KREWSKI_CRF],
                "monteCarloIterations": 500,
            }
        )

        r = result["results"][0]["attributableCases"]
        assert r["mean"] > 0
        assert math.isfinite(r["mean"])
        assert math.isfinite(r["upper95"])

    def test_very_high_concentration_gemm(self):
        """C = 200 μg/m³ computes without error (GEMM)."""
        result = compute_hia(
            {
                "baselineConcentration": 200.0,
                "controlConcentration": 2.4,
                "baselineIncidence": 0.008,
                "population": 1_000_000,
                "selectedCRFs": [GEMM_CRF],
                "monteCarloIterations": 500,
            }
        )

        r = result["results"][0]["attributableCases"]
        assert r["mean"] > 0
        assert math.isfinite(r["mean"])

    def test_empty_crf_list(self):
        """No CRFs → empty results, zero total deaths."""
        result = compute_hia(
            {
                "baselineConcentration": 12.0,
                "controlConcentration": 5.0,
                "baselineIncidence": 0.008,
                "population": 1_000_000,
                "selectedCRFs": [],
            }
        )

        assert len(result["results"]) == 0
        assert result["totalDeaths"]["mean"] == 0

    def test_zero_population(self):
        """Population = 0 → 0 cases."""
        result = compute_hia(
            {
                "baselineConcentration": 12.0,
                "controlConcentration": 5.0,
                "baselineIncidence": 0.008,
                "population": 0,
                "selectedCRFs": [KREWSKI_CRF],
                "monteCarloIterations": 100,
            }
        )

        assert result["results"][0]["attributableCases"]["mean"] == 0.0


# ── Cross-engine consistency check ──────────────────────────────────


class TestCrossEngineConsistency:
    """Verify Python engine matches expected JS engine outputs.

    These are deterministic analytic checks (not MC) to ensure
    both engines implement the same formulas.
    """

    def test_log_linear_analytic(self):
        """Analytic log-linear result matches hand calculation."""
        beta = 0.005827
        delta_c = 7.0
        y0 = 0.008
        pop = 1_000_000

        expected_paf = 1 - math.exp(-beta * delta_c)
        expected_cases = expected_paf * y0 * pop

        betas = np.array([beta])
        cases, paf = log_linear(betas, delta_c, y0, pop)

        assert cases[0] == pytest.approx(expected_cases, rel=1e-6)
        assert paf[0] == pytest.approx(expected_paf, rel=1e-6)

    def test_gemm_analytic(self):
        """Analytic GEMM result matches hand calculation."""
        theta = 0.007
        c_base = 50.0
        c_ctrl = 2.4
        tmrel = 2.4
        mu = 20.0
        tau = 8.0

        z_base = max(0, c_base - tmrel)  # 47.6
        z_ctrl = max(0, c_ctrl - tmrel)  # 0.0

        sigmoid_base = 1 / (1 + math.exp(-(z_base - mu) / tau))
        hr_base = math.exp(theta * z_base * sigmoid_base)
        hr_ctrl = 1.0  # z_ctrl = 0

        expected_paf = (hr_base - hr_ctrl) / hr_base

        thetas = np.array([theta])
        cases, paf = gemm(thetas, c_base, c_ctrl, 0.008, 1_000_000)

        assert paf[0] == pytest.approx(expected_paf, rel=1e-6)
