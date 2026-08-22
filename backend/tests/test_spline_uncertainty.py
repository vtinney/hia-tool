"""Spline CRFs (MR-BRT / Fusion) must propagate the RR uncertainty band.

Before this, the tabulated-RR path interpolated only the mean curve, so
every draw produced the identical PAF and the reported 95% CI collapsed
to the point estimate (lower == mean == upper) for every GBD MR-BRT run.
"""
import numpy as np
import pytest

from backend.services.hia_engine import mr_brt, fusion, _compute_single_crf


def _banded_table():
    """[exposure, rr_lower, rr_mean, rr_upper] — linear toy curves."""
    c = np.linspace(0, 100, 101)
    return np.column_stack([c, 1 + 0.005 * c, 1 + 0.010 * c, 1 + 0.015 * c])


def _mean_only_table():
    c = np.linspace(0, 100, 101)
    return np.column_stack([c, 1 + 0.010 * c])


ANALYTICAL_Z = np.array([-1.96, 0.0, 1.96])


class TestBandedSpline:
    def test_analytical_z_spreads_the_ci(self):
        betas = np.array([0.004, 0.006, 0.008])  # only used for shape
        cases, paf = mr_brt(
            betas, 30.0, 5.0, 0.008, 1_000_000,
            spline_table=_banded_table(), z=ANALYTICAL_Z,
        )
        assert paf[0] < paf[1] < paf[2]
        assert cases[0] < cases[1] < cases[2]

    def test_mid_position_matches_mean_curve(self):
        betas = np.array([0.004, 0.006, 0.008])
        _, paf = mr_brt(
            betas, 30.0, 5.0, 0.008, 1_000_000,
            spline_table=_banded_table(), z=ANALYTICAL_Z,
        )
        rr_base, rr_ctrl = 1 + 0.010 * 30, 1 + 0.010 * 5
        assert paf[1] == pytest.approx((rr_base - rr_ctrl) / rr_base, rel=1e-9)

    def test_mc_draws_have_spread(self):
        rng = np.random.default_rng(42)
        betas = rng.normal(0.006, 0.001, size=500)
        z = (betas - 0.006) / 0.001
        cases, paf = mr_brt(
            betas, 30.0, 5.0, 0.008, 1_000_000,
            spline_table=_banded_table(), z=z,
        )
        assert np.std(paf) > 0
        assert np.std(cases) > 0

    def test_mean_only_table_still_degenerate(self):
        betas = np.array([0.004, 0.006, 0.008])
        _, paf = mr_brt(
            betas, 30.0, 5.0, 0.008, 1_000_000,
            spline_table=_mean_only_table(), z=ANALYTICAL_Z,
        )
        assert paf[0] == paf[1] == paf[2]

    def test_fusion_banded_path(self):
        betas = np.array([0.004, 0.006, 0.008])
        _, paf = fusion(
            betas, 30.0, 5.0, 0.008, 1_000_000,
            spline_table=_banded_table(), z=ANALYTICAL_Z,
        )
        assert paf[0] < paf[1] < paf[2]

    def test_dispatcher_passes_z(self):
        betas = np.array([0.004, 0.006, 0.008])
        crf = {"id": "x", "spline_table": _banded_table()}
        _, paf = _compute_single_crf(
            "mr-brt", betas, 30.0, 5.0, 0.008, 1_000_000, crf=crf, z=ANALYTICAL_Z,
        )
        assert paf[0] < paf[2]
