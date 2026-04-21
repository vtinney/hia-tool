"""Tests for the Fusion-CanCHEC hybrid path through ``hia_engine``.

Verifies that when a Fusion RR table exists on disk (produced by
``backend.etl.process_fusion``), the engine interpolates PAFs from the
tabulated curve — i.e. the PAF varies with concentration as expected,
rather than being a flat log-linear approximation.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services import hia_engine


@pytest.fixture
def fusion_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the engine at a tmp dir and write a tiny fake Fusion parquet.

    The fake curve has RR 1.0 at PM=0 and rises smoothly so the PAF
    sampler has unambiguous expected values:

        RR(0)  = 1.0
        RR(5)  = 1.10
        RR(10) = 1.25
        RR(20) = 1.50
    """
    fake_root = tmp_path / "fusion"
    monkeypatch.setattr(hia_engine, "_FUSION_ROOT", fake_root)
    hia_engine._load_fusion_table.cache_clear()

    table_path = fake_root / "pm25" / "all_cause_mortality.parquet"
    table_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({
        "exposure": [0.0, 5.0, 10.0, 20.0],
        "rr_mean": [1.0, 1.10, 1.25, 1.50],
    })
    df.to_parquet(table_path, engine="pyarrow", index=False)
    return fake_root


FUSION_ACM_CRF = {
    "id": "fusion_pm25_acm",
    "pollutant": "pm25",
    "endpoint": "All-cause mortality",
    "source": "Weichenthal et al. 2022 (CanCHEC Fusion)",
    "beta": 0.00700,
    "betaLow": 0.00520,
    "betaHigh": 0.00880,
    "functionalForm": "fusion-hybrid",
    "defaultRate": 0.008,
}


def test_fusion_uses_tabulated_rr_when_table_is_loaded(fusion_root: Path):
    """PAF should equal (RR(c_base)-RR(c_ctrl))/RR(c_base), not β·ΔC.

    Baseline=10, control=5 → PAF = (1.25-1.10)/1.25 = 0.12.
    The log-linear fallback with β=0.007 and ΔC=5 would give PAF ≈ 0.0344,
    which is obviously different — so if this test passes, the engine is
    genuinely using the concentration-dependent spline.
    """
    result = hia_engine.compute_hia({
        "baselineConcentration": 10.0,
        "controlConcentration": 5.0,
        "baselineIncidence": 0.008,
        "population": 1_000_000,
        "selectedCRFs": [FUSION_ACM_CRF],
        "monteCarloIterations": 1,
    })

    assert len(result["results"]) == 1
    paf = result["results"][0]["attributableFraction"]["mean"]
    # Expected: (1.25 - 1.10) / 1.25 = 0.12
    assert paf == pytest.approx(0.12, rel=1e-3)
    # Log-linear would give roughly 0.0344 — must be clearly different.
    assert paf > 0.10


def test_fusion_paf_is_zero_when_baseline_equals_control(fusion_root: Path):
    result = hia_engine.compute_hia({
        "baselineConcentration": 10.0,
        "controlConcentration": 10.0,
        "baselineIncidence": 0.008,
        "population": 1_000_000,
        "selectedCRFs": [FUSION_ACM_CRF],
        "monteCarloIterations": 1,
    })
    paf = result["results"][0]["attributableFraction"]["mean"]
    assert paf == pytest.approx(0.0, abs=1e-12)


def test_fusion_paf_varies_with_concentration(fusion_root: Path):
    """Two scenarios with the same ΔC=5 but different absolute levels
    should produce different PAFs under the spline — log-linear wouldn't."""
    low = hia_engine.compute_hia({
        "baselineConcentration": 5.0,
        "controlConcentration": 0.0,
        "baselineIncidence": 0.008,
        "population": 1_000_000,
        "selectedCRFs": [FUSION_ACM_CRF],
        "monteCarloIterations": 1,
    })
    high = hia_engine.compute_hia({
        "baselineConcentration": 20.0,
        "controlConcentration": 15.0,
        "baselineIncidence": 0.008,
        "population": 1_000_000,
        "selectedCRFs": [FUSION_ACM_CRF],
        "monteCarloIterations": 1,
    })

    paf_low = low["results"][0]["attributableFraction"]["mean"]
    paf_high = high["results"][0]["attributableFraction"]["mean"]
    # Low: (1.10 - 1.00) / 1.10 = 0.0909
    # High: (1.50 - interp(15)) / 1.50. interp(15) between (10, 1.25) and
    # (20, 1.50) → 1.375. So PAF = (1.50 - 1.375) / 1.50 = 0.0833.
    # These are clearly different values despite ΔC=5 in both scenarios.
    assert paf_low == pytest.approx(0.0909, rel=1e-3)
    assert paf_high == pytest.approx(0.0833, rel=1e-3)
    assert paf_low != pytest.approx(paf_high, rel=1e-2)


def test_fusion_falls_back_to_log_linear_without_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """If no Fusion table is on disk, we warn once and use log-linear.

    Engine should NOT crash — this is the documented fallback behavior.
    """
    monkeypatch.setattr(hia_engine, "_FUSION_ROOT", tmp_path / "no-fusion")
    hia_engine._load_fusion_table.cache_clear()

    result = hia_engine.compute_hia({
        "baselineConcentration": 10.0,
        "controlConcentration": 5.0,
        "baselineIncidence": 0.008,
        "population": 1_000_000,
        "selectedCRFs": [FUSION_ACM_CRF],
        "monteCarloIterations": 1,
    })
    assert len(result["results"]) == 1
    # Warnings surface the fallback so the UI can notify the user
    assert any("fusion" in w.lower() for w in result.get("warnings", []))
