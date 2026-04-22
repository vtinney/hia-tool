"""Cause-based rollups and mortality total splits.

The HIA engine emits one result per CRF. For the Results page we need:
- Per-cause totals (sum across CRFs tagged with the same cause)
- A cause-specific mortality total (excluding all-cause to avoid double-counting)
- A separate all-cause mortality total (only when an all-cause CRF was selected)
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any

CAUSE_LABELS = {
    "all_cause": "All-cause mortality",
    "ihd": "Ischemic heart disease",
    "stroke": "Stroke",
    "lung_cancer": "Lung cancer",
    "copd": "COPD",
    "lri": "Lower respiratory infection",
    "diabetes": "Type 2 diabetes",
    "dementia": "Dementia",
    "asthma": "Asthma incidence",
    "asthma_ed": "Asthma ED visits",
    "respiratory_mortality": "Respiratory mortality",
    "respiratory_hosp": "Respiratory hospitalization",
    "cardiovascular": "Cardiovascular mortality",
    "cardiovascular_hosp": "Cardiovascular hospitalization",
    "cardiac_hosp": "Cardiac hospitalization",
    "birth_weight": "Low birth weight",
    "gestational_age": "Preterm birth",
}


def _zero_ci() -> dict[str, float]:
    return {"mean": 0.0, "lower95": 0.0, "upper95": 0.0}


def _sum_ci(a: dict[str, float], b: dict[str, float]) -> dict[str, float]:
    return {
        "mean": a["mean"] + b["mean"],
        "lower95": a["lower95"] + b["lower95"],
        "upper95": a["upper95"] + b["upper95"],
    }


def build_cause_rollups(
    crf_results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Group CRF results by cause; sum cases, population-weight rates.

    Each ``crf_results`` item must carry private ``_cause`` and
    ``_endpointType`` keys added by the compute router.
    """
    by_cause: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in crf_results:
        by_cause[r["_cause"]].append(r)

    rollups: list[dict[str, Any]] = []
    for cause, results in by_cause.items():
        cases = _zero_ci()
        rate = _zero_ci()
        for r in results:
            cases = _sum_ci(cases, r["attributableCases"])
            # Simple mean across CRFs in the same cause — not
            # population-weighted because rates already are per-100k.
            rate["mean"] += r["attributableRate"]["mean"]
            rate["lower95"] += r["attributableRate"]["lower95"]
            rate["upper95"] += r["attributableRate"]["upper95"]
        rollups.append({
            "cause": cause,
            "endpointLabel": CAUSE_LABELS.get(cause, cause),
            "attributableCases": cases,
            "attributableRate": rate,
            "crfIds": [r["crfId"] for r in results],
        })
    return rollups


def split_mortality_totals(
    crf_results: list[dict[str, Any]],
) -> tuple[dict[str, float], dict[str, float] | None]:
    """Return (totalDeaths, allCauseDeaths).

    ``totalDeaths`` = sum across CRFs with ``_endpointType == "mortality"``
    AND ``_cause != "all_cause"`` (cause-specific mortality only).

    ``allCauseDeaths`` = sum across CRFs with ``_cause == "all_cause"``
    AND ``_endpointType == "mortality"``, or ``None`` when no all-cause
    mortality CRF was selected.

    The two totals are never summed together — that would double-count.
    """
    total_deaths = _zero_ci()
    all_cause_deaths = _zero_ci()
    any_all_cause = False

    for r in crf_results:
        if r.get("_endpointType") != "mortality":
            continue
        if r.get("_cause") == "all_cause":
            all_cause_deaths = _sum_ci(all_cause_deaths, r["attributableCases"])
            any_all_cause = True
        else:
            total_deaths = _sum_ci(total_deaths, r["attributableCases"])

    return total_deaths, (all_cause_deaths if any_all_cause else None)
