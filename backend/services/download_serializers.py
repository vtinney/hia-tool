"""Serializers for the Results download endpoints.

CSV  (long): one row per polygon × CRF.
GeoJSON (wide): one Feature per polygon, per-cause rollups pivoted to properties.
"""
from __future__ import annotations

import csv
import io
from typing import Any


CSV_HEADER = [
    "polygon_id", "polygon_name", "parent_id",
    "baseline_c", "control_c", "delta_c", "population",
    "crf_id", "crf_source", "cause", "endpoint_type", "endpoint",
    "attributable_cases_mean", "attributable_cases_lower95", "attributable_cases_upper95",
    "attributable_fraction_mean",
    "rate_per_100k_mean", "rate_per_100k_lower95", "rate_per_100k_upper95",
]


def result_to_csv_long(
    result: dict[str, Any],
    crf_metadata: dict[str, dict[str, str]],
) -> str:
    """Serialize a SpatialComputeResponseV2 to long-format CSV.

    ``crf_metadata`` maps CRF id → {cause, endpointType} so we can
    enrich each row without inferring from the endpoint string.
    """
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(CSV_HEADER)

    for zone in result["zones"]:
        base_c = zone["baselineConcentration"]
        ctrl_c = zone["controlConcentration"]
        delta = base_c - ctrl_c
        for crf_result in zone["results"]:
            meta = crf_metadata.get(crf_result["crfId"], {})
            w.writerow([
                zone["zoneId"],
                zone.get("zoneName") or "",
                zone.get("parentId") or "",
                base_c, ctrl_c, delta, zone["population"],
                crf_result["crfId"],
                crf_result.get("study", ""),
                meta.get("cause", ""),
                meta.get("endpointType", ""),
                crf_result.get("endpoint", ""),
                crf_result["attributableCases"]["mean"],
                crf_result["attributableCases"]["lower95"],
                crf_result["attributableCases"]["upper95"],
                crf_result["attributableFraction"]["mean"],
                crf_result["attributableRate"]["mean"],
                crf_result["attributableRate"]["lower95"],
                crf_result["attributableRate"]["upper95"],
            ])

    return buf.getvalue()


def result_to_geojson_wide(result: dict[str, Any]) -> dict[str, Any]:
    """Serialize to GeoJSON. Per-cause rollups become pivoted properties."""
    # Build reverse index crfId → cause from causeRollups.
    crf_to_cause = {}
    for roll in result.get("causeRollups", []):
        for cid in roll.get("crfIds", []):
            crf_to_cause[cid] = roll["cause"]

    features = []
    for zone in result["zones"]:
        props = {
            "polygon_id": zone["zoneId"],
            "polygon_name": zone.get("zoneName"),
            "parent_id": zone.get("parentId"),
            "baseline_c": zone["baselineConcentration"],
            "control_c": zone["controlConcentration"],
            "delta_c": zone["baselineConcentration"] - zone["controlConcentration"],
            "population": zone["population"],
        }

        by_cause: dict[str, dict[str, float]] = {}
        for cr in zone["results"]:
            cause = crf_to_cause.get(cr["crfId"], "unknown")
            bucket = by_cause.setdefault(cause, {
                "cases_mean": 0.0, "cases_lower95": 0.0, "cases_upper95": 0.0,
                "rate_mean": 0.0, "rate_lower95": 0.0, "rate_upper95": 0.0,
            })
            bucket["cases_mean"] += cr["attributableCases"]["mean"]
            bucket["cases_lower95"] += cr["attributableCases"]["lower95"]
            bucket["cases_upper95"] += cr["attributableCases"]["upper95"]
            bucket["rate_mean"] += cr["attributableRate"]["mean"]
            bucket["rate_lower95"] += cr["attributableRate"]["lower95"]
            bucket["rate_upper95"] += cr["attributableRate"]["upper95"]

        for cause, b in by_cause.items():
            props[f"cases_{cause}_mean"] = b["cases_mean"]
            props[f"cases_{cause}_lower95"] = b["cases_lower95"]
            props[f"cases_{cause}_upper95"] = b["cases_upper95"]
            props[f"rate_per_100k_{cause}_mean"] = b["rate_mean"]
            props[f"rate_per_100k_{cause}_lower95"] = b["rate_lower95"]
            props[f"rate_per_100k_{cause}_upper95"] = b["rate_upper95"]

        features.append({
            "type": "Feature",
            "geometry": zone["geometry"],
            "properties": props,
        })

    return {"type": "FeatureCollection", "features": features}
