"""Tests for CSV long-format and GeoJSON wide-format serializers."""
from backend.services.download_serializers import (
    result_to_csv_long, result_to_geojson_wide,
)


def _sample_result() -> dict:
    return {
        "resultId": "abc123",
        "zones": [
            {
                "zoneId": "06001",
                "zoneName": "Alameda",
                "parentId": "06",
                "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]},
                "baselineConcentration": 11.4,
                "controlConcentration": 5.0,
                "population": 1_600_000,
                "results": [
                    {
                        "crfId": "epa_pm25_ihd_adult", "study": "Pope 2004",
                        "endpoint": "Ischemic heart disease",
                        "attributableCases": {"mean": 120, "lower95": 80, "upper95": 160},
                        "attributableFraction": {"mean": 0.03, "lower95": 0.02, "upper95": 0.04},
                        "attributableRate": {"mean": 7.5, "lower95": 5.0, "upper95": 10.0},
                    },
                ],
            },
        ],
        "causeRollups": [
            {
                "cause": "ihd", "endpointLabel": "Ischemic heart disease",
                "attributableCases": {"mean": 120, "lower95": 80, "upper95": 160},
                "attributableRate": {"mean": 7.5, "lower95": 5.0, "upper95": 10.0},
                "crfIds": ["epa_pm25_ihd_adult"],
            },
        ],
    }


def test_csv_long_has_one_row_per_polygon_crf():
    csv_str = result_to_csv_long(_sample_result(), crf_metadata={
        "epa_pm25_ihd_adult": {"cause": "ihd", "endpointType": "mortality"},
    })
    lines = csv_str.strip().splitlines()
    assert len(lines) == 2  # header + 1 row
    header = lines[0].split(",")
    assert "polygon_id" in header
    assert "cause" in header
    assert "attributable_cases_mean" in header
    assert "06001" in lines[1]


def test_geojson_wide_pivots_causes():
    gj = result_to_geojson_wide(_sample_result())
    assert gj["type"] == "FeatureCollection"
    assert len(gj["features"]) == 1
    props = gj["features"][0]["properties"]
    assert props["polygon_id"] == "06001"
    assert props["cases_ihd_mean"] == 120
    assert props["cases_ihd_lower95"] == 80
    assert props["rate_per_100k_ihd_mean"] == 7.5
    assert "geometry" not in props
