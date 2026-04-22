"""Tests for cause-rollup aggregation and mortality split."""
from backend.services.rollups import build_cause_rollups, split_mortality_totals


def _crf_result(crf_id, cause, endpoint_type, cases_mean, rate_mean):
    return {
        "crfId": crf_id,
        "study": "test",
        "endpoint": crf_id,
        "attributableCases": {"mean": cases_mean, "lower95": cases_mean * 0.8, "upper95": cases_mean * 1.2},
        "attributableFraction": {"mean": 0.01, "lower95": 0.005, "upper95": 0.015},
        "attributableRate": {"mean": rate_mean, "lower95": rate_mean * 0.8, "upper95": rate_mean * 1.2},
        "_cause": cause,
        "_endpointType": endpoint_type,
    }


def test_build_cause_rollups_sums_within_cause():
    # Two stroke CRFs → one "stroke" rollup summing both
    results = [
        _crf_result("epa_pm25_stroke_adult", "stroke", "mortality", 100, 5.0),
        _crf_result("gbd_pm25_stroke",       "stroke", "mortality", 80, 4.0),
        _crf_result("epa_pm25_ihd_adult",    "ihd",    "mortality", 50, 2.5),
    ]
    rollups = build_cause_rollups(results)
    by_cause = {r["cause"]: r for r in rollups}
    assert by_cause["stroke"]["attributableCases"]["mean"] == 180
    assert by_cause["ihd"]["attributableCases"]["mean"] == 50
    assert sorted(by_cause["stroke"]["crfIds"]) == sorted(
        ["epa_pm25_stroke_adult", "gbd_pm25_stroke"]
    )


def test_split_mortality_totals_excludes_all_cause():
    # IHD + stroke are cause-specific; ACM is separate
    results = [
        _crf_result("epa_pm25_ihd_adult",  "ihd",       "mortality", 50, 2.5),
        _crf_result("epa_pm25_stroke_adult","stroke",   "mortality", 30, 1.5),
        _crf_result("epa_pm25_acm_adult",  "all_cause", "mortality", 200, 10.0),
    ]
    total_deaths, all_cause_deaths = split_mortality_totals(results)
    assert total_deaths["mean"] == 80  # IHD + stroke, NOT including all-cause
    assert all_cause_deaths is not None
    assert all_cause_deaths["mean"] == 200


def test_split_mortality_totals_no_all_cause():
    results = [
        _crf_result("epa_pm25_ihd_adult", "ihd",    "mortality", 50, 2.5),
        _crf_result("epa_pm25_stroke_adult","stroke","mortality", 30, 1.5),
    ]
    total_deaths, all_cause_deaths = split_mortality_totals(results)
    assert total_deaths["mean"] == 80
    assert all_cause_deaths is None


def test_build_cause_rollups_skips_non_mortality_when_filtered():
    """Rollups include ALL causes for UI, but endpoint_type filtering
    happens at the mortality-total step."""
    results = [
        _crf_result("hrapie_pm25_resp_hosp", "respiratory_hosp", "hospitalization", 25, 1.2),
        _crf_result("epa_pm25_ihd_adult",    "ihd",              "mortality",        50, 2.5),
    ]
    rollups = build_cause_rollups(results)
    causes = {r["cause"] for r in rollups}
    assert causes == {"respiratory_hosp", "ihd"}
    total_deaths, _ = split_mortality_totals(results)
    assert total_deaths["mean"] == 50  # hosp excluded
