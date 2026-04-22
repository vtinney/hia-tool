"""Tests for the discriminated-union request and extended response models."""
import pytest
from pydantic import ValidationError

from backend.routers.compute import (
    SpatialComputeRequest, SpatialComputeResponse, ProvenanceModel,
    CauseRollup, EstimateCI, CRFInput,
)
# Use TypeAdapter for discriminated-union validation since SpatialComputeRequest
# is an Annotated union, not a BaseModel.
from pydantic import TypeAdapter
_REQ_ADAPTER = TypeAdapter(SpatialComputeRequest)


def _sample_crf() -> dict:
    return {
        "id": "epa_pm25_ihd_adult", "source": "Pope 2004", "endpoint": "IHD",
        "beta": 0.015, "betaLow": 0.01, "betaHigh": 0.02,
        "functionalForm": "log-linear", "defaultRate": 0.0025,
        "cause": "ihd", "endpointType": "mortality",
    }


def test_builtin_request_validates():
    payload = {
        "mode": "builtin",
        "pollutant": "pm25", "country": "us", "year": 2022,
        "analysisLevel": "tract", "stateFilter": "06",
        "controlMode": "benchmark", "controlConcentration": 5.0,
        "selectedCRFs": [_sample_crf()],
        "monteCarloIterations": 1000,
    }
    req = _REQ_ADAPTER.validate_python(payload)
    assert req.mode == "builtin"
    assert req.analysisLevel == "tract"


def test_uploaded_request_validates():
    payload = {
        "mode": "uploaded",
        "concentrationFileId": 1, "populationFileId": 2, "boundaryFileId": 3,
        "selectedCRFs": [_sample_crf()],
    }
    req = _REQ_ADAPTER.validate_python(payload)
    assert req.mode == "uploaded"


def test_custom_boundary_request_validates():
    payload = {
        "mode": "builtin_custom_boundary",
        "pollutant": "pm25", "country": "us", "year": 2022,
        "boundaryFileId": 3,
        "controlMode": "scalar", "controlConcentration": 5.0,
        "selectedCRFs": [_sample_crf()],
    }
    req = _REQ_ADAPTER.validate_python(payload)
    assert req.mode == "builtin_custom_boundary"


def test_discriminator_rejects_unknown_mode():
    with pytest.raises(ValidationError):
        _REQ_ADAPTER.validate_python({"mode": "banana"})


def test_cause_rollup_has_required_fields():
    r = CauseRollup(
        cause="ihd", endpointLabel="Ischemic heart disease",
        attributableCases=EstimateCI(mean=100, lower95=50, upper95=150),
        attributableRate=EstimateCI(mean=5.0, lower95=2.5, upper95=7.5),
        crfIds=["epa_pm25_ihd_adult"],
    )
    assert r.cause == "ihd"
    assert len(r.crfIds) == 1
