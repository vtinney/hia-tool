"""POST /api/compute — run HIA computation and return results."""

from __future__ import annotations

from pydantic import BaseModel, Field
from fastapi import APIRouter

from backend.services.hia_engine import compute_hia

router = APIRouter(prefix="/api", tags=["compute"])


# ── Request / response models ──────────────────────────────────────


class CRFInput(BaseModel):
    id: str
    source: str = ""
    endpoint: str = ""
    beta: float
    betaLow: float
    betaHigh: float
    functionalForm: str = "log-linear"
    defaultRate: float | None = None


class ComputeRequest(BaseModel):
    baselineConcentration: float
    controlConcentration: float
    baselineIncidence: float
    population: float
    selectedCRFs: list[CRFInput]
    monteCarloIterations: int = Field(default=1000, ge=100, le=50_000)


class EstimateCI(BaseModel):
    mean: float
    lower95: float
    upper95: float


class CRFResult(BaseModel):
    crfId: str
    study: str
    endpoint: str
    attributableCases: EstimateCI
    attributableFraction: EstimateCI
    attributableRate: EstimateCI


class ComputeResponse(BaseModel):
    results: list[CRFResult]
    totalDeaths: EstimateCI


# ── Endpoint ───────────────────────────────────────────────────────


@router.post("/compute", response_model=ComputeResponse)
async def run_compute(req: ComputeRequest) -> ComputeResponse:
    """Run HIA computation synchronously and return results.

    Accepts the same config shape as the frontend JS engine.
    """
    config = req.model_dump()
    # Convert CRF Pydantic models to plain dicts for the engine
    config["selectedCRFs"] = [crf.model_dump() for crf in req.selectedCRFs]
    result = compute_hia(config)
    return ComputeResponse(**result)
