"""POST /api/compute — run HIA computation and return results."""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Annotated, Literal, Union

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from pydantic import Field as PydField
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.database import get_db
from backend.models.file_upload import FileUpload
from backend.services.hia_engine import compute_hia, _summarise_spatial
from backend.services.resolver import (
    prepare_builtin_inputs,
    prepare_custom_boundary_inputs,
    ResolvedInputs,
    Provenance,
    YearGapTooLarge,
)
from backend.services.rollups import build_cause_rollups, split_mortality_totals

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["compute"])

STORAGE_PATH = Path(os.getenv("STORAGE_PATH", "./data"))
UPLOAD_DIR = STORAGE_PATH / "uploads"

_executor = ProcessPoolExecutor(max_workers=2)


# Temporary stub — replaced in Task 12 by backend/services/results_cache.save_result
def save_result(result_id, response):
    pass


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
    totalDeaths: EstimateCI | None = None
    warnings: list[str] | None = None


# ── Spatial models ─────────────────────────────────────────────────


class ZoneResult(BaseModel):
    zoneId: str
    zoneName: str | None = None
    parentId: str | None = None  # NEW
    geometry: dict | None = None
    baselineConcentration: float
    controlConcentration: float
    population: float
    results: list[CRFResult]


# ── New discriminated-union request models ────────────────────────

class CRFInputV2(BaseModel):
    """Extension of CRFInput that carries cause / endpointType."""
    id: str
    source: str = ""
    endpoint: str = ""
    beta: float
    betaLow: float
    betaHigh: float
    functionalForm: str = "log-linear"
    defaultRate: float | None = None
    cause: str = "all_cause"          # enum validated client-side
    endpointType: str = "mortality"   # mortality | hospitalization | ed_visit | incidence | prevalence


class BuiltinMode(BaseModel):
    mode: Literal["builtin"]
    pollutant: str
    country: str
    year: int
    analysisLevel: Literal["country", "state", "county", "tract"]
    stateFilter: str | None = None
    countyFilter: str | None = None
    controlMode: Literal["scalar", "builtin", "rollback", "benchmark"]
    controlConcentration: float | None = None
    controlRollbackPercent: float | None = None
    selectedCRFs: list[CRFInputV2]
    monteCarloIterations: int = Field(default=1000, ge=100, le=50_000)


class UploadedMode(BaseModel):
    mode: Literal["uploaded"]
    concentrationFileId: int
    controlFileId: int | None = None
    controlConcentration: float | None = None
    populationFileId: int
    boundaryFileId: int
    selectedCRFs: list[CRFInputV2]
    monteCarloIterations: int = Field(default=1000, ge=100, le=50_000)


class CustomBoundaryBuiltinMode(BaseModel):
    mode: Literal["builtin_custom_boundary"]
    pollutant: str
    country: str
    year: int
    boundaryFileId: int
    controlMode: Literal["scalar", "builtin", "rollback", "benchmark"]
    controlConcentration: float | None = None
    controlRollbackPercent: float | None = None
    selectedCRFs: list[CRFInputV2]
    monteCarloIterations: int = Field(default=1000, ge=100, le=50_000)


SpatialComputeRequest = Annotated[
    Union[BuiltinMode, UploadedMode, CustomBoundaryBuiltinMode],
    PydField(discriminator="mode"),
]


# ── New response models ──────────────────────────────────────────

class ProvenanceModel(BaseModel):
    concentration: dict
    population: dict
    incidence: dict


class CauseRollup(BaseModel):
    cause: str
    endpointLabel: str
    attributableCases: EstimateCI
    attributableRate: EstimateCI
    crfIds: list[str]


class SpatialComputeResponseV2(BaseModel):
    """Extended spatial response with provenance, rollups, and separate totals."""
    resultId: str
    zones: list[ZoneResult]
    aggregate: ComputeResponse
    causeRollups: list[CauseRollup]
    totalDeaths: EstimateCI
    allCauseDeaths: EstimateCI | None = None
    provenance: ProvenanceModel
    warnings: list[str] = Field(default_factory=list)
    processingTimeSeconds: float = 0.0


# ── Scalar endpoint ────────────────────────────────────────────────


@router.post("/compute", response_model=ComputeResponse)
async def run_compute(req: ComputeRequest) -> ComputeResponse:
    """Run HIA computation synchronously and return results.

    Accepts the same config shape as the frontend JS engine.
    """
    config = req.model_dump()
    config["selectedCRFs"] = [crf.model_dump() for crf in req.selectedCRFs]
    result = compute_hia(config)
    return ComputeResponse(**result)


# ── Spatial endpoint ───────────────────────────────────────────────


def _resolve_file_path(record: FileUpload) -> Path:
    """Resolve a FileUpload record to a filesystem path."""
    return UPLOAD_DIR / record.stored_filename


@router.post("/compute/spatial", response_model=SpatialComputeResponseV2)
async def run_spatial_compute(
    req: SpatialComputeRequest,
    db: AsyncSession = Depends(get_db),
) -> SpatialComputeResponseV2:
    """Unified spatial compute: built-in, uploaded, or custom boundary."""
    start = time.perf_counter()

    try:
        if req.mode == "builtin":
            resolved = prepare_builtin_inputs(
                pollutant=req.pollutant, country=req.country, year=req.year,
                analysis_level=req.analysisLevel,
                state_filter=req.stateFilter, county_filter=req.countyFilter,
                control_mode=req.controlMode,
                control_value=req.controlConcentration,
                rollback_percent=req.controlRollbackPercent,
            )
        elif req.mode == "builtin_custom_boundary":
            boundary_record = await _get_upload(db, req.boundaryFileId)
            resolved = prepare_custom_boundary_inputs(
                pollutant=req.pollutant, country=req.country, year=req.year,
                boundary_path=str(_resolve_file_path(boundary_record)),
                control_mode=req.controlMode,
                control_value=req.controlConcentration,
                rollback_percent=req.controlRollbackPercent,
            )
        else:  # uploaded
            resolved = await _resolve_uploaded(db, req)
    except YearGapTooLarge as e:
        raise HTTPException(status_code=422, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    crfs_as_dicts = [crf.model_dump() for crf in req.selectedCRFs]

    loop = asyncio.get_event_loop()
    raw = await loop.run_in_executor(
        _executor,
        _run_spatial_compute_v2,
        resolved,
        crfs_as_dicts,
        req.monteCarloIterations,
    )

    crf_results_with_tags = raw["aggregate_crf_results"]
    rollups = build_cause_rollups(crf_results_with_tags)
    total_deaths, all_cause_deaths = split_mortality_totals(crf_results_with_tags)

    # Strip private keys before serialization
    for r in crf_results_with_tags:
        r.pop("_cause", None)
        r.pop("_endpointType", None)

    result_id = str(uuid.uuid4())
    elapsed = time.perf_counter() - start

    response = SpatialComputeResponseV2(
        resultId=result_id,
        zones=[ZoneResult.model_validate(z) for z in raw["zones"]],
        aggregate=ComputeResponse(
            results=[CRFResult.model_validate(r) for r in crf_results_with_tags],
            totalDeaths=EstimateCI.model_validate(total_deaths),
        ),
        causeRollups=[CauseRollup.model_validate(r) for r in rollups],
        totalDeaths=EstimateCI.model_validate(total_deaths),
        allCauseDeaths=(
            EstimateCI.model_validate(all_cause_deaths) if all_cause_deaths else None
        ),
        provenance=ProvenanceModel.model_validate({
            "concentration": resolved.provenance.concentration,
            "population": resolved.provenance.population,
            "incidence": resolved.provenance.incidence,
        }),
        warnings=resolved.warnings,
        processingTimeSeconds=elapsed,
    )
    save_result(result_id, response)
    return response


async def _get_upload(db: AsyncSession, file_id: int) -> FileUpload:
    result = await db.execute(select(FileUpload).where(FileUpload.id == file_id))
    record = result.scalar_one_or_none()
    if record is None:
        raise HTTPException(status_code=404, detail=f"File upload {file_id} not found")
    if record.status == "error":
        raise HTTPException(
            status_code=400,
            detail=f"File {record.original_filename} failed validation",
        )
    return record


async def _resolve_uploaded(
    db: AsyncSession, req: UploadedMode,
) -> ResolvedInputs:
    """Adapter: wrap the existing prepare_spatial_inputs output as ResolvedInputs."""
    from backend.services.geo_processor import prepare_spatial_inputs

    file_ids = [req.concentrationFileId, req.populationFileId, req.boundaryFileId]
    if req.controlFileId:
        file_ids.append(req.controlFileId)

    result = await db.execute(select(FileUpload).where(FileUpload.id.in_(file_ids)))
    records = {r.id: r for r in result.scalars().all()}
    for fid in file_ids:
        if fid not in records:
            raise HTTPException(status_code=404, detail=f"File upload {fid} not found")

    spatial = prepare_spatial_inputs(
        concentration_raster_path=str(_resolve_file_path(records[req.concentrationFileId])),
        population_raster_path=str(_resolve_file_path(records[req.populationFileId])),
        boundary_path=str(_resolve_file_path(records[req.boundaryFileId])),
        control_raster_path=(
            str(_resolve_file_path(records[req.controlFileId]))
            if req.controlFileId else None
        ),
        control_value=req.controlConcentration,
    )

    return ResolvedInputs(
        zone_ids=spatial["zone_ids"],
        zone_names=spatial["zone_names"],
        parent_ids=[None] * len(spatial["zone_ids"]),
        geometries=spatial["geometries"],
        c_baseline=spatial["c_baseline"],
        c_control=spatial["c_control"],
        population=spatial["population"],
        provenance=Provenance(
            concentration={"grain": "raster", "source": "uploaded"},
            population={"grain": "raster", "source": "uploaded"},
            incidence={"grain": "crf_default", "source": "crf_library"},
        ),
        warnings=[],
    )


def _run_spatial_compute_v2(
    resolved: ResolvedInputs,
    selected_crfs: list[dict],
    mc_iterations: int,
) -> dict:
    """Worker: same math as the original but consumes ResolvedInputs."""
    import numpy as np
    from backend.services.hia_engine import (
        _beta_se, _compute_single_crf, _summarise, _summarise_spatial,
    )

    n_zones = len(resolved.zone_ids)
    rng = np.random.default_rng()
    per_100k = 100_000

    zones: list[dict] = [
        {
            "zoneId": resolved.zone_ids[i],
            "zoneName": resolved.zone_names[i],
            "parentId": resolved.parent_ids[i],
            "geometry": resolved.geometries[i],
            "baselineConcentration": float(resolved.c_baseline[i]),
            "controlConcentration": float(resolved.c_control[i]),
            "population": float(resolved.population[i]),
            "results": [],
        }
        for i in range(n_zones)
    ]

    crf_results_agg: list[dict] = []

    for crf in selected_crfs:
        se = _beta_se(crf["betaLow"], crf["betaHigh"])
        form = crf.get("functionalForm", "log-linear")
        y0 = crf.get("defaultRate") or 0.008
        betas = rng.normal(loc=crf["beta"], scale=se, size=mc_iterations)

        zone_cases = np.zeros((mc_iterations, n_zones))
        zone_paf = np.zeros((mc_iterations, n_zones))
        for zi in range(n_zones):
            cases_zi, paf_zi = _compute_single_crf(
                form, betas,
                float(resolved.c_baseline[zi]),
                float(resolved.c_control[zi]),
                y0, float(resolved.population[zi]),
                crf=crf,
            )
            zone_cases[:, zi] = cases_zi
            zone_paf[:, zi] = paf_zi

        cases_by_zone = _summarise_spatial(zone_cases)
        paf_by_zone = _summarise_spatial(zone_paf)
        pop_arr = resolved.population.copy()
        pop_arr[pop_arr == 0] = 1
        zone_rate = (zone_cases / pop_arr[np.newaxis, :]) * per_100k
        rate_by_zone = _summarise_spatial(zone_rate)

        for zi in range(n_zones):
            zones[zi]["results"].append({
                "crfId": crf["id"],
                "study": crf.get("source", ""),
                "endpoint": crf.get("endpoint", ""),
                "attributableCases": cases_by_zone[zi],
                "attributableFraction": paf_by_zone[zi],
                "attributableRate": rate_by_zone[zi],
            })

        total_cases_per_iter = zone_cases.sum(axis=1)
        total_pop = resolved.population.sum()

        crf_results_agg.append({
            "crfId": crf["id"],
            "study": crf.get("source", ""),
            "endpoint": crf.get("endpoint", ""),
            "attributableCases": _summarise(total_cases_per_iter),
            "attributableFraction": _summarise(
                total_cases_per_iter / (y0 * total_pop) if total_pop > 0
                else np.zeros(mc_iterations)
            ),
            "attributableRate": _summarise(
                (total_cases_per_iter / total_pop * per_100k) if total_pop > 0
                else np.zeros(mc_iterations)
            ),
            "_cause": crf.get("cause", "all_cause"),
            "_endpointType": crf.get("endpointType", "mortality"),
        })

    return {"zones": zones, "aggregate_crf_results": crf_results_agg}
