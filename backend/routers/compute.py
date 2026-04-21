"""POST /api/compute — run HIA computation and return results."""

from __future__ import annotations

import asyncio
import logging
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.database import get_db
from backend.models.file_upload import FileUpload
from backend.services.hia_engine import compute_hia, _summarise_spatial

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["compute"])

STORAGE_PATH = Path(os.getenv("STORAGE_PATH", "./data"))
UPLOAD_DIR = STORAGE_PATH / "uploads"

_executor = ProcessPoolExecutor(max_workers=2)


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


class SpatialComputeRequest(BaseModel):
    concentrationFileId: int
    controlFileId: int | None = None
    controlConcentration: float | None = None
    populationFileId: int
    boundaryFileId: int
    baselineIncidence: float
    selectedCRFs: list[CRFInput]
    monteCarloIterations: int = Field(default=1000, ge=100, le=50_000)


class ZoneResult(BaseModel):
    zoneId: str
    zoneName: str | None = None
    geometry: dict | None = None
    baselineConcentration: float
    controlConcentration: float
    population: float
    results: list[CRFResult]


class SpatialComputeResponse(BaseModel):
    zones: list[ZoneResult]
    aggregate: ComputeResponse
    totalDeaths: EstimateCI


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


def _run_spatial_compute(
    conc_path: str,
    pop_path: str,
    boundary_path: str,
    ctrl_path: str | None,
    ctrl_value: float | None,
    baseline_incidence: float,
    selected_crfs: list[dict],
    mc_iterations: int,
) -> dict:
    """Worker function for ProcessPoolExecutor. Runs in a separate process."""
    import numpy as np

    from backend.services.geo_processor import prepare_spatial_inputs
    from backend.services.hia_engine import (
        _beta_se,
        _compute_single_crf,
        _summarise,
        _summarise_spatial,
    )

    # Step 1: Prepare spatial inputs
    spatial = prepare_spatial_inputs(
        concentration_raster_path=conc_path,
        population_raster_path=pop_path,
        boundary_path=boundary_path,
        control_raster_path=ctrl_path,
        control_value=ctrl_value,
    )

    zone_ids = spatial["zone_ids"]
    zone_names = spatial["zone_names"]
    c_baseline = spatial["c_baseline"]  # shape (n_zones,)
    c_control = spatial["c_control"]  # shape (n_zones,)
    population = spatial["population"]  # shape (n_zones,)
    geometries = spatial["geometries"]
    n_zones = len(zone_ids)

    rng = np.random.default_rng()
    per_100k = 100_000
    mortality_keywords = ("mortality", "death", "deaths")

    # Step 2: Per-zone, per-CRF computation
    # zones[i] = { zoneId, results: [...] }
    zone_results: list[dict] = []
    for zi in range(n_zones):
        zone_results.append({
            "zoneId": zone_ids[zi],
            "zoneName": zone_names[zi],
            "geometry": geometries[zi],
            "baselineConcentration": float(c_baseline[zi]),
            "controlConcentration": float(c_control[zi]),
            "population": float(population[zi]),
            "results": [],
        })

    # Aggregate accumulators
    agg_mortality_cases = np.zeros(mc_iterations)

    all_crf_results_agg: list[dict] = []

    for crf in selected_crfs:
        se = _beta_se(crf["betaLow"], crf["betaHigh"])
        form = crf.get("functionalForm", "log-linear")
        y0 = crf.get("defaultRate", baseline_incidence) or baseline_incidence

        betas = rng.normal(loc=crf["beta"], scale=se, size=mc_iterations)

        delta_c = c_baseline - c_control  # (n_zones,)

        # Compute per zone: betas (n_iter,) x delta_c (n_zones,) -> (n_iter, n_zones)
        # We need to call the CRF for each zone since some forms need c_base/c_ctrl
        zone_cases = np.zeros((mc_iterations, n_zones))
        zone_paf = np.zeros((mc_iterations, n_zones))

        for zi in range(n_zones):
            cases_zi, paf_zi = _compute_single_crf(
                form, betas,
                float(c_baseline[zi]), float(c_control[zi]),
                y0, float(population[zi]),
                crf=crf,
            )
            zone_cases[:, zi] = cases_zi
            zone_paf[:, zi] = paf_zi

        # Per-zone summaries
        cases_by_zone = _summarise_spatial(zone_cases)
        paf_by_zone = _summarise_spatial(zone_paf)

        pop_arr = population.copy()
        pop_arr[pop_arr == 0] = 1  # avoid division by zero
        zone_rate = (zone_cases / pop_arr[np.newaxis, :]) * per_100k
        rate_by_zone = _summarise_spatial(zone_rate)

        for zi in range(n_zones):
            zone_results[zi]["results"].append({
                "crfId": crf["id"],
                "study": crf.get("source", ""),
                "endpoint": crf.get("endpoint", ""),
                "attributableCases": cases_by_zone[zi],
                "attributableFraction": paf_by_zone[zi],
                "attributableRate": rate_by_zone[zi],
            })

        # Aggregate across zones (sum cases per MC iteration)
        total_cases_per_iter = zone_cases.sum(axis=1)  # (n_iter,)
        total_pop = population.sum()

        agg_cases_summary = _summarise(total_cases_per_iter)
        agg_paf_summary = _summarise(
            total_cases_per_iter / (y0 * total_pop) if total_pop > 0
            else np.zeros(mc_iterations)
        )
        agg_rate_summary = _summarise(
            (total_cases_per_iter / total_pop * per_100k) if total_pop > 0
            else np.zeros(mc_iterations)
        )

        all_crf_results_agg.append({
            "crfId": crf["id"],
            "study": crf.get("source", ""),
            "endpoint": crf.get("endpoint", ""),
            "attributableCases": agg_cases_summary,
            "attributableFraction": agg_paf_summary,
            "attributableRate": agg_rate_summary,
        })

        # Accumulate mortality
        is_mortality = any(
            kw in crf.get("endpoint", "").lower() for kw in mortality_keywords
        )
        if is_mortality:
            agg_mortality_cases += total_cases_per_iter

    total_deaths = _summarise(agg_mortality_cases)

    return {
        "zones": zone_results,
        "aggregate": {
            "results": all_crf_results_agg,
            "totalDeaths": total_deaths,
        },
        "totalDeaths": total_deaths,
    }


@router.post("/compute/spatial", response_model=SpatialComputeResponse)
async def run_spatial_compute(
    req: SpatialComputeRequest,
    db: AsyncSession = Depends(get_db),
) -> SpatialComputeResponse:
    """Run spatially-resolved HIA computation.

    Looks up uploaded file paths, delegates heavy computation to a
    ProcessPoolExecutor worker, and returns per-zone results.
    """
    # Resolve file IDs to paths
    file_ids = [req.concentrationFileId, req.populationFileId, req.boundaryFileId]
    if req.controlFileId:
        file_ids.append(req.controlFileId)

    result = await db.execute(
        select(FileUpload).where(FileUpload.id.in_(file_ids))
    )
    records = {r.id: r for r in result.scalars().all()}

    for fid in file_ids:
        if fid not in records:
            raise HTTPException(status_code=404, detail=f"File upload {fid} not found")
        if records[fid].status == "error":
            raise HTTPException(
                status_code=400,
                detail=f"File {records[fid].original_filename} failed validation: "
                f"{records[fid].error_message}",
            )

    conc_path = str(_resolve_file_path(records[req.concentrationFileId]))
    pop_path = str(_resolve_file_path(records[req.populationFileId]))
    boundary_path = str(_resolve_file_path(records[req.boundaryFileId]))
    ctrl_path = (
        str(_resolve_file_path(records[req.controlFileId]))
        if req.controlFileId
        else None
    )

    crfs = [crf.model_dump() for crf in req.selectedCRFs]

    loop = asyncio.get_event_loop()
    raw = await loop.run_in_executor(
        _executor,
        _run_spatial_compute,
        conc_path,
        pop_path,
        boundary_path,
        ctrl_path,
        req.controlConcentration,
        req.baselineIncidence,
        crfs,
        req.monteCarloIterations,
    )

    return SpatialComputeResponse(**raw)
