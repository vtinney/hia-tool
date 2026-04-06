"""POST/GET/DELETE /api/uploads — file upload management for geospatial data."""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, Form, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.database import get_db
from backend.models.file_upload import FileUpload

# ── Constants ──────────────────────────────────────────────────────

STORAGE_PATH = Path(os.getenv("STORAGE_PATH", "./data"))
UPLOAD_DIR = STORAGE_PATH / "uploads"

MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB

ALLOWED_EXTENSIONS: dict[str, list[str]] = {
    "concentration": [".tif", ".tiff", ".csv", ".nc"],
    "population": [".tif", ".tiff", ".csv"],
    "boundary": [".zip", ".gpkg", ".geojson"],
}

EXTENSION_TO_TYPE: dict[str, str] = {
    ".tif": "geotiff",
    ".tiff": "geotiff",
    ".nc": "netcdf",
    ".csv": "csv",
    ".zip": "shapefile",
    ".gpkg": "geopackage",
    ".geojson": "geojson",
}


# ── Pydantic schemas ──────────────────────────────────────────────


class FileUploadOut(BaseModel):
    id: int
    original_filename: str
    file_type: str
    category: str
    file_size_bytes: int
    crs: str | None
    bounds_json: dict | None
    metadata_json: dict | None
    status: str
    error_message: str | None
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Validation helpers ─────────────────────────────────────────────


def _validate_raster(path: Path) -> dict:
    """Extract metadata from a raster file using rasterio."""
    import rasterio

    with rasterio.open(path) as src:
        bounds = src.bounds
        return {
            "crs": str(src.crs) if src.crs else None,
            "bounds": {
                "west": bounds.left,
                "south": bounds.bottom,
                "east": bounds.right,
                "north": bounds.top,
            },
            "metadata": {
                "width": src.width,
                "height": src.height,
                "band_count": src.count,
                "dtype": str(src.dtypes[0]),
                "nodata": src.nodata,
                "resolution": list(src.res),
            },
        }


def _validate_vector(path: Path) -> dict:
    """Extract metadata from a vector file using geopandas."""
    import geopandas as gpd

    prefix = "zip://" if path.suffix == ".zip" else ""
    gdf = gpd.read_file(f"{prefix}{path}")
    bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
    geom_types = gdf.geometry.geom_type.unique().tolist()

    return {
        "crs": str(gdf.crs) if gdf.crs else None,
        "bounds": {
            "west": float(bounds[0]),
            "south": float(bounds[1]),
            "east": float(bounds[2]),
            "north": float(bounds[3]),
        },
        "metadata": {
            "feature_count": len(gdf),
            "geometry_types": geom_types,
            "columns": [c for c in gdf.columns if c != "geometry"],
        },
    }


def _validate_file(path: Path, file_type: str) -> dict:
    """Dispatch validation based on file type. Returns {crs, bounds, metadata}."""
    if file_type in ("geotiff",):
        return _validate_raster(path)
    elif file_type in ("shapefile", "geopackage", "geojson"):
        return _validate_vector(path)
    # csv and netcdf: skip geo-validation for now
    return {"crs": None, "bounds": None, "metadata": {}}


# ── Router ─────────────────────────────────────────────────────────

router = APIRouter(prefix="/api", tags=["uploads"])


@router.post("/uploads", response_model=FileUploadOut, status_code=201)
async def upload_file(
    file: UploadFile,
    category: str = Form(...),
    db: AsyncSession = Depends(get_db),
) -> FileUpload:
    # Validate category
    if category not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid category. Must be one of: {list(ALLOWED_EXTENSIONS.keys())}",
        )

    # Validate extension
    original_name = file.filename or "unknown"
    ext = Path(original_name).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS[category]:
        raise HTTPException(
            status_code=400,
            detail=f"File type '{ext}' not allowed for category '{category}'. "
            f"Allowed: {ALLOWED_EXTENSIONS[category]}",
        )

    file_type = EXTENSION_TO_TYPE.get(ext, "unknown")

    # Generate stored filename
    stored_name = f"{uuid.uuid4().hex}_{original_name}"
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    stored_path = UPLOAD_DIR / stored_name

    # Stream file to disk
    total_size = 0
    with open(stored_path, "wb") as f:
        while chunk := await file.read(1024 * 1024):  # 1 MB chunks
            total_size += len(chunk)
            if total_size > MAX_FILE_SIZE:
                stored_path.unlink(missing_ok=True)
                raise HTTPException(status_code=413, detail="File exceeds 500 MB limit")
            f.write(chunk)

    # Create DB record
    record = FileUpload(
        original_filename=original_name,
        stored_filename=stored_name,
        file_type=file_type,
        category=category,
        file_size_bytes=total_size,
        status="uploaded",
    )

    # Run geo-validation
    try:
        info = _validate_file(stored_path, file_type)
        record.crs = info.get("crs")
        record.bounds_json = info.get("bounds")
        record.metadata_json = info.get("metadata")
        record.status = "validated"
    except Exception as exc:
        record.status = "error"
        record.error_message = str(exc)

    db.add(record)
    await db.commit()
    await db.refresh(record)
    return record


@router.get("/uploads", response_model=list[FileUploadOut])
async def list_uploads(
    category: str | None = None,
    db: AsyncSession = Depends(get_db),
) -> list[FileUpload]:
    stmt = select(FileUpload).order_by(FileUpload.created_at.desc())
    if category:
        stmt = stmt.where(FileUpload.category == category)
    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.get("/uploads/{file_id}", response_model=FileUploadOut)
async def get_upload(
    file_id: int,
    db: AsyncSession = Depends(get_db),
) -> FileUpload:
    result = await db.execute(select(FileUpload).where(FileUpload.id == file_id))
    record = result.scalar_one_or_none()
    if record is None:
        raise HTTPException(status_code=404, detail="Upload not found")
    return record


@router.delete("/uploads/{file_id}", status_code=204)
async def delete_upload(
    file_id: int,
    db: AsyncSession = Depends(get_db),
) -> None:
    result = await db.execute(select(FileUpload).where(FileUpload.id == file_id))
    record = result.scalar_one_or_none()
    if record is None:
        raise HTTPException(status_code=404, detail="Upload not found")

    # Remove file from disk
    stored_path = UPLOAD_DIR / record.stored_filename
    stored_path.unlink(missing_ok=True)

    db.delete(record)
    await db.commit()
