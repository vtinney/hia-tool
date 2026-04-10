"""Raster catalog step: copy PM2.5 TIFs and write a catalog parquet."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

import pandas as pd
import rasterio

logger = logging.getLogger("gbd_pollution.rasters")


def _inspect_raster(path: Path) -> dict:
    """Open a raster and extract metadata for the catalog row."""
    with rasterio.open(path) as src:
        b = src.bounds
        transform = src.transform
        pixel_size = abs(transform.a)
        return {
            "crs": str(src.crs),
            "pixel_size_deg": float(pixel_size),
            "nodata": float(src.nodata) if src.nodata is not None else float("nan"),
            "xmin": float(b.left),
            "ymin": float(b.bottom),
            "xmax": float(b.right),
            "ymax": float(b.top),
        }


def build_raster_catalog(
    *,
    years: list[int],
    raw_template: Path,
    output_dir: Path,
) -> None:
    """Copy PM2.5 rasters from the raw template to the processed dir,
    rename to ``{year}.tif``, and write a catalog parquet describing
    each one.

    Years whose source file is missing are silently skipped — the
    catalog simply omits that row.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []
    for year in years:
        src = Path(str(raw_template).replace("{year}", str(year)))
        if not src.exists():
            logger.warning("source raster missing for year %d: %s", year, src)
            continue
        dst = output_dir / f"{year}.tif"
        shutil.copy2(src, dst)

        meta = _inspect_raster(dst)
        rows.append({
            "year": int(year),
            "relative_path": f"{output_dir.name}/{year}.tif",
            "unit": "ug_m3",
            "source": "IHME GBD 2023",
            **meta,
        })

    cat = pd.DataFrame(rows, columns=[
        "year", "relative_path", "crs", "pixel_size_deg", "nodata",
        "xmin", "ymin", "xmax", "ymax", "unit", "source",
    ])
    cat["year"] = cat["year"].astype("int16") if not cat.empty else cat["year"]
    cat["unit"] = cat["unit"].astype("category") if not cat.empty else cat["unit"]

    catalog_path = output_dir / "catalog.parquet"
    cat.to_parquet(catalog_path, index=False)
    logger.info("wrote raster catalog with %d rows to %s", len(cat), catalog_path)
