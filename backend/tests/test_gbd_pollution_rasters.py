"""Tests for the PM2.5 raster catalog step."""

from pathlib import Path

import pandas as pd

from backend.etl.gbd_pollution.rasters import build_raster_catalog

FIXTURES = Path(__file__).parent / "fixtures" / "gbd_pollution"


def test_build_raster_catalog_copies_and_renames(tmp_path: Path):
    processed_root = tmp_path / "processed"
    pm25_dir = processed_root / "pm25_gbd2023"

    # Stage a source raster under a fake raw filename for 2019
    raw_template = tmp_path / "raw_IHME_{year}.tif"
    fake_raw = Path(str(raw_template).replace("{year}", "2019"))
    fake_raw.write_bytes((FIXTURES / "tiny_pm25.tif").read_bytes())

    build_raster_catalog(
        years=[2019],
        raw_template=raw_template,
        output_dir=pm25_dir,
    )

    # File was copied under the new name
    copied = pm25_dir / "2019.tif"
    assert copied.exists()

    # Catalog parquet exists and has the expected columns
    catalog_path = pm25_dir / "catalog.parquet"
    assert catalog_path.exists()
    catalog = pd.read_parquet(catalog_path)
    assert set(catalog.columns) >= {
        "year", "relative_path", "crs", "pixel_size_deg",
        "nodata", "xmin", "ymin", "xmax", "ymax", "unit", "source",
    }
    row = catalog[catalog["year"] == 2019].iloc[0]
    assert row["relative_path"] == "pm25_gbd2023/2019.tif"
    assert row["crs"] == "EPSG:4326"
    assert row["unit"] == "ug_m3"
    assert abs(row["xmin"] - 100) < 1e-6
    assert abs(row["xmax"] - 110) < 1e-6


def test_build_raster_catalog_skips_missing_years(tmp_path: Path):
    processed_root = tmp_path / "processed"
    pm25_dir = processed_root / "pm25_gbd2023"
    raw_template = tmp_path / "missing_{year}.tif"  # no files exist

    build_raster_catalog(
        years=[2019, 2020],
        raw_template=raw_template,
        output_dir=pm25_dir,
    )

    catalog_path = pm25_dir / "catalog.parquet"
    assert catalog_path.exists()
    catalog = pd.read_parquet(catalog_path)
    assert len(catalog) == 0
