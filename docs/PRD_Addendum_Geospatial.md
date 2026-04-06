# PRD Addendum: Geospatial Processing Layer

**Version:** 1.0 | April 2026
**Parent document:** HIA_Internal_Beta_Build_Plan_v2.docx
**Author:** Veronica Southerland, PhD, MPH

---

## 1. Overview

This addendum extends the HIA Walkthrough PRD to define the geospatial processing layer — the infrastructure that enables the application to accept raster concentration surfaces, gridded population data, and vector boundary files, perform zonal statistics, and produce spatially-resolved health impact results.

The existing HIA computation engine (Sprint 5) already supports NumPy array inputs via vectorized functional forms. This addendum specifies the **geospatial I/O pipeline** that transforms uploaded spatial files into the structured arrays the engine consumes, and the **spatially-tagged output format** that enables choropleth mapping and zone-level result tables.

---

## 2. Problem Statement

The current application accepts only scalar values (one concentration, one population count) entered manually. Real-world HIAs require:

- **Gridded concentration data** — GeoTIFF surfaces from satellite retrievals (van Donkelaar PM2.5, TROPOMI NO2), air quality models (CMAQ, InMAP), or spatial interpolation of monitor data
- **Gridded population data** — WorldPop, GHS-POP, GPWv4 rasters or Census tract-level tabular data
- **Study area boundaries** — shapefiles or GeoPackages defining administrative units (counties, states, municipios, districts) over which results are aggregated

Without this layer, users cannot leverage the spatial datasets identified in the PRD (Section 16) or produce the interactive choropleth maps specified in the Results dashboard (Section 7).

---

## 3. Supported File Formats

### 3.1 Raster Formats (concentration and population surfaces)

| Format | Extension(s) | Use Case |
|--------|-------------|----------|
| GeoTIFF | `.tif`, `.tiff` | Primary format for gridded data (van Donkelaar, WorldPop, CMAQ output) |
| NetCDF | `.nc` | Climate/atmospheric model output (future support) |
| CSV with coordinates | `.csv` | Monitor data or tabular spatial data |

### 3.2 Vector Formats (study area boundaries)

| Format | Extension(s) | Use Case |
|--------|-------------|----------|
| Zipped Shapefile | `.zip` | Most common exchange format for admin boundaries |
| GeoPackage | `.gpkg` | Modern single-file alternative to shapefile |
| GeoJSON | `.geojson` | Lightweight web-native format |

### 3.3 Upload Constraints

- Maximum file size: **500 MB** per file
- All uploads stored locally in `./data/uploads/` (STORAGE_BACKEND=local)
- Files tracked in SQLite via a `file_uploads` table
- UUID-prefixed filenames prevent collisions and path traversal

---

## 4. Geospatial Processing Pipeline

### 4.1 Upload and Validation

Upon file upload, the backend:

1. Saves the file to `./data/uploads/{uuid}_{filename}`
2. Creates a `FileUpload` database record
3. Performs lightweight validation:
   - **Rasters**: Opens with `rasterio`, extracts CRS, bounding box, resolution, band count, nodata value
   - **Vectors**: Opens with `geopandas`/`fiona`, extracts CRS, bounding box, feature count, geometry type, column names
4. Stores extracted metadata in the database record
5. Returns file ID and metadata to the frontend

### 4.2 Zonal Statistics

The core spatial operation is **zonal statistics** — computing summary statistics of a raster within each polygon of a boundary file. This uses the `rasterstats` library.

| Input | Statistic | Output |
|-------|-----------|--------|
| Concentration raster + boundaries | `mean` | Mean concentration per zone |
| Population raster + boundaries | `sum` | Total population per zone |

### 4.3 CRS Handling

- All inputs are reprojected to **EPSG:4326** (WGS84) as the common coordinate reference system
- `rasterstats` handles raster-vector CRS mismatch internally, but explicit alignment is preferred
- If an uploaded file lacks CRS metadata, the upload is flagged with a warning and the user is prompted to specify the CRS

### 4.4 Nodata Handling

- Raster nodata values (e.g., -9999, NaN, 0 for ocean pixels) are excluded from zonal statistics
- Zones with insufficient valid raster coverage are flagged in results
- The nodata value is auto-detected from raster metadata and exposed in the upload response

---

## 5. Spatial Compute Flow

### 5.1 Data Flow Diagram

```
┌─────────────┐    ┌─────────────┐    ┌──────────────┐
│  Step 1      │    │  Step 2      │    │  Step 3       │
│  Boundary    │    │  Concentration│    │  Population   │
│  .zip/.gpkg  │    │  .tif raster │    │  .tif raster  │
└──────┬───────┘    └──────┬───────┘    └──────┬────────┘
       │                   │                   │
       ▼                   ▼                   ▼
  POST /api/uploads   POST /api/uploads   POST /api/uploads
  (category=boundary) (category=conc.)    (category=pop.)
       │                   │                   │
       ▼                   ▼                   ▼
  FileUpload(id=1)   FileUpload(id=2)   FileUpload(id=3)
       │                   │                   │
       └───────────┬───────┴───────────────────┘
                   ▼
         POST /api/compute/spatial
         { boundaryFileId: 1,
           concentrationFileId: 2,
           populationFileId: 3,
           selectedCRFs: [...],
           monteCarloIterations: 1000 }
                   │
                   ▼
         geo_processor.prepare_spatial_inputs()
         ┌─────────────────────────────────────┐
         │ 1. Read boundary polygons            │
         │ 2. Zonal stats: mean(conc) per zone  │
         │ 3. Zonal stats: sum(pop) per zone    │
         │ 4. Return arrays + geometries        │
         └──────────────────┬──────────────────┘
                            ▼
         hia_engine.compute_hia({
           baselineConcentration: [12.3, 15.1, 8.7, ...],  # n_zones
           population: [50000, 120000, 35000, ...],          # n_zones
           selectedCRFs: [...],
         })
         ┌─────────────────────────────────────┐
         │ beta[n_iter] × delta_c[n_zones]     │
         │ → broadcasts to [n_iter, n_zones]   │
         │ → _summarise_spatial() per zone     │
         └──────────────────┬──────────────────┘
                            ▼
         SpatialComputeResponse {
           zones: [{ zoneId, geometry, results }],
           aggregate: { population-weighted totals },
           totalDeaths: { mean, lower95, upper95 }
         }
                            │
                            ▼
         Frontend: MapBox choropleth + zone table
```

### 5.2 Scalar vs. Spatial Routing

The application preserves two compute paths:

| Mode | Trigger | Engine | Endpoint |
|------|---------|--------|----------|
| **Scalar** | Manual entry (no uploadIds) | Client-side JS `computeHIA()` | None (local) |
| **Spatial** | File uploads present (uploadIds set) | Backend Python `compute_hia()` | `POST /api/compute/spatial` |

Step 6 (Run Analysis) detects which path to use based on whether `uploadId` values exist in the Zustand store.

### 5.3 ProcessPoolExecutor

Spatial analyses with large rasters are CPU-bound. Following the PRD's TASK_BACKEND=sync design, heavy computation runs in a `ProcessPoolExecutor` (max 2 workers) to avoid blocking the async event loop. No Celery is required during beta.

---

## 6. Spatial Results Format

### 6.1 Per-Zone Results

Each zone (polygon) in the boundary file receives its own set of HIA results:

```json
{
  "zoneId": "06037",
  "zoneName": "Los Angeles County",
  "geometry": { "type": "Polygon", "coordinates": [...] },
  "baselineConcentration": 12.3,
  "controlConcentration": 5.0,
  "population": 10014009,
  "results": [
    {
      "crfId": "epa_pm25_acm_adult",
      "endpoint": "All-cause mortality",
      "attributableCases": { "mean": 4521, "lower95": 3102, "upper95": 5940 },
      "attributableFraction": { "mean": 0.042, "lower95": 0.029, "upper95": 0.055 },
      "attributableRate": { "mean": 45.1, "lower95": 31.0, "upper95": 59.3 }
    }
  ]
}
```

### 6.2 Aggregate Results

A population-weighted aggregate is computed across all zones for summary cards:

- Total attributable cases = sum across zones
- Population-weighted mean PAF
- Total economic value (when Step 7 valuation is enabled)

### 6.3 Frontend Visualization

- **Choropleth map**: MapBox GL JS renders zones colored by attributable cases, PAF, or rate
- **Sortable zone table**: One row per zone with all CRF results
- **Export**: CSV includes zone IDs and all results; GeoJSON export includes geometries

---

## 7. Database Schema Addition

### file_uploads Table

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment ID |
| user_id | VARCHAR(64) | Default "local" |
| original_filename | VARCHAR(512) | User's original filename |
| stored_filename | VARCHAR(512) | UUID-prefixed stored name |
| file_type | VARCHAR(32) | "geotiff", "shapefile", "geopackage", "csv" |
| category | VARCHAR(32) | "concentration", "population", "boundary" |
| file_size_bytes | INTEGER | File size |
| crs | VARCHAR(128) | e.g., "EPSG:4326" |
| bounds_json | JSON | {west, south, east, north} |
| metadata_json | JSON | Band count, resolution, feature count, etc. |
| status | VARCHAR(32) | "uploaded", "validated", "error" |
| error_message | TEXT | Null unless status = "error" |
| created_at | DATETIME | Auto-set |

---

## 8. API Endpoints

### 8.1 File Upload

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/uploads` | Upload file (multipart/form-data: file + category) |
| GET | `/api/uploads` | List uploads (filter by ?category=) |
| GET | `/api/uploads/{id}` | Get single upload metadata |
| DELETE | `/api/uploads/{id}` | Delete upload and file |

### 8.2 Spatial Compute

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/compute/spatial` | Run spatially-resolved HIA computation |

---

## 9. Technology Stack Additions

| Package | Version | Purpose |
|---------|---------|---------|
| `rasterio` | >=1.3.10 | GeoTIFF reading, CRS handling |
| `geopandas` | >=0.14.4 | Vector I/O, spatial operations |
| `rasterstats` | >=0.19.0 | Zonal statistics (raster × vector overlay) |
| `fiona` | >=1.9.6 | Low-level vector file I/O (used by geopandas) |
| `shapely` | >=2.0.4 | Geometry operations |
| `pyproj` | >=3.6.1 | CRS transformations |
| `python-multipart` | >=0.0.9 | FastAPI multipart/form-data upload support |

**Windows installation**: `rasterio` and `fiona` ship pre-built wheels on PyPI for Python 3.9–3.12. If pip installation fails, use `conda install -c conda-forge rasterio geopandas rasterstats`.

---

## 10. Sprint Placement

This work spans two existing sprints in the PRD roadmap:

| Sprint | Weeks | Geospatial Additions |
|--------|-------|---------------------|
| **5** (HIA computation engine) | 9–10 | `_summarise_spatial()` in engine, `/api/compute/spatial` endpoint |
| **6** (Run + Valuation) | 11–12 | File upload pipeline, `geo_processor.py`, frontend upload integration |
| **8** (Data pipeline + built-in datasets) | 15–16 | Pre-processed rasters for priority countries, auto-load in Steps 2–3 |

---

## 11. Transition Path

| Component | Internal Beta | External Production |
|-----------|--------------|-------------------|
| File storage | `./data/uploads/` (local) | S3 with presigned upload URLs |
| Spatial queries | GeoPandas + rasterstats (in-memory) | PostGIS for vector, COG + STAC for rasters |
| Large rasters | ProcessPoolExecutor | Celery workers with higher memory |

No code changes needed for the beta — the same `STORAGE_BACKEND` env var pattern from the main PRD applies.
