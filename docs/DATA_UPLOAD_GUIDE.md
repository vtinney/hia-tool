# HIA Tool — Comprehensive Dataset Upload Guide

**Version:** 1.0 | April 2026

This guide catalogs every dataset the HIA tool needs, where each one goes, what format it must be in, and what preprocessing is required before upload.

---

## Storage Architecture

All data lives under `./data/` (the `STORAGE_PATH` env var). The structure is:

```
data/
├── hia.db                          # SQLite metadata database
├── uploads/                        # Raw user-uploaded files (UUID-prefixed)
│   ├── {uuid}_{filename}.tif
│   ├── {uuid}_{filename}.zip
│   └── ...
└── processed/                      # Pre-processed built-in datasets (DATA_ROOT)
    ├── pm25/
    │   └── {country}/
    │       └── {year}.parquet
    ├── ozone/
    │   └── {country}/
    │       └── {year}.parquet
    ├── no2/
    │   └── {country}/
    │       └── {year}.parquet
    ├── so2/
    │   └── {country}/
    │       └── {year}.parquet
    ├── population/
    │   └── {country}/
    │       └── {year}.parquet
    └── incidence/
        └── {country}/
            └── {cause}/
                └── {year}.parquet
```

**Migration to AWS:** When `STORAGE_BACKEND=s3`, uploads go to S3 with presigned URLs. The `processed/` directory structure maps 1:1 to S3 key prefixes. Change `DATABASE_URL` to PostgreSQL at the same time.

---

## I. Air Quality / Concentration Datasets

These populate the **Step 2 (Air Quality)** built-in dropdown and the `data/processed/{pollutant}/{country}/{year}.parquet` files.

### 1. van Donkelaar / ACAG V5.GL.03 — Satellite-derived PM2.5

| Field | Value |
|-------|-------|
| **App ID** | `acag_v5` |
| **Source** | Washington University Atmospheric Composition Analysis Group |
| **URL** | https://sites.wustl.edu/acag/datasets/surface-pm2-5/ |
| **Raw format** | NetCDF (`.nc`) or GeoTIFF (`.tif`), ~0.01° resolution (~1 km) |
| **Coverage** | Global, annual composites (1998–2021) |
| **Target path** | `data/processed/pm25/{country}/{year}.parquet` |
| **Processing required** | Run ETL: clip to country boundary, zonal stats (mean per admin unit), output Parquet |
| **ETL command** | `python backend/etl/process_pm25.py --input raw/V5GL03.HybridPM25.Global.202001-202012.nc --boundaries boundaries/{country}_admin1.geojson --output data/processed/pm25/{country}/2020.parquet` |

**Parquet schema:**
```
admin_id        (str)   — Admin unit identifier
admin_name      (str)   — Admin unit name
mean_pm25       (float) — Mean annual PM2.5 (μg/m³)
pixel_count     (int)   — Number of valid raster pixels in zone
geometry        (str)   — WKT polygon geometry
```

### 2. GBD 2019 — Global PM2.5 Estimates

| Field | Value |
|-------|-------|
| **App ID** | `gbd2019` |
| **Source** | IHME Global Burden of Disease study |
| **URL** | https://ghdx.healthdata.org/ |
| **Raw format** | GeoTIFF or tabular CSV |
| **Coverage** | Global, annual |
| **Target path** | `data/processed/pm25/{country}/{year}.parquet` |
| **Processing** | Same ETL as ACAG — zonal stats against admin boundaries |

### 3. WHO Ambient Air Pollution Database 2024

| Field | Value |
|-------|-------|
| **App ID** | `who_aap_2024` |
| **Source** | WHO |
| **URL** | https://www.who.int/data/gho/data/themes/air-pollution |
| **Raw format** | CSV (city-level monitoring data with lat/lon) |
| **Coverage** | Global cities, annual averages |
| **Target path** | `data/processed/pm25/{country}/{year}.parquet` |
| **Processing** | Join city coordinates to admin boundaries, assign mean concentration per admin unit, output Parquet with same schema |

### 4. US EPA AQS Monitor Data

| Field | Value |
|-------|-------|
| **App ID** | `epa_aqs` |
| **Source** | US EPA Air Quality System |
| **URL** | https://aqs.epa.gov/aqsweb/airdata/download_files.html |
| **Raw format** | CSV (monitor-level, annual summaries) |
| **Coverage** | US only, multi-pollutant |
| **Target path** | `data/processed/{pollutant}/us/{year}.parquet` |
| **Processing** | Spatial join monitor locations to county/state boundaries, compute area-weighted means per admin unit |

### 5. CAMS Global Reanalysis (EAC4)

| Field | Value |
|-------|-------|
| **App ID** | `cams_reanalysis` |
| **Source** | Copernicus Atmosphere Monitoring Service |
| **URL** | https://ads.atmosphere.copernicus.eu/ |
| **Raw format** | NetCDF (`.nc`), global gridded |
| **Coverage** | Global, multi-pollutant (PM2.5, O3, NO2, SO2) |
| **Target path** | `data/processed/{pollutant}/{country}/{year}.parquet` |
| **Processing** | Select pollutant variable from NetCDF, compute annual mean, zonal stats per admin unit |

### 6. OpenAQ Aggregated Data

| Field | Value |
|-------|-------|
| **App ID** | `openaq` |
| **Source** | OpenAQ Foundation |
| **URL** | https://openaq.org/ (API or bulk download) |
| **Raw format** | JSON/CSV (station-level measurements with coordinates) |
| **Coverage** | Global monitoring stations |
| **Target path** | `data/processed/{pollutant}/{country}/{year}.parquet` |
| **Processing** | Aggregate to annual means per station, spatial join to admin boundaries |

### 7. TROPOMI NO2 (Sentinel-5P)

| Field | Value |
|-------|-------|
| **App ID** | N/A (upload only for now) |
| **Source** | ESA Sentinel-5P satellite |
| **URL** | https://s5phub.copernicus.eu/ |
| **Raw format** | NetCDF (Level 2/3 products), GeoTIFF processed versions |
| **Coverage** | Global, daily → annual composites |
| **Target path** | `data/processed/no2/{country}/{year}.parquet` |
| **Processing** | Same ETL pattern — zonal stats of tropospheric NO2 column |

---

## II. Population Datasets

These populate **Step 3 (Population)** built-in dropdown and `data/processed/population/{country}/{year}.parquet`.

### 8. WorldPop — Gridded Age/Sex Population Estimates (REQUIRES PREPROCESSING)

| Field | Value |
|-------|-------|
| **App ID** | `worldpop_2020` |
| **Source** | WorldPop, University of Southampton |
| **URL** | https://www.worldpop.org/geodata/listing?id=65 |
| **Raw format** | GeoTIFF — **separate files per age/sex group** |
| **Coverage** | Global, ~100m resolution, annual |
| **Target path** | `data/processed/population/{country}/{year}.parquet` |

**This is the most complex dataset to prepare.** WorldPop distributes population as individual GeoTIFFs per age group and sex. You must combine them.

#### WorldPop File Naming Convention (raw downloads):
```
{country}_{sex}_{agelow}_{agehigh}_{year}.tif

Examples:
mex_f_0_2020.tif       (Mexico, female, age 0)
mex_f_1_2020.tif       (Mexico, female, age 1)
mex_f_5_2020.tif       (Mexico, female, age 5-9)
mex_f_10_2020.tif      (Mexico, female, age 10-14)
mex_m_0_2020.tif       (Mexico, male, age 0)
mex_m_1_2020.tif       (Mexico, male, age 1)
...
```

#### Required Preprocessing — Combine Male + Female into 17 Age Bins:

The app expects **17 five-year age bins** (matching the `AGE_BINS` constant in Step3Population.jsx):

```
0–4, 5–9, 10–14, 15–19, 20–24, 25–29, 30–34, 35–39,
40–44, 45–49, 50–54, 55–59, 60–64, 65–69, 70–74, 75–79, 80+
```

**Step-by-step process:**

1. **Download** all age/sex GeoTIFFs for the country and year
2. **For each age bin** (e.g., 0–4), sum the male + female rasters:
   - `age_0_4 = m_0 + m_1 + m_2 + m_3 + m_4 + f_0 + f_1 + f_2 + f_3 + f_4`
   - (Some WorldPop releases use single-year ages 0–4, others use grouped 0 + 1 + 5-year bins)
3. **Compute total population** raster: sum all age bins
4. **Run zonal statistics** against admin boundaries (sum per zone for each age bin)
5. **Output Parquet** with the required schema

**Parquet schema for population:**
```
admin_id        (str)   — Admin unit identifier
admin_name      (str)   — Admin unit name
total           (int)   — Total population in zone
age_0_4         (int)   — Population ages 0–4
age_5_9         (int)   — Population ages 5–9
age_10_14       (int)   — Population ages 10–14
age_15_19       (int)   — Population ages 15–19
age_20_24       (int)   — Population ages 20–24
age_25_29       (int)   — Population ages 25–29
age_30_34       (int)   — Population ages 30–34
age_35_39       (int)   — Population ages 35–39
age_40_44       (int)   — Population ages 40–44
age_45_49       (int)   — Population ages 45–49
age_50_54       (int)   — Population ages 50–54
age_55_59       (int)   — Population ages 55–59
age_60_64       (int)   — Population ages 60–64
age_65_69       (int)   — Population ages 65–69
age_70_74       (int)   — Population ages 70–74
age_75_79       (int)   — Population ages 75–79
age_80_plus     (int)   — Population ages 80+
```

**Important:** The column names MUST start with `age_` prefix — the backend (`data.py:169`) detects age group columns by filtering `c.startswith("age_")`. The frontend converts `age_0_4` → `0–4` for display.

#### Alternative: Upload as GeoTIFF

Users can also upload a single combined WorldPop GeoTIFF directly through the Step 3 file upload. The backend runs zonal stats (sum) on it. However, this gives only total population per zone — no age breakdown unless you pre-stack age bins into multi-band GeoTIFFs.

### 9. GPWv4 — Gridded Population of the World

| Field | Value |
|-------|-------|
| **App ID** | `gpw_v4` |
| **Source** | CIESIN, Columbia University |
| **URL** | https://sedac.ciesin.columbia.edu/data/collection/gpw-v4 |
| **Raw format** | GeoTIFF or NetCDF, ~1 km resolution |
| **Coverage** | Global, every 5 years (2000, 2005, 2010, 2015, 2020) |
| **Target path** | `data/processed/population/{country}/{year}.parquet` |
| **Processing** | Zonal stats (sum) against admin boundaries. No age breakdown available — use alongside a separate age distribution source (e.g., UN WPP) |

### 10. US Census ACS 5-Year Estimates

| Field | Value |
|-------|-------|
| **App ID** | `census_acs` |
| **Source** | US Census Bureau |
| **URL** | https://data.census.gov/ |
| **Raw format** | CSV (tabular, by tract/county/state with GEOID) |
| **Coverage** | US only, annual rolling 5-year estimates |
| **Target path** | `data/processed/population/us/{year}.parquet` |
| **Processing** | Already tabular — map Census age brackets to the 17 standard bins, join by GEOID to admin boundaries |

### 11. UN World Population Prospects 2022

| Field | Value |
|-------|-------|
| **App ID** | `un_wpp_2022` |
| **Source** | United Nations Population Division |
| **URL** | https://population.un.org/wpp/Download/ |
| **Raw format** | CSV/Excel (national-level age/sex estimates) |
| **Coverage** | 237 countries, 5-year intervals (1950–2100 projections) |
| **Target path** | `data/processed/population/{country}/{year}.parquet` |
| **Processing** | National-level only (no sub-national). Map to 17 age bins. Useful as age distribution percentages when combined with a spatially-resolved total from GPWv4 or WorldPop |

---

## III. Health Incidence Rate Datasets

These populate **Step 4 (Health Data)** built-in dropdown and `data/processed/incidence/{country}/{cause}/{year}.parquet`.

### 12. GBD 2019/2023 — Baseline Incidence & Mortality Rates

| Field | Value |
|-------|-------|
| **App ID** | `gbd2019_rates` |
| **Source** | IHME GBD Results Tool |
| **URL** | https://vizhub.healthdata.org/gbd-results/ |
| **Raw format** | CSV (downloaded from GBD Results Tool) |
| **Coverage** | Global, by country, age, sex, cause, year |
| **Target path** | `data/processed/incidence/{country}/{cause}/{year}.parquet` |

**Required causes** (matching CRF library endpoints):
- `all_cause_mortality` — All-cause mortality (ACM)
- `ihd` — Ischemic heart disease
- `stroke` — Stroke (cerebrovascular disease)
- `lung_cancer` — Tracheal, bronchus, lung cancer
- `copd` — Chronic obstructive pulmonary disease
- `lri` — Lower respiratory infections
- `dm2` — Type 2 diabetes mellitus
- `asthma` — Asthma (incidence, for NO2 pediatric CRF)

**Parquet schema for incidence:**
```
admin_id        (str)   — Admin unit ID (country ISO or sub-national)
admin_name      (str)   — Admin unit name
incidence_rate  (float) — Baseline rate (deaths or cases per person-year)
cause           (str)   — Cause slug (matches directory name)
age_group       (str)   — Age range (optional, for age-specific rates)
```

**Processing:** Download from GBD Results Tool → filter by location, cause, age, metric (rate) → normalize to per-person-year → save as Parquet in the correct directory.

### 13. BenMAP-CE Default Health Incidence Rates

| Field | Value |
|-------|-------|
| **App ID** | `benmap_rates` |
| **Source** | US EPA BenMAP-CE software package |
| **URL** | https://www.epa.gov/benmap |
| **Raw format** | CSV or database export from BenMAP |
| **Coverage** | US only, county-level |
| **Target path** | `data/processed/incidence/us/{cause}/{year}.parquet` |
| **Processing** | Export from BenMAP, reformat columns to match schema above |

### 14. WHO Global Health Estimates 2020

| Field | Value |
|-------|-------|
| **App ID** | `who_ghe_2020` |
| **Source** | WHO |
| **URL** | https://www.who.int/data/global-health-estimates |
| **Raw format** | Excel/CSV (national-level, by cause and age group) |
| **Coverage** | Global, national level |
| **Target path** | `data/processed/incidence/{country}/{cause}/{year}.parquet` |
| **Processing** | Download cause-specific sheets, compute rates from counts ÷ population, output Parquet |

### 15. CDC WONDER Mortality Data

| Field | Value |
|-------|-------|
| **App ID** | `cdc_wonder` |
| **Source** | CDC |
| **URL** | https://wonder.cdc.gov/ |
| **Raw format** | Tab-delimited text (query results) |
| **Coverage** | US only, by state/county, cause (ICD-10), age |
| **Target path** | `data/processed/incidence/us/{cause}/{year}.parquet` |
| **Processing** | Query by relevant ICD-10 codes, reformat to Parquet schema |

---

## IV. Concentration-Response Functions (CRF) Data

The CRF library is already embedded in the app at `frontend/src/data/crf-library.json`. However, some functional forms require additional tabulated data:

### 16. GBD 2023 MR-BRT Spline Tables

| Field | Value |
|-------|-------|
| **What** | Tabulated relative risk (RR) values at specific concentration knot points |
| **Source** | IHME MR-BRT (meta-regression—Bayesian, regularized, trimmed) |
| **Why needed** | CRFs with `"functionalForm": "mr-brt"` use spline interpolation rather than a single beta coefficient. The beta/betaLow/betaHigh in crf-library.json are linear approximations. Full MR-BRT implementation requires the spline lookup table |
| **Format needed** | JSON array embedded in crf-library.json or a separate `mr-brt-splines.json` |
| **Schema** | `{ crfId: "gbd_pm25_acm_adult", knots: [{ concentration: 5.0, rr: 1.00, rrLow: 1.00, rrHigh: 1.00 }, { concentration: 10.0, rr: 1.08, ... }, ...] }` |
| **Target path** | `frontend/src/data/mr-brt-splines.json` (frontend) or `data/processed/crf/mr-brt-splines.json` (backend) |

**Affects these CRF IDs:** `gbd_pm25_acm_adult`, `gbd_pm25_ihd`, `gbd_pm25_stroke`, `gbd_pm25_lc`, `gbd_pm25_copd`, `gbd_pm25_lri`, `gbd_pm25_dm2`, `gbd_ozone_copd_mort`, `gbd_no2_asthma_child`

### 17. GEMM Coefficients (Burnett et al. 2018)

| Field | Value |
|-------|-------|
| **What** | Sigmoid shape parameters (θ, α, μ, ν, τ) for GEMM no-linear-threshold model |
| **Source** | Burnett et al. 2018 supplementary materials |
| **Format** | Already partially embedded via beta values. Full GEMM implementation needs the 5-parameter set per cause |
| **Affects** | `gemm_pm25_acm`, `gemm_pm25_cvd`, `gemm_pm25_resp` |

### 18. Fusion Hybrid Coefficients (Weichenthal et al. 2022)

| Field | Value |
|-------|-------|
| **What** | Marginal hazard ratio curve for trapezoidal integration |
| **Source** | Weichenthal et al. 2022 supplementary tables |
| **Affects** | `fusion_pm25_acm`, `fusion_pm25_cvd`, `fusion_pm25_lc` |

---

## V. Boundary / Administrative Datasets

These are used in **Step 1 (Study Area)** and as the zonal geometries for spatial analysis.

### 19. Natural Earth Administrative Boundaries

| Field | Value |
|-------|-------|
| **Already in app** | Yes — `frontend/public/data/ne_countries_raw.geojson` and `ne_states_raw.geojson` |
| **Source** | Natural Earth (1:110m) |
| **URL** | https://www.naturalearthdata.com/ |
| **Format** | GeoJSON (already converted) |
| **Download script** | `scripts/download_boundaries.py` |
| **Usage** | Country/state selection in Step 1 map, fallback boundaries for zonal stats |

### 20. GADM Administrative Boundaries (recommended for sub-national)

| Field | Value |
|-------|-------|
| **Not yet in app** | Recommended addition for finer admin levels |
| **Source** | GADM (Database of Global Administrative Areas) |
| **URL** | https://gadm.org/download_world.html |
| **Raw format** | GeoPackage (`.gpkg`) or Shapefile (`.zip`) |
| **Coverage** | Global, admin levels 0–4 |
| **Target path** | Upload via Step 1 boundary upload, or pre-stage in `data/processed/boundaries/` |
| **Format requirement** | Must have CRS (EPSG:4326 preferred), features with ID and name attributes |

---

## VI. Economic Valuation Data

Used in **Step 7 (Valuation)**.

### 21. World Bank GNI Data

| Field | Value |
|-------|-------|
| **Already in app** | Yes — `frontend/src/data/world-bank-gni.json` |
| **Source** | World Bank |
| **Usage** | VSL transfer calculations using income elasticity |
| **Update frequency** | Annual — update the JSON file when new WB data releases |

---

## VII. Geospatial Format Requirements

All geospatial data that enters the app (whether uploaded or pre-processed) must meet these requirements:

### Raster Requirements (GeoTIFF)
- **CRS:** EPSG:4326 (WGS84) — the app auto-reprojects, but EPSG:4326 is preferred
- **Format:** GeoTIFF (`.tif` / `.tiff`) — primary supported format
- **Nodata:** Must have a defined nodata value in metadata (e.g., -9999, NaN)
- **Bands:** Single band for concentration; single band per age group for population
- **Size limit:** 500 MB per file (upload endpoint)
- **Resolution:** Any (the app handles via zonal stats), but ~1km or finer recommended

### Vector Requirements (Boundaries)
- **CRS:** EPSG:4326 preferred (auto-reprojected if different)
- **Format:** Zipped Shapefile (`.zip`), GeoPackage (`.gpkg`), or GeoJSON (`.geojson`)
- **Required attributes:** At least one ID column and one name column. The backend uses heuristic detection looking for: `GEOID`, `ADM1_CODE`, `ADM0_A3`, `ISO_A3`, `admin_id`, `id`, `code` (for ID) and `NAME`, `admin_name`, `name` (for name)
- **Geometry type:** Polygon or MultiPolygon
- **Size limit:** 500 MB

### Processed Parquet Requirements
- **Engine:** PyArrow (the backend reads with `engine="pyarrow"`)
- **Required columns vary by type** — see schemas in sections above
- **Column names:** Use snake_case; age columns MUST be prefixed with `age_`
- **Geometry column:** WKT string format in a column named `geometry`

---

## VIII. Quick Reference — Processing Priority

| Priority | Dataset | Complexity | Why First |
|----------|---------|-----------|-----------|
| 1 | **WorldPop age/sex** | HIGH — multi-file merge | Core to all spatial analyses; age bins needed for CRF matching |
| 2 | **ACAG V5 PM2.5** | MEDIUM — ETL exists | Most common pollutant; ETL script already written |
| 3 | **GBD incidence rates** | MEDIUM — manual download | Needed for any mortality/morbidity calculation |
| 4 | **MR-BRT spline tables** | MEDIUM — data extraction | Required for accurate GBD 2023 CRFs (currently using linear approx) |
| 5 | **Admin boundaries (GADM)** | LOW — download + stage | Higher resolution than Natural Earth for sub-national work |
| 6 | **WHO/EPA/OpenAQ concentration** | LOW — varies | Supplementary data sources |
| 7 | **Census ACS / CDC WONDER** | LOW — US-specific | Only needed for US county-level analyses |
| 8 | **GEMM/Fusion coefficients** | LOW — literature | Only needed if using those specific CRF frameworks |

---

## IX. WorldPop Preprocessing Script (Template)

Below is a Python script outline for combining WorldPop age/sex rasters. This would go in `backend/etl/process_worldpop.py`:

```python
"""
Combine WorldPop age/sex GeoTIFFs into a single Parquet with 17 age bins.

Usage:
    python process_worldpop.py \
        --input-dir data/raw/worldpop/mex_2020/ \
        --boundaries data/boundaries/mexico_admin1.geojson \
        --output data/processed/population/mexico/2020.parquet

Input directory should contain files like:
    mex_f_0_2020.tif, mex_f_1_2020.tif, mex_f_5_2020.tif, ...
    mex_m_0_2020.tif, mex_m_1_2020.tif, mex_m_5_2020.tif, ...
"""

import argparse
from pathlib import Path
import rasterio
import numpy as np
import geopandas as gpd
from rasterstats import zonal_stats
import pandas as pd

# Map WorldPop individual files to HIA 5-year bins
# Adjust this mapping based on the specific WorldPop product downloaded
AGE_BIN_MAP = {
    'age_0_4':    ['0', '1', '2', '3', '4'],     # or ['0', '1'] for grouped
    'age_5_9':    ['5'],                           # already 5-year bin
    'age_10_14':  ['10'],
    'age_15_19':  ['15'],
    'age_20_24':  ['20'],
    'age_25_29':  ['25'],
    'age_30_34':  ['30'],
    'age_35_39':  ['35'],
    'age_40_44':  ['40'],
    'age_45_49':  ['45'],
    'age_50_54':  ['50'],
    'age_55_59':  ['55'],
    'age_60_64':  ['60'],
    'age_65_69':  ['65'],
    'age_70_74':  ['70'],
    'age_75_79':  ['75'],
    'age_80_plus': ['80'],
}

def find_rasters(input_dir, age_codes, sexes=('f', 'm')):
    """Find all raster files matching age codes for both sexes."""
    paths = []
    for sex in sexes:
        for code in age_codes:
            pattern = f"*_{sex}_{code}_*.tif"
            matches = list(Path(input_dir).glob(pattern))
            paths.extend(matches)
    return paths

def sum_rasters(raster_paths):
    """Sum multiple aligned rasters into one array."""
    result = None
    profile = None
    for path in raster_paths:
        with rasterio.open(path) as src:
            data = src.read(1, masked=True)
            if result is None:
                result = data.copy()
                profile = src.profile.copy()
            else:
                result += data
    return result, profile

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-dir', required=True)
    parser.add_argument('--boundaries', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()

    boundaries = gpd.read_file(args.boundaries)
    if boundaries.crs and boundaries.crs.to_epsg() != 4326:
        boundaries = boundaries.to_crs(epsg=4326)

    # Detect ID and name columns
    id_col = next((c for c in boundaries.columns
                   if c.lower() in ('geoid', 'adm1_code', 'admin_id', 'id', 'code', 'iso_a3')), None)
    name_col = next((c for c in boundaries.columns
                     if c.lower() in ('name', 'admin_name', 'adm1_name')), None)

    records = []
    for idx, row in boundaries.iterrows():
        record = {
            'admin_id': row[id_col] if id_col else str(idx),
            'admin_name': row[name_col] if name_col else f'Zone {idx}',
            'geometry': row.geometry.wkt,
        }
        records.append(record)

    # For each age bin, sum M+F rasters, run zonal stats
    for bin_name, age_codes in AGE_BIN_MAP.items():
        raster_paths = find_rasters(args.input_dir, age_codes)
        if not raster_paths:
            print(f"  WARNING: No rasters found for {bin_name}")
            for rec in records:
                rec[bin_name] = 0
            continue

        combined, profile = sum_rasters(raster_paths)

        # Write temporary combined raster for zonal stats
        import tempfile, os
        tmp = tempfile.NamedTemporaryFile(suffix='.tif', delete=False)
        tmp.close()
        with rasterio.open(tmp.name, 'w', **profile) as dst:
            dst.write(combined.filled(profile.get('nodata', -9999)), 1)

        stats = zonal_stats(
            boundaries, tmp.name, stats=['sum'],
            nodata=profile.get('nodata', -9999)
        )
        os.unlink(tmp.name)

        for i, s in enumerate(stats):
            records[i][bin_name] = int(s['sum'] or 0)

    # Compute total
    age_cols = [k for k in AGE_BIN_MAP.keys()]
    for rec in records:
        rec['total'] = sum(rec.get(c, 0) for c in age_cols)

    df = pd.DataFrame(records)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.output, engine='pyarrow', index=False)
    print(f"Wrote {len(df)} zones to {args.output}")

if __name__ == '__main__':
    main()
```

---

## X. CSV Upload Formats (for manual/file uploads in the UI)

If users prefer CSV upload instead of built-in datasets:

### Population CSV (Step 3)
Required columns: `spatial_unit_id`, `age_group`, `population`
```csv
spatial_unit_id,age_group,population
adm1_001,0–4,50000
adm1_001,5–9,48000
adm1_001,80+,12000
adm1_002,0–4,35000
...
```

### Health Incidence CSV (Step 4)
Required columns: `endpoint`, `age_group`, `rate`
```csv
endpoint,age_group,rate
All-cause mortality,25–99,0.008
Ischemic heart disease,25–99,0.0025
Lung cancer,25–99,0.0006
COPD,25–99,0.0005
Lower respiratory infection,0–4,0.0004
```

### Concentration CSV (Step 2)
Can be uploaded as GeoTIFF or CSV with coordinate columns.

---

## XI. Preprocessing Workflow — How to Use These Scripts

### Directory Structure (already created)

Raw data goes in `data/raw/`, organized by source. Processed output lands in `data/processed/`.

```
data/
├── raw/                              # YOU download files here
│   ├── worldpop/                     # WorldPop age/sex GeoTIFFs
│   │   └── {country}_{year}/         # e.g., mex_2020/ with all .tif files
│   ├── acag/                         # ACAG V5 PM2.5 surfaces
│   │   └── V5GL03_PM25_{year}.nc     # or .tif
│   ├── gbd/                          # GBD Results Tool CSV exports
│   │   └── ihme_gbd_{query}.csv
│   ├── boundaries/                   # Admin boundary files
│   │   └── {country}_admin1.geojson  # or .gpkg / .zip
│   ├── who_aap/                      # WHO Ambient Air Pollution DB
│   ├── epa_aqs/                      # US EPA AQS monitor exports
│   ├── cams/                         # CAMS reanalysis NetCDFs
│   ├── openaq/                       # OpenAQ bulk downloads
│   ├── tropomi/                      # TROPOMI NO2 products
│   ├── census_acs/                   # US Census ACS tables
│   ├── cdc_wonder/                   # CDC WONDER query results
│   ├── benmap/                       # BenMAP-CE exports
│   ├── who_ghe/                      # WHO Global Health Estimates
│   └── mr_brt/                       # MR-BRT spline data from IHME
│
├── uploads/                          # App-managed (UUID-prefixed user uploads)
│
└── processed/                        # ETL OUTPUT — app reads from here
    ├── pm25/{country}/{year}.parquet
    ├── ozone/{country}/{year}.parquet
    ├── no2/{country}/{year}.parquet
    ├── so2/{country}/{year}.parquet
    ├── population/{country}/{year}.parquet
    ├── incidence/{country}/{cause}/{year}.parquet
    ├── crf/                          # MR-BRT spline tables, GEMM params
    │   └── mr-brt-splines.json
    └── boundaries/                   # Pre-staged admin boundaries
        └── {country}_admin{level}.geojson
```

### ETL Scripts (in `backend/etl/`)

```
backend/etl/
├── __init__.py                  # already exists
├── process_pm25.py              # already exists — concentration rasters
├── process_worldpop.py          # combines WorldPop age/sex → Parquet
├── process_gbd_incidence.py     # GBD CSV → per-cause Parquet
└── process_boundaries.py        # GADM/boundary cleanup → GeoJSON
```

### Step-by-Step Workflow

**You do:**
1. Download raw data from the source website
2. Drop the files into the appropriate `data/raw/{source}/` folder
3. Tell Claude what you downloaded and for which country/year

**Claude does:**
1. Writes or updates the ETL script in `backend/etl/`
2. Runs the script against your raw data + boundaries
3. Outputs Parquet to the correct `data/processed/` path
4. Verifies the output (row counts, column names, value ranges)

### What Claude Needs From You Per Dataset

| Dataset | Tell Claude |
|---------|------------|
| **WorldPop** | Country, year, and the file naming pattern in the folder (run `ls data/raw/worldpop/{folder}/` to show it) |
| **ACAG PM2.5** | File format (NetCDF or GeoTIFF), variable name if NetCDF |
| **GBD incidence** | Which causes/locations you queried, show first few lines of the CSV |
| **Boundaries** | Admin level (1=states, 2=counties), source (GADM, national agency) |
| **EPA AQS** | Which pollutant(s), which annual summary file |
| **Census ACS** | Which table IDs, geography level |

### Recommended Order (one country end-to-end first)

1. **Boundaries** — download admin boundaries for your test country → `data/raw/boundaries/`
2. **ACAG PM2.5** — download concentration surface → Claude runs existing `process_pm25.py`
3. **WorldPop** — download all age/sex rasters → Claude runs `process_worldpop.py`
4. **GBD incidence** — download from IHME Results Tool → Claude runs `process_gbd_incidence.py`
5. **Test in app** — verify Steps 2, 3, 4 all load the built-in data
6. **Repeat** for additional countries

---

## Summary Checklist

- [ ] Download and process **ACAG V5 PM2.5** GeoTIFFs → Parquet (ETL exists)
- [ ] Download and combine **WorldPop age/sex** GeoTIFFs → Parquet (script template above)
- [ ] Download **GBD 2019/2023 incidence rates** from IHME → Parquet (per country/cause)
- [ ] Extract **MR-BRT spline tables** from GBD 2023 → JSON for crf-library
- [ ] Optionally download **GADM boundaries** for sub-national admin levels
- [ ] Optionally download **WHO AAP, EPA AQS, CAMS, OpenAQ** concentration data
- [ ] Optionally download **Census ACS, CDC WONDER** for US-specific analyses
- [ ] Optionally extract **GEMM and Fusion** model coefficients from literature
- [ ] Update **World Bank GNI** JSON if newer data available
- [ ] Verify all Parquet files use correct column names (`age_` prefix, `admin_id`, etc.)
- [ ] Verify all GeoTIFFs have defined CRS and nodata values
