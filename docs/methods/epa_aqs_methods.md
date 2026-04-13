# US EPA AQS Monitor Data — Processing Methods

## Data Source

| Field | Value |
|-------|-------|
| **Dataset** | EPA Air Quality System (AQS) Annual Concentration by Monitor |
| **Source** | US Environmental Protection Agency |
| **URL** | https://aqs.epa.gov/aqsweb/airdata/download_files.html |
| **Format** | Zipped CSV, one file per year (`annual_conc_by_monitor_YYYY.zip`) |
| **Coverage** | US + territories, 2015–2024 |
| **Pollutants** | PM2.5, Ozone, NO2, SO2 |
| **Spatial resolution** | Monitor-level point data (latitude/longitude) |

## Pollutant Selection

Each AQS annual file contains hundreds of parameters (criteria pollutants, speciated PM2.5, meteorology, VOCs, metals, etc.). We extract four criteria pollutants using specific parameter codes and sample durations to get one canonical annual mean per monitor:

| Pollutant | Parameter Code | Sample Duration | Units | Output column |
|-----------|---------------|-----------------|-------|---------------|
| **PM2.5** | 88101 | 24 HOUR | µg/m³ (LC) | `mean_pm25` |
| **Ozone** | 44201 | 8-HR RUN AVG BEGIN HOUR | ppm | `mean_ozone` |
| **NO2** | 42602 | 1 HOUR | ppb | `mean_no2` |
| **SO2** | 42401 | 1 HOUR | ppb | `mean_so2` |

### Parameter code rationale

- **88101** (PM2.5 - Local Conditions): The standard FRM/FEM PM2.5 mass concentration used for NAAQS compliance. "Local Conditions" means reported at ambient temperature and pressure, the standard EPA reporting convention.
- **44201** (Ozone): The only ozone parameter code. The 8-hour running average is the metric used for the current NAAQS standard (2015).
- **42602** (Nitrogen dioxide): Standard NO2 parameter. The 1-hour mean is used for the annual summary.
- **42401** (Sulfur dioxide): Standard SO2 parameter using the 1-hour measurement. Code 42403 (SO2 max 5-min avg) is excluded.

### Deduplication

Each monitor can have multiple rows for the same (parameter code, sample duration) combination due to different **Pollutant Standards** (e.g., "PM25 24-hour 1997", "PM25 24-hour 2006", "PM25 24-hour 2012", "PM25 24-hour 2024"). The `Arithmetic Mean` is identical across standards for the same monitor — only the exceedance counts differ. We keep the first row per unique (State Code, County Code, Site Num, POC) tuple.

## Processing Pipeline

### Step 1: Load and Filter

For each year in the range:

1. Open the ZIP file, locate the CSV inside (some ZIPs nest the CSV in a subdirectory).
2. Read the CSV with `low_memory=False` to handle mixed-type columns.
3. Filter to rows matching the target parameter code and sample duration.
4. Deduplicate by (State Code, County Code, Site Num, POC), keeping first row.
5. Drop rows with missing coordinates or `Arithmetic Mean`.

### Step 2: Create Point Geometries

Each monitor is converted to a Shapely `Point(Longitude, Latitude)` in EPSG:4326.

### Step 3: Spatial Join to Boundaries

Monitor points are joined to three boundary sets (same as the WHO AAP pipeline):

#### a) Natural Earth Countries (110m)

- **ID**: `ISO_A3` — produces 2–4 rows per year (USA, CAN, PRI, VIR depending on monitor locations)
- **Join**: Point-in-polygon, no buffer
- Low utility for EPA data since nearly all monitors are US.

#### b) Natural Earth US States (110m)

- **ID**: `iso_3166_2` (e.g., `US-CA`)
- **Name**: `name` (e.g., `California`)
- **Join**: Point-in-polygon, no buffer
- Produces 49–51 state rows per year depending on monitor deployment.

#### c) GHS-SMOD Urban Centres (2020)

- **ID**: `ID_UC_G0` (numeric)
- **Join**: Point-in-polygon with **5 km buffer**
- Captures ~55% of monitors; the remaining 45% are rural/suburban monitors outside GHS urban centre boundaries.

### Step 4: Aggregation

For each (boundary polygon, year) pair:

| Output field | Aggregation |
|-------------|-------------|
| `mean_{pollutant}` | Arithmetic mean of `Arithmetic Mean` across matched monitors |
| `station_count` | Number of monitors matched |

This is an **unweighted** mean across monitors within each polygon.

### Step 5: Output

One Parquet file per (pollutant, boundary type, year):

```
data/processed/epa_aqs/
    pm25/
        ne_countries/{year}.parquet
        ne_states/{year}.parquet
        ghs_smod/{year}.parquet
    ozone/
        ne_countries/{year}.parquet
        ne_states/{year}.parquet
        ghs_smod/{year}.parquet
    no2/
        ...
    so2/
        ...
```

## Output Schema

Each Parquet file contains:

| Column | Type | Description |
|--------|------|-------------|
| `admin_id` | str | Boundary polygon ID (ISO_A3, iso_3166_2, or GHS SMOD ID) |
| `admin_name` | str | Polygon name (country, state, or ID string for SMOD) |
| `mean_{pollutant}` | float | Mean annual concentration across matched monitors |
| `station_count` | int | Number of AQS monitors matched to this polygon for this year |
| `geometry` | str | Polygon geometry as WKT (from unbuffered original boundary) |

The `mean_{pollutant}` column name varies by pollutant: `mean_pm25`, `mean_ozone`, `mean_no2`, `mean_so2`.

## Data Coverage Summary (2015–2024)

| Pollutant | Monitors/year | NE States coverage | GHS SMOD match rate |
|-----------|--------------|-------------------|-------------------|
| PM2.5 | 533–961 | 50–51 states | ~55% of monitors |
| Ozone | 1,267–1,327 | 51 states | ~32% of monitors |
| NO2 | 455–483 | 49–50 states | ~63% of monitors |
| SO2 | 394–511 | 51 states | ~39% of monitors |

Monitor counts decline from 2015 to 2024 for PM2.5 (961 → 533) as EPA retires older FRM samplers. Ozone, NO2, and SO2 monitor counts are more stable.

## Limitations

1. **Unweighted mean across monitors.** States with more urban monitors will have higher-biased PM2.5 means. This is not the same as a population-weighted or area-weighted average. For population-weighted estimates, use the ACAG satellite-derived product.

2. **Monitor siting bias.** EPA monitors are required near population centres and potential NAAQS violations, so they over-represent urban and industrial areas. Rural concentrations are undersampled.

3. **PM2.5 monitor retirement.** The 24-hour FRM PM2.5 monitor network has been shrinking. 2024 has ~55% as many monitors as 2015. The continuous FEM network (parameter 88502) is growing but uses a different measurement method.

4. **Ozone units are ppm, not ppb.** EPA reports ozone in parts per million. Multiply by 1000 to convert to ppb for comparison with WHO guidelines (which use µg/m³ — further conversion needed).

5. **SO2 includes non-US territories.** Some monitors are in Puerto Rico and the US Virgin Islands, which match to NE country polygons other than USA (PRI, VIR).

6. **GHS SMOD match rate varies by pollutant.** Ozone monitors are often placed in rural/suburban areas (to measure regional transport), so fewer match urban centre polygons.

## CLI Reference

```bash
python backend/etl/process_epa_aqs.py \
    --input-dir data/raw/epa_aqs \
    --boundaries-dir data/raw/boundaries \
    --output-dir data/processed/epa_aqs \
    --years 2015-2024 \
    --verbose
```

| Flag | Default | Description |
|------|---------|-------------|
| `--input-dir` | (required) | Directory with `annual_conc_by_monitor_YYYY.zip` files |
| `--boundaries-dir` | (required) | Root of boundary subdirectories |
| `--output-dir` | (required) | Output root for Parquet files |
| `--years` | `2015-2024` | Year range (inclusive) |
| `--pollutants` | all four | Subset: `pm25`, `ozone`, `no2`, `so2` |
| `--boundaries` | all three | Subset: `ne_countries`, `ne_states`, `ghs_smod` |
| `--verbose` / `-v` | off | Debug logging |
