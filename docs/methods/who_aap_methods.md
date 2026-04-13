# WHO Ambient Air Pollution Database — Processing Methods

## Data Source

| Field | Value |
|-------|-------|
| **Dataset** | WHO Ambient Air Quality Database, Version 6.1 |
| **Release** | January 2024 |
| **Source URL** | https://www.who.int/data/gho/data/themes/air-pollution/who-air-quality-database |
| **Format** | Excel (.xlsx), single data sheet |
| **Coverage** | 7,182 cities across 123 countries, years 2010–2022 |
| **Pollutants** | PM2.5, PM10, NO2 (annual mean concentrations, µg/m³) |
| **Spatial resolution** | City-level point data (latitude/longitude of monitoring stations or city centroids) |

## Input Schema

Key columns from the WHO AAP Excel (`Update 2024 (V6.1)` sheet):

| Column | Type | Description |
|--------|------|-------------|
| `iso3` | str | ISO 3166-1 alpha-3 country code |
| `country_name` | str | Country name |
| `city` | str | City/settlement name (with country suffix, e.g. "A Coruna/ESP") |
| `year` | int | Measurement year |
| `pm25_concentration` | float | Annual mean PM2.5 (µg/m³) |
| `latitude` | float | Station/city latitude (WGS 84) |
| `longitude` | float | Station/city longitude (WGS 84) |
| `pm25_tempcov` | int | Temporal coverage of PM2.5 measurement (% of year) |
| `type_of_stations` | str | Station type (Urban, Suburban, etc.) |
| `population` | float | City population (where available) |

## Processing Pipeline

### Step 1: Filter Input Data

1. Load the Excel file, selecting the `Update 2024 (V6.1)` sheet.
2. Drop rows where `pm25_concentration` is null (only ~54% of rows have PM2.5).
3. Drop rows without valid `year`, `latitude`, or `longitude`.
4. Filter to the requested year range (default: 2015–2021).

**Year range rationale**: Data coverage is best between 2015 and 2021 (2,000–3,000 PM2.5 records per year). Years 2010–2014 have sparser PM2.5 coverage. Year 2022 has only 55 PM2.5 records and is excluded by default.

### Step 2: Create Point Geometries

Each city row is converted to a Shapely `Point(longitude, latitude)` in EPSG:4326 (WGS 84). This produces a GeoDataFrame of city monitoring points.

### Step 3: Spatial Join to Boundaries

Points are joined to three boundary sets via `sjoin(..., predicate="within")`:

#### a) Natural Earth Countries (110m)

- **Source**: Natural Earth 1:110m Admin-0 countries
- **File**: `natural_earth_gee/ne_countries/ne_countries.shp`
- **ID column**: `ISO_A3`
- **Name column**: `NAME`
- **Join method**: Point-in-polygon (no buffer)
- **Polygons**: 177 countries

#### b) Natural Earth US States (110m)

- **Source**: Natural Earth 1:110m Admin-1 states/provinces (US only)
- **File**: `natural_earth_gee/ne_states/ne_states.shp`
- **ID column**: `iso_3166_2`
- **Name column**: `name`
- **Join method**: Point-in-polygon (no buffer)
- **Polygons**: 51 US states + DC

#### c) GHS-SMOD Urban Centres (2020)

- **Source**: GHS Urban Centre Database R2023A, derived from GHS-SMOD
- **File**: `GHS_SMOD/GHS_SMOD_E2020_GLOBE_R2023A_54009_1000_UC_V2_0.shp`
- **ID column**: `ID_UC_G0`
- **Name column**: None (urban centres identified by numeric ID)
- **Join method**: Point-in-polygon with **5 km buffer**
- **Polygons**: 11,534 urban centres globally
- **CRS note**: Source is in Mollweide (ESRI:54009); reprojected to EPSG:4326 before join.

**Buffer rationale**: WHO city coordinates represent monitoring station locations or city centroids, which may fall slightly outside the compact GHS-SMOD urban boundary polygons. A 5 km buffer around each urban centre polygon captures stations in the urban fringe without merging distinct cities.

**Buffer implementation**: Polygons are projected to EPSG:3857 (Web Mercator), buffered by 5,000 metres, then reprojected back to EPSG:4326 for the spatial join. This avoids degree-based buffer distortion.

### Step 4: Aggregation

For each (boundary polygon, year) pair, we compute:

| Output field | Aggregation |
|-------------|-------------|
| `mean_pm25` | Arithmetic mean of `pm25_concentration` across all matched stations |
| `station_count` | Number of monitoring stations matched to the polygon |

This is an **unweighted** mean across stations. No population weighting or temporal-coverage weighting is applied. The `station_count` field allows downstream consumers to filter by data density (e.g., require >= 3 stations for a reliable polygon-level estimate).

### Step 5: Output

One Parquet file per (boundary type, year):

```
data/processed/who_aap/
    ne_countries/
        2015.parquet
        2016.parquet
        ...
        2021.parquet
    ne_states/
        2015.parquet
        ...
    ghs_smod/
        2015.parquet
        ...
```

## Output Schema

Each Parquet file contains these columns:

| Column | Type | Description |
|--------|------|-------------|
| `admin_id` | str | Boundary polygon identifier (ISO_A3, iso_3166_2, or GHS SMOD ID) |
| `admin_name` | str | Polygon name (country name, state name, or ID string for SMOD) |
| `mean_pm25` | float | Mean annual PM2.5 concentration (µg/m³) across matched stations |
| `station_count` | int | Number of WHO monitoring stations matched to this polygon for this year |
| `geometry` | str | Polygon geometry as WKT (from the **unbuffered** original boundary) |

## Limitations

1. **Spatial coverage is uneven.** High-income countries (Europe, North America) have far more monitoring stations than low-income countries. Many country-level polygons will have data from only 1–2 cities.

2. **Not population-weighted.** The mean PM2.5 across stations within a polygon does not account for where people live. A single rural background station has equal weight to a dense urban station. For population-weighted estimates, use the ACAG satellite-derived PM2.5 product processed via `process_pm25.py`.

3. **Temporal gaps.** Not all cities report every year. The `station_count` varies across years for the same polygon.

4. **Station representativeness.** WHO notes that measurements are "intended to reflect city/town averages rather than individual monitoring stations," but in practice the degree of spatial averaging varies by reporting country.

5. **GHS-SMOD buffer may cause double-counting.** Where urban centres are close together (< 10 km apart), the 5 km buffer can cause a station to match multiple polygons. In practice this is rare, but consumers should be aware.

6. **Year ceiling is 2022.** The V6.1 database (January 2024 release) contains data only through 2022, with extremely sparse 2022 coverage (55 PM2.5 records globally).

## CLI Reference

```bash
python backend/etl/process_who_aap.py \
    --input data/raw/who_aap/who_aap_v6.1.xlsx \
    --boundaries-dir data/raw/boundaries \
    --output-dir data/processed/who_aap \
    --years 2015-2021 \
    --verbose
```

| Flag | Default | Description |
|------|---------|-------------|
| `--input` | (required) | Path to WHO AAP Excel file |
| `--boundaries-dir` | (required) | Root of boundary subdirectories |
| `--output-dir` | (required) | Output root for Parquet files |
| `--years` | `2015-2021` | Year range (inclusive), format `YYYY-YYYY` |
| `--boundaries` | all three | Space-separated subset: `ne_countries`, `ne_states`, `ghs_smod` |
| `--verbose` / `-v` | off | Debug logging |
