# Methods — Population-Weighted PM2.5 Exposure Estimates

*Source material for the HIA tool methods section. Written in prose; cite and adapt as needed.*

## Data sources

**Ambient PM2.5.** Annual mean surface PM2.5 concentrations were obtained from the Atmospheric Composition Analysis Group's global satellite-derived product (van Donkelaar et al., V5.GL.04), accessed through Google Earth Engine at ~0.01° (≈1 km) spatial resolution. Years 2015 through the most recent available year (2022 at time of processing) were used.

**Population.** Gridded population counts were taken from the sat-io community mirror of the WorldPop age- and sex-structured product (`projects/sat-io/open-datasets/WORLDPOP/agesex`) at 100 m resolution, which provides sex- and age-disaggregated bands in 5-year bins from 0 to 90+ (20 age bins per sex). This mirror provides annual estimates from 2015 through 2030, so the full 2015–2022 PM2.5 period is covered without any population carry-forward. Total population at each pixel is computed as the sum over all 20 age bands for both sexes, which is numerically identical to the WorldPop total population surface since the age bands cover 100% of the population by construction. The actual population source year is retained in the output as `pop_source_year` for traceability.

**Administrative and urban boundaries.** Three families of boundary geometries were used, all pre-uploaded to Earth Engine as feature collections:

1. Natural Earth countries (1:10m)
2. Natural Earth states and provinces (1:10m)
3. GHS-SMOD urban areas, vectorized with one feature per city

Global Administrative Areas (GADM) boundaries were initially considered but were deferred from this pass because of integration complexity.

## Grid harmonization

To combine PM2.5 and population on a common grid while preserving total population counts, WorldPop's 100 m rasters were aggregated to the PM2.5 grid using Earth Engine's `reduceResolution` with a **sum** reducer, then reprojected to the PM2.5 projection. The sum reducer is essential: a mean reducer would preserve density but destroy counts, corrupting any downstream weighting. Each resampled WorldPop age bin was computed as the sum of the corresponding male and female bands (e.g., `age_25 = m_25 + f_25`) prior to aggregation, yielding 20 age-specific population surfaces. Total population per pixel was then computed as the sum across the 20 age bands, giving a single pop_total surface on the PM2.5 grid.

## Population-weighted PM2.5

Within each boundary feature, the population-weighted annual PM2.5 concentration was computed as:

```
PM2.5_popweighted = Σ_i (PM2.5_i · pop_i) / Σ_i pop_i
```

where `i` indexes grid cells whose centroid falls within the feature. Numerically, this was implemented in Earth Engine by constructing a per-pixel product image `pm25 × pop_total`, then using `reduceRegions` with a **sum** reducer to accumulate both the numerator (`pm25 × pop_total`) and the denominator (`pop_total`) over each feature, taking the ratio in a post-processing step. An unweighted spatial mean of PM2.5 within each feature was computed separately via a second `reduceRegions` call with a mean reducer, and is retained as `pm25_mean` for comparison.

For each feature, population counts for the 20 age bins were also summed within the feature and reported alongside the exposure columns, so that downstream health-impact calculations can apply age-specific concentration-response functions without re-reducing the source grids.

## Output

Results are written as long-format Parquet files, one per boundary set, with columns for feature identifiers, year, total and age-structured population, unweighted and population-weighted PM2.5, and the WorldPop source year used. This mirrors the standard ingestion format of the HIA tool's data pipeline.

## Known caveats

- **Boundary assignment.** `reduceRegions` assigns a pixel to a feature based on pixel coverage; small features near the ~1 km PM2.5 grid scale may be represented by only a handful of pixels, increasing variance in their weighted means.
- **Grid alignment.** Because population is aggregated to the PM2.5 grid rather than PM2.5 being disaggregated to population, the spatial resolution of weighting is effectively the PM2.5 grid (~1 km), not 100 m.
- **Dataset version.** Results are tied to Van Donkelaar V5.GL.04 and to the sat-io mirror of WorldPop age-sex as retrieved from Earth Engine at processing time; re-running against newer versions or against the Google-maintained `WorldPop/GP/100m/pop_age_sex` (which at processing time contained only 2020) will shift absolute values.

## References

- van Donkelaar, A., et al. *Global Annual PM2.5 Grids from MODIS, MISR and SeaWiFS Aerosol Optical Depth (AOD) with GWR, V5.GL.04.* Atmospheric Composition Analysis Group, Washington University in St. Louis.
- Tatem, A. J. *WorldPop, open data for spatial demography.* Scientific Data, 2017.
- GHSL — Global Human Settlement Layer, European Commission Joint Research Centre.
- Natural Earth — 1:10m Cultural Vectors.
