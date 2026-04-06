## Population Data

The exposed population is a direct multiplier in the HIA equation: attributable cases = PAF × incidence rate × population. Getting the right population — particularly the right **age distribution** — is critical.

### Why age-specific population matters

Most CRFs are age-restricted. The landmark Krewski et al. (2009) study enrolled adults aged 30+. The GBD estimates separate CRFs for children under 5 (for LRI) and adults 25+ (for NCD causes). Applying a 25+ CRF to the total population would overestimate attributable cases by including children who were not in the study cohort.

The age distribution also matters because mortality rates rise steeply with age. A population with a large elderly share will have more attributable deaths per unit exposure change than a younger population, even at the same total size.

### Population data sources

- **Census data** (e.g., U.S. Census ACS, national statistical offices) provides the most reliable age-sex-geography breakdown but may be outdated between census years.
- **WorldPop** provides annual gridded population estimates at ~100m resolution, modeled from census data, satellite imagery, and covariates. Good for spatial analyses in LMICs.
- **GPWv4** (Gridded Population of the World) provides simpler proportional allocation of census data to grid cells.
- **UN World Population Prospects** provides national-level age-sex projections. Useful for multi-year or future-scenario analyses.

### When to use each source

| Source | Best for | Limitation |
|--------|----------|------------|
| Census | City/subnational analyses in data-rich countries | Intercensal years require interpolation |
| WorldPop | Gridded spatial analyses, LMICs | Modeled, not directly observed |
| UN WPP | National totals, trend analyses | No subnational detail |

### References

- Krewski, D., et al. (2009). Extended follow-up and spatial analysis of the ACS study. *Health Effects Institute Research Report*, 140.
- Tatem, A. J. (2017). WorldPop, open data for spatial demography. *Scientific Data*, 4, 170004.
