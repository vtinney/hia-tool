## Baseline Incidence Rates

The **baseline incidence rate** (y₀) is the background rate of a health outcome in the population *before* the modeled exposure change. It determines how many cases are "available" to be attributed to air pollution.

### What is an incidence rate?

An incidence rate is the number of new cases of a disease per person per year. For example, an all-cause mortality rate of 0.008 means 8 deaths per 1,000 people per year. Rates vary by:

- **Age**: Mortality rates rise exponentially with age. A rate for ages 25+ will be lower than for ages 65+.
- **Geography**: Rates differ between countries and even between regions within a country due to demographics, healthcare access, and baseline health.
- **Time**: Rates change as disease patterns and healthcare evolve.

### Why rates matter

In the log-linear HIA equation, attributable cases = PAF × y₀ × population. The incidence rate is a direct multiplier. Using a rate that is twice the true value doubles the estimated health burden. This makes rate selection one of the most consequential choices in an HIA.

### National vs. subnational rates

**National rates** (e.g., from the GBD or WHO Global Health Estimates) are readily available for most countries and health outcomes. They provide a starting point.

**Subnational rates** (e.g., from CDC WONDER in the U.S., or national vital statistics systems) better reflect local health conditions. A state in the U.S. South may have a 20–40% higher cardiovascular mortality rate than a state in the Northeast.

When subnational rates are available, they should be preferred. When they are not, national rates are acceptable but should be noted as a limitation.

### Default rates in the CRF library

Each CRF in this tool includes a **default rate** drawn from the original study population or from global disease burden estimates. These defaults are a reasonable starting point but may not match your study population.

### References

- GBD 2019 Diseases and Injuries Collaborators (2020). Global burden of 369 diseases and injuries. *The Lancet*, 396(10258), 1204–1222.
- Krewski, D., et al. (2009). Extended follow-up and spatial analysis of the ACS study. *HEI Research Report*, 140.
