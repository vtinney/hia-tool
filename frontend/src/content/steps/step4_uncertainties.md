## Key Uncertainties — Health Data

### Rate variability

Baseline incidence rates are averages that mask substantial variation by age, sex, socioeconomic status, and geography. Using a national all-ages rate for a specific city or age group introduces misclassification.

### Cause-of-death coding

Death certificate accuracy varies by country. In low-income countries, a large fraction of deaths are coded to "ill-defined" causes (GBD "garbage codes"), requiring statistical redistribution. This affects cause-specific incidence rates more than all-cause mortality.

### Temporal lag

Published incidence rates are typically 1–3 years behind the current year. Using a 2019 rate for a 2023 analysis assumes disease patterns have not shifted — which may not hold after events like the COVID-19 pandemic.

### Sensitivity to rate selection

The HIA result is linearly proportional to the incidence rate: doubling y₀ doubles attributable cases. In a sensitivity analysis, varying the rate by ±20% from the central estimate will show the magnitude of this effect.

### Interaction with CRF age ranges

If you enter an all-ages incidence rate but select a CRF that applies only to adults 25+, the rate-population product is misaligned. The tool's default rates attempt to match the CRF's study population, but custom rates require careful age matching.

### Under-reporting of morbidity endpoints

Mortality rates are better characterized than morbidity rates (hospital admissions, emergency visits, asthma exacerbations). Morbidity data may come from healthcare utilization databases that undercount uninsured or underserved populations.

### References

- Naghavi, M., et al. (2010). Algorithms for enhancing public health utility of national causes-of-death data. *Population Health Metrics*, 8(1), 9.
- GBD 2019 Diseases and Injuries Collaborators (2020). Global burden of 369 diseases and injuries. *The Lancet*, 396(10258), 1204–1222.
