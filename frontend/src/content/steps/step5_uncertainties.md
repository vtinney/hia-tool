## Key Uncertainties — CRFs

### Exposure range extrapolation

Most CRFs are derived from cohorts in North America and Europe where PM2.5 levels are typically 5–30 μg/m³. Applying these to populations exposed to 50–150 μg/m³ requires extrapolation. Log-linear models extrapolate aggressively; GEMM and Fusion are designed to attenuate at high concentrations, but the evidence base is still thin above 80 μg/m³.

### Cohort transportability

CRFs reflect the exposure-response relationship in the study population. Factors like baseline health, smoking prevalence, indoor air quality, and healthcare access differ between study cohorts and your target population. This **effect modification** is rarely quantifiable.

### Confounding and model specification

Epidemiological studies adjust for confounders (smoking, income, BMI, etc.) but residual confounding always remains. Different model specifications in the same data can yield beta estimates that differ by 20–50% (Krewski et al., 2009).

### Publication bias

Meta-analyses may overrepresent positive findings. The GBD MR-BRT method includes trimming to reduce the influence of outlier studies, but publication bias cannot be fully eliminated.

### Beta coefficient uncertainty

The 95% confidence interval around beta reflects sampling uncertainty in the original study. Monte Carlo propagation in the HIA engine translates this into uncertainty on attributable cases. However, this captures only statistical uncertainty — not structural model uncertainty or transportability bias.

### Multiple CRF estimates

Different CRFs for the same endpoint can give substantially different results. For PM2.5 all-cause mortality, GEMM tends to estimate higher burdens than GBD MR-BRT at moderate concentrations. Neither is "wrong" — they reflect different modeling choices.

### References

- Burnett, R. T., & Cohen, A. (2020). Relative risk functions for estimating excess mortality. *Annual Review of Public Health*, 41, 337–353.
- Krewski, D., et al. (2009). Extended follow-up and spatial analysis of the ACS study. *HEI Research Report*, 140.
