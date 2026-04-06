## Best Practices — Air Quality

1. **Report data source and year explicitly.** Whether you use ACAG V5.GL.03, EPA AQS, or WHO AAP data, document the source, version, and temporal coverage.

2. **Use population-weighted concentrations.** A simple area-weighted mean overrepresents rural areas. Weight by gridded population to reflect actual exposure (Apte et al., 2015).

3. **Run sensitivity analyses on the counterfactual.** Report results under at least two control scenarios (e.g., WHO guideline and a regulatory standard) to show how sensitive your findings are to this choice.

4. **Prefer 3–5 year averages for chronic analyses.** This smooths inter-annual meteorological variability and better represents long-term exposure.

5. **Check your data range against CRF validity.** If your baseline concentrations exceed the range studied in your chosen CRF cohort, the extrapolation is uncertain. Non-linear CRFs (GEMM, Fusion) handle high-concentration extrapolation better than log-linear models.

6. **Consider using multiple data sources.** If both satellite and monitor data are available, compare them. Agreement builds confidence; divergence flags a data quality issue.

7. **For rollback scenarios, justify the percentage.** Cite the regulation or intervention that would achieve the modeled reduction. Unrealistic rollbacks undermine policy credibility.

### References

- Apte, J. S., et al. (2015). Addressing global mortality from ambient PM2.5. *Environmental Science & Technology*, 49(13), 8057–8066.
- Brauer, M., et al. (2016). Ambient air pollution exposure estimation for the Global Burden of Disease 2013. *Environmental Science & Technology*, 50(1), 79–88.
