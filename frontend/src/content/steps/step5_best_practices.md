## Best Practices — CRFs

1. **Match the CRF to your analysis context.**
   - U.S. regulatory analysis → EPA log-linear (Turner et al., 2016; Krewski et al., 2009)
   - Global burden estimation → GBD MR-BRT
   - All-cause PM2.5 mortality → GEMM (Burnett et al., 2018)
   - High-pollution settings → Fusion (Weichenthal et al., 2022)
   - European policy → HRAPIE

2. **Report results from multiple CRFs.** Running the same analysis with 2–3 different frameworks shows structural uncertainty — the variation due to modeling choices, not just statistical sampling.

3. **Check the CRF's valid concentration range.** If your baseline exceeds the maximum concentration in the CRF's source cohort, flag this in your report. Non-linear CRFs are more defensible for extrapolation.

4. **Verify the age range.** Each CRF applies to a specific age group. Applying a 30+ CRF to a 25+ population slightly overestimates cases; applying it to all ages grossly overestimates.

5. **Do not sum overlapping endpoints.** If you select CRFs for both all-cause mortality and IHD mortality, the IHD deaths are already counted in the all-cause total. Sum only mutually exclusive endpoints, or use cause-specific CRFs that together compose total mortality.

6. **Prefer meta-analyses over single studies.** CRFs from multi-city or multi-cohort meta-analyses (e.g., the 41-cohort GEMM, or GBD systematic reviews) are more generalizable than estimates from a single cohort.

7. **Consider using the default beta coefficients.** The CRF library includes peer-reviewed central estimates and confidence intervals. Override only if you have a study-specific CRF for your exact population.

### References

- Turner, M. C., et al. (2016). Long-term ozone exposure and mortality. *American Journal of Respiratory and Critical Care Medicine*, 193(10), 1134–1142.
- Burnett, R. T., et al. (2018). *PNAS*, 115(38), 9592–9597.
- Weichenthal, S., et al. (2022). *Science Advances*, 8(39).
