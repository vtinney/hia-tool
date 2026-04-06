## Best Practices — Analysis Execution

1. **Use random effects pooling by default.** Unless you have a specific reason to assume homogeneity (e.g., all CRFs are from the same meta-analysis), random effects is the safer choice. It accounts for between-study variation.

2. **Run at least 1,000 Monte Carlo iterations for reported results.** The mean stabilizes quickly, but the 2.5th and 97.5th percentiles need more samples. Use 100–500 for exploratory runs and 1,000+ for final results.

3. **Report results from multiple CRF frameworks.** Present a table showing attributable cases under EPA, GBD, and GEMM (or whichever frameworks are relevant). The spread characterizes structural uncertainty.

4. **Do not sum across overlapping endpoints.** If you report all-cause mortality from GEMM, do not also add IHD + stroke + COPD + lung cancer from GBD. These are alternative decompositions of the same deaths.

5. **Review your summary before running.** The Step 6 review panel shows all inputs. Check that:
   - The concentration delta is positive (baseline > control)
   - Population is in the correct order of magnitude
   - CRFs match the selected pollutant
   - Incidence rates are per-person-per-year (not per 100,000)

6. **Save your configuration as a template.** This allows you to re-run the analysis with updated data or share the setup with collaborators for review.

7. **Run a one-at-a-time sensitivity analysis.** Vary baseline concentration, incidence rate, and CRF choice individually to identify which input drives the most variation in results.

### References

- Fann, N., et al. (2012). Estimating the national public health burden. *Risk Analysis*, 32(1), 81–95.
- WHO (2016). Health risk assessment of air pollution: general principles. WHO Regional Office for Europe.
