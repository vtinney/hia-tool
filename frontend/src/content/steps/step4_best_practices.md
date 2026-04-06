## Best Practices — Health Data

1. **Use local rates when available.** Subnational vital statistics or hospital discharge data that match your study area are preferable to national averages (Fann et al., 2012).

2. **Match rate age groups to CRF age ranges.** If the CRF is for ages 30+, use the mortality rate for ages 30+ — not the all-ages rate. The default rates in the CRF library are pre-matched; override only when you have better local data.

3. **Use cause-specific rates for cause-specific CRFs.** The GBD MR-BRT framework provides CRFs for specific causes (IHD, stroke, COPD, lung cancer, LRI, diabetes). Pair each with its corresponding cause-specific incidence rate.

4. **For all-cause mortality, use all-cause rates.** GEMM and some EPA CRFs estimate all-cause (non-accidental) mortality. Use the non-accidental death rate, not total mortality that includes accidents.

5. **Run a sensitivity analysis on rates.** Vary incidence rates by ±20% and report the range of attributable cases. This quantifies the impact of rate uncertainty (Anenberg et al., 2010).

6. **Document your rate source.** Report the data source, year, age range, and geographic level. Common sources include:
   - GBD Results Tool (ghdx.healthdata.org)
   - CDC WONDER (wonder.cdc.gov)
   - WHO Global Health Estimates
   - National vital statistics offices

7. **Consider the baseline health status of your population.** Populations with higher baseline disease rates will show larger absolute health burdens from the same exposure change, even if the relative risk is identical.

### References

- Fann, N., et al. (2012). Estimating the national public health burden associated with exposure to ambient PM2.5 and ozone. *Risk Analysis*, 32(1), 81–95.
- Anenberg, S. C., et al. (2010). An estimate of the global burden of anthropogenic ozone and fine particulate matter. *Environmental Health Perspectives*, 118(9), 1189–1195.
