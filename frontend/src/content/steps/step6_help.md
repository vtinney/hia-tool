## Running the Analysis

This step configures how the HIA computation is executed: how to combine multiple CRFs and how to quantify uncertainty.

### Pooling methods

When you select multiple CRFs for the same endpoint, you need to decide how to combine them:

- **Fixed effects**: Assumes all CRFs estimate the same true effect. Computes a precision-weighted average. Produces narrow confidence intervals. Appropriate when CRFs are from similar populations and study designs.

- **Random effects**: Assumes CRFs estimate different but related true effects (i.e., there is heterogeneity). Produces wider confidence intervals that account for between-study variation. More conservative and generally preferred when combining CRFs from different frameworks or populations (DerSimonian & Laird, 1986).

- **Run separately**: Does not pool. Reports each CRF's result independently. Useful for comparing frameworks or when CRFs address different endpoints.

### Monte Carlo uncertainty propagation

The **beta coefficient** (log-relative-risk) in each CRF has a confidence interval reflecting statistical uncertainty from the original study. Monte Carlo simulation samples from this distribution to propagate uncertainty into the final result.

For each iteration:
1. Sample β from N(β̂, SE²)
2. Compute attributable cases using the sampled β
3. Repeat N times
4. Report the mean and 2.5th/97.5th percentiles of the distribution

### How many iterations?

| Iterations | Use case |
|-----------|----------|
| 100 | Quick preview, testing |
| 500 | Reasonable precision for most analyses |
| 1,000 | Standard for publication-quality results |
| 5,000 | High precision, stable percentile estimates |

The percentile estimates (95% CI bounds) stabilize more slowly than the mean. For a presentation or quick look, 500 is sufficient. For a report or publication, use 1,000 or more.

### What the results mean

The output reports:
- **Attributable cases**: Excess health events caused by the exposure above the counterfactual
- **Attributable fraction (PAF)**: The proportion of total cases attributable to the exposure
- **Attributable rate**: Cases per 100,000 population, for comparability across populations

### References

- DerSimonian, R., & Laird, N. (1986). Meta-analysis in clinical trials. *Controlled Clinical Trials*, 7(3), 177–188.
- Fann, N., et al. (2012). Estimating the national public health burden. *Risk Analysis*, 32(1), 81–95.
