## Key Uncertainties — Analysis Execution

### Statistical vs. structural uncertainty

Monte Carlo propagation quantifies **statistical uncertainty** — the precision of the beta estimate from the original epidemiological study. It does not capture **structural uncertainty**: the choice of CRF framework, functional form (linear vs. non-linear), or confounding adjustment.

To characterize structural uncertainty, compare results across multiple CRF frameworks. The spread between EPA log-linear, GEMM, and GBD MR-BRT estimates for the same inputs often exceeds the 95% CI from any single CRF.

### Pooling assumption sensitivity

Fixed-effects pooling assumes homogeneity and will produce artificially narrow confidence intervals if the underlying CRFs are heterogeneous. Random-effects pooling is more robust but can be dominated by imprecise studies with wide confidence intervals.

### Correlation between CRFs

Monte Carlo sampling treats each CRF independently — betas are drawn from separate distributions. In reality, CRFs estimated from overlapping cohort populations may be correlated. The current approach slightly overestimates total uncertainty when CRFs are positively correlated and underestimates it when they are negatively correlated.

### Linearity of summing across endpoints

Summing attributable cases across multiple cause-specific endpoints assumes the health effects are additive and non-overlapping. If a death is attributable to both IHD and COPD pathways, simple summation may double-count.

### Normal approximation for beta

The Monte Carlo engine assumes β ~ N(β̂, SE²). For very small studies or extreme effect sizes, the true sampling distribution may be skewed. For the well-powered cohort studies in the CRF library, the normal approximation is generally adequate.

### References

- Burnett, R. T., & Cohen, A. (2020). Relative risk functions for estimating excess mortality. *Annual Review of Public Health*, 41, 337–353.
- DerSimonian, R., & Laird, N. (1986). *Controlled Clinical Trials*, 7(3), 177–188.
