## Economic Valuation

Economic valuation translates health impacts into monetary terms, making them comparable to the costs of pollution control measures. This step is **optional** — many HIAs report only health outcomes.

### Value of a Statistical Life (VSL)

The **VSL** is the most common metric for valuing mortality risk reductions. It is *not* the value of an individual's life. Rather, it represents society's aggregate willingness to pay for a small reduction in mortality risk.

For example, if 10,000 people are each willing to pay $1,180 for a 1-in-10,000 reduction in death risk, the VSL = $1,180 × 10,000 = **$11,800,000**.

The U.S. EPA (2024) uses a central VSL of **$11.8 million** (2024 USD), based on meta-analyses of stated and revealed preference studies (Viscusi, 2018).

### OECD benefit transfer

The VSL varies with income — wealthier populations have higher willingness to pay for risk reduction. The **OECD benefit transfer** method adjusts the U.S. VSL to another country:

**VSL_country = VSL_US × (GNI_country / GNI_US)^ε**

where:
- **GNI** is gross national income per capita (PPP)
- **ε** (epsilon) is the **income elasticity** of VSL

| Elasticity | Interpretation |
|-----------|---------------|
| 0.8 | VSL rises less than proportionally with income (OECD recommendation) |
| 1.0 | VSL rises proportionally with income (standard assumption) |
| 1.2 | VSL rises more than proportionally (some meta-analyses suggest this) |

A higher elasticity produces lower VSL estimates for lower-income countries. The choice is consequential — it affects the result by 2–5× for low-income countries.

### Controversies

- **Equity concerns**: Using income-adjusted VSL implies that deaths in poorer countries are "worth less." Some analysts use a single global VSL to avoid this implication.
- **Age adjustment**: Some approaches discount VSL for older populations (VSLY — Value of a Statistical Life Year). This is technically defensible but ethically debated.
- **Morbidity valuation**: Cost-of-illness (COI) methods value hospital admissions and lost work days using direct medical costs and productivity losses. These understate true willingness to pay.

### References

- Viscusi, W. K. (2018). Pricing lives: guideposts for a safer society. *Princeton University Press*.
- OECD (2012). Mortality risk valuation in environment, health and transport policies. *OECD Publishing*.
- Robinson, L. A., et al. (2019). Valuing mortality risk reductions in global benefit-cost analysis. *Journal of Benefit-Cost Analysis*, 10(S1), 15–50.
