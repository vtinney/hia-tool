## Concentration-Response Functions

A **concentration-response function** (CRF) quantifies the relationship between a change in air pollutant concentration and a change in health risk. It is the epidemiological heart of any HIA.

### The five CRF frameworks

#### 1. EPA Log-Linear
The standard BenMAP approach: RR = exp(β × ΔC). Simple, well-validated for U.S. regulatory analyses. Assumes a **linear** relationship between log-relative-risk and concentration — reasonable at typical U.S. levels (5–20 μg/m³) but may underestimate risk attenuation at high concentrations.

**Best for**: U.S. policy analyses, regulatory impact assessments.

#### 2. GBD MR-BRT (Meta-Regression — Bayesian, Regularised, Trimmed)
The IHME's non-linear spline model. Provides **cause-specific** CRFs (IHD, stroke, COPD, lung cancer, LRI, type 2 diabetes). The gold standard for Global Burden of Disease analyses. Captures the flattening of risk at high concentrations.

**Best for**: Global or national burden assessments, cause-specific attribution.

#### 3. GEMM (Global Exposure Mortality Model)
Burnett et al. (2018) fitted a shape-constrained hazard function to **41 cohorts** worldwide. Covers all non-accidental mortality from PM2.5. The non-linear shape handles both low and high concentration ranges.

**Best for**: All-cause PM2.5 mortality in diverse settings, particularly when cause-specific data is limited.

#### 4. Fusion
Weichenthal et al. (2022) developed a hybrid model combining cohort evidence with toxicological constraints. It has the best **extrapolation properties** at high concentrations (>50 μg/m³), where other models have little data.

**Best for**: High-pollution settings (South Asia, sub-Saharan Africa) where extrapolation beyond cohort data is necessary.

#### 5. HRAPIE
The WHO European consensus CRFs. Log-linear, recommended for European policy analyses. Includes morbidity endpoints (hospital admissions, bronchitis, work days lost) not covered by other frameworks.

**Best for**: European policy, morbidity-inclusive analyses.

### Linear vs. non-linear debate

At concentrations below ~30 μg/m³, log-linear and non-linear models produce similar results. Above that, they diverge: log-linear models predict ever-increasing risk, while non-linear models (GEMM, MR-BRT, Fusion) show risk attenuation. The non-linear models are generally considered more biologically plausible.

### References

- Burnett, R. T., et al. (2018). Global estimates of mortality. *PNAS*, 115(38), 9592–9597.
- Weichenthal, S., et al. (2022). The Fusion risk function. *Science Advances*, 8(39).
- Zheng, P., et al. (2022). The Burden of Proof framework for GBD 2020. *Nature Medicine*, 28, 2064–2072.
- WHO (2013). HRAPIE project: recommendations for CRFs. WHO Europe.
