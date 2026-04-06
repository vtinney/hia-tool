## Defining Your Study Area

A health impact assessment starts by defining **where**, **what**, and **when** you are studying.

### Geographic scale

The scale of your study area directly determines the precision of your results. City-level analyses can use local monitoring data and census tracts, while national analyses rely on modeled surfaces and broader administrative units.

Smaller areas allow better exposure characterization but may have too few health events for stable incidence rates. Larger areas provide statistical stability but mask spatial variation in both exposure and vulnerability.

### Pollutant selection

Your pollutant choice determines which concentration-response functions (CRFs) are available downstream:

- **PM2.5** has the most extensive CRF library — all five frameworks (EPA, GBD, GEMM, Fusion, HRAPIE) provide estimates.
- **Ozone** has well-established short-term CRFs and emerging chronic CRFs from the EPA and GBD.
- **NO₂** has growing epidemiological evidence but fewer validated long-term CRFs.
- **SO₂** has primarily short-term exposure CRFs.

### Year selection

Choose years that align with available data:

- **Concentration data** may be available annually (satellite products) or for specific monitoring periods.
- **Population data** anchors to census years (e.g., 2020 for the U.S.) and is interpolated for other years.
- **Incidence rates** from the GBD are available through 2021; national vital statistics may lag 1–2 years.

Using mismatched years introduces temporal mismatch uncertainty. When possible, align all data to the same year or use overlapping multi-year averages.

### References

- Burnett, R. T., et al. (2018). Global estimates of mortality associated with long-term exposure to outdoor fine particulate matter. *PNAS*, 115(38), 9592–9597.
- Cohen, A. J., et al. (2017). Estimates and 25-year trends of the global burden of disease attributable to ambient air pollution. *The Lancet*, 389(10082), 1907–1918.
