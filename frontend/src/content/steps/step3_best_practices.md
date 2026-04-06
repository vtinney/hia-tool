## Best Practices — Population

1. **Use age-stratified data that matches your CRF age ranges.** If your CRF applies to adults 25+, use population counts for ages 25 and above. Do not apply an adult CRF to the total population.

2. **Prefer subnational population data when available.** National totals miss the spatial distribution of population density, which correlates with pollution exposure.

3. **Align population year with concentration year.** A 2020 concentration surface paired with 2010 population data introduces systematic bias in areas with population growth or decline.

4. **Use gridded population for spatial analyses.** WorldPop or GPWv4 grids allow pixel-level or fine-zone population weighting. This is more accurate than assigning district-level populations uniformly.

5. **Report the population source, year, and age breakdown.** Transparency about population inputs is essential for reproducibility and review (Anenberg et al., 2016).

6. **Check that age groups sum correctly.** When entering manual age distributions as percentages, verify they sum to 100%. The tool will flag this, but manual data entry is error-prone.

7. **Consider vulnerable subpopulations.** Children, elderly, outdoor workers, and people with pre-existing respiratory/cardiovascular disease may be more susceptible. If your CRF library includes age-specific or susceptibility-weighted functions, consider using them.

### References

- Anenberg, S. C., et al. (2016). Survey of ambient air pollution health risk assessment tools. *Risk Analysis*, 36(9), 1718–1736.
- Apte, J. S., et al. (2015). Addressing global mortality from ambient PM2.5. *Environmental Science & Technology*, 49(13), 8057–8066.
