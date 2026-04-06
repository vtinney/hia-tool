## Air Quality Data

This step defines the **exposure contrast** — the difference between current air quality (baseline) and a hypothetical cleaner scenario (control). This delta drives the entire HIA calculation.

### Satellite vs. monitor data

**Ground monitors** (e.g., EPA AQS, EEA AirBase) provide direct measurements at point locations. They are accurate where available but have sparse spatial coverage, especially in low- and middle-income countries.

**Satellite-derived surfaces** (e.g., van Donkelaar ACAG V5, Hammer et al. 2020) provide global gridded estimates by combining satellite aerosol optical depth with chemical transport models and ground calibration. They offer complete spatial coverage but have greater uncertainty at the pixel level, particularly in arid or biomass-burning regions.

For most global or national HIAs, satellite-derived surfaces are the practical choice. For U.S. city-level analyses, EPA AQS monitor data or fused products (e.g., EPA downscaler) may be preferred.

### The counterfactual scenario

The **control** (or counterfactual) represents the air quality you are comparing against. Common approaches:

- **Policy benchmark**: A regulatory standard (e.g., WHO guideline of 5 μg/m³, US NAAQS of 9 μg/m³)
- **Rollback**: A percentage reduction from the baseline (e.g., 20% reduction)
- **Alternative scenario**: A modeled future scenario (e.g., post-regulation concentrations)
- **Theoretical minimum**: The TMREL used by the GBD (2.4–5.9 μg/m³ for PM2.5)

The choice of counterfactual is often the most influential assumption in an HIA.

### Rollback methods

A **proportional rollback** reduces all concentrations by the same percentage. This is simple but may be unrealistic — regulatory interventions typically affect high-pollution areas more than clean areas. An **absolute rollback** subtracts a fixed amount, which may produce negative values in clean areas.

### References

- Hammer, M. S., et al. (2020). Global estimates and long-term trends of fine particulate matter concentrations. *Environmental Science & Technology*, 54(13), 7879–7890.
- Anenberg, S. C., et al. (2010). An estimate of the global burden of anthropogenic ozone and fine particulate matter. *Environmental Health Perspectives*, 118(9), 1189–1195.
