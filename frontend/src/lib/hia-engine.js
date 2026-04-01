/**
 * HIA Engine — Health Impact Assessment computation module.
 *
 * Implements the core epidemiological calculations that convert a change in
 * air-quality concentration into attributable health outcomes.  Four
 * concentration-response functional forms are supported:
 *
 *   1. Log-linear (EPA / HRAPIE standard)
 *   2. MR-BRT spline (GBD 2023) — placeholder with log-linear fallback
 *   3. GEMM SCHIF (Global Exposure Mortality Model, Burnett et al. 2018)
 *   4. Fusion hybrid (Weichenthal et al. 2022) — placeholder with
 *      trapezoidal integration scaffolding
 *
 * Uncertainty is propagated via Monte Carlo sampling of the beta (log-RR)
 * parameter, assumed normally distributed with mean = beta_hat and
 * SE derived from the 95 % CI bounds in the CRF library.
 *
 * @module hia-engine
 */

// ────────────────────────────────────────────────────────────────────
//  RNG — Box-Muller transform for normal random variates
// ────────────────────────────────────────────────────────────────────

/**
 * Generate a standard-normal random variate using the Box-Muller transform.
 * @returns {number} A sample from N(0, 1).
 */
function randn() {
  let u = 0
  let v = 0
  while (u === 0) u = Math.random()
  while (v === 0) v = Math.random()
  return Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v)
}

/**
 * Sample from N(mu, sigma²).
 * @param {number} mu    - Mean.
 * @param {number} sigma - Standard deviation (>0).
 * @returns {number}
 */
function sampleNormal(mu, sigma) {
  return mu + sigma * randn()
}

// ────────────────────────────────────────────────────────────────────
//  Helpers
// ────────────────────────────────────────────────────────────────────

/**
 * Derive the standard error of beta from the 95 % CI.
 *
 * The CRF library stores betaLow and betaHigh, which represent the 2.5th
 * and 97.5th percentiles of the beta distribution.  Under a normal
 * assumption the width of the 95 % CI equals 2 × 1.96 × SE.
 *
 * @param {number} betaLow  - Lower bound of 95 % CI.
 * @param {number} betaHigh - Upper bound of 95 % CI.
 * @returns {number} Estimated SE of beta.
 */
function betaSE(betaLow, betaHigh) {
  return (betaHigh - betaLow) / (2 * 1.96)
}

/**
 * Compute summary statistics (mean, 2.5th, 97.5th percentile) from an
 * array of Monte Carlo samples.
 *
 * @param {number[]} samples - Array of numeric samples.
 * @returns {{ mean: number, lower95: number, upper95: number }}
 */
function summarise(samples) {
  const n = samples.length
  if (n === 0) return { mean: 0, lower95: 0, upper95: 0 }

  const sorted = Float64Array.from(samples).sort()
  const mean = samples.reduce((a, b) => a + b, 0) / n
  const lower95 = sorted[Math.floor(n * 0.025)]
  const upper95 = sorted[Math.floor(n * 0.975)]

  return { mean, lower95, upper95 }
}

// ────────────────────────────────────────────────────────────────────
//  1. LOG-LINEAR  (EPA / HRAPIE standard form)
// ────────────────────────────────────────────────────────────────────

/**
 * Log-linear concentration-response function.
 *
 * The standard BenMAP / EPA form for chronic-exposure CRFs:
 *
 *     ΔY = y₀ × Pop × (1 − 1/exp(β × ΔC))
 *
 * where
 *   - β   = log relative risk per unit concentration
 *   - ΔC  = C_baseline − C_control  (positive means a reduction)
 *   - y₀  = baseline incidence rate  (cases per person per year)
 *   - Pop = exposed population count
 *
 * The population attributable fraction (PAF) is:
 *
 *     PAF = 1 − 1/exp(β × ΔC)  =  1 − 1/RR
 *
 * @param {number} beta   - Log-RR per unit concentration.
 * @param {number} deltaC - Change in concentration (baseline − control).
 * @param {number} y0     - Baseline incidence rate (per person per year).
 * @param {number} pop    - Exposed population.
 * @returns {{ cases: number, paf: number }}
 */
function logLinear(beta, deltaC, y0, pop) {
  const rr = Math.exp(beta * deltaC)
  const paf = 1 - 1 / rr
  const cases = paf * y0 * pop
  return { cases, paf }
}

// ────────────────────────────────────────────────────────────────────
//  2. MR-BRT SPLINE  (GBD 2023)
// ────────────────────────────────────────────────────────────────────

/**
 * MR-BRT (Meta-Regression — Bayesian, Regularised, Trimmed) spline
 * risk function.
 *
 * The GBD uses a non-linear spline fit to model the RR as a function
 * of concentration rather than a single log-linear slope.  The PAF is:
 *
 *     PAF = (RR(C_base) − RR(C_ctrl)) / RR(C_base)
 *
 * The full implementation requires a tabulated array of
 * [concentration, RR] knot-points from the IHME data release.
 *
 * **Current implementation**: Placeholder that uses a log-linear
 * approximation of the spline and logs a one-time console warning.
 * The interpolation infrastructure is in place — once real GBD spline
 * knot data is loaded, replace `PLACEHOLDER_SPLINE_TABLE` and the
 * fallback flag.
 *
 * @param {number} beta   - Central log-RR per unit (used for fallback).
 * @param {number} cBase  - Baseline concentration.
 * @param {number} cCtrl  - Control/counterfactual concentration.
 * @param {number} y0     - Baseline incidence rate.
 * @param {number} pop    - Exposed population.
 * @param {Array<[number, number]>|null} splineTable
 *   Optional tabulated [concentration, RR] pairs.  If null, the
 *   log-linear fallback is used.
 * @returns {{ cases: number, paf: number }}
 */
let _mrBrtWarned = false

function mrBrt(beta, cBase, cCtrl, y0, pop, splineTable = null) {
  if (splineTable && splineTable.length >= 2) {
    const rrBase = interpolateRR(splineTable, cBase)
    const rrCtrl = interpolateRR(splineTable, cCtrl)
    const paf = (rrBase - rrCtrl) / rrBase
    return { cases: paf * y0 * pop, paf }
  }

  // Fallback: log-linear approximation
  if (!_mrBrtWarned) {
    console.warn(
      '[hia-engine] MR-BRT spline data not loaded — using log-linear ' +
      'approximation.  Results will differ from GBD estimates.',
    )
    _mrBrtWarned = true
  }
  return logLinear(beta, cBase - cCtrl, y0, pop)
}

/**
 * Linear interpolation of RR from a tabulated spline.
 *
 * @param {Array<[number, number]>} table
 *   Sorted array of [concentration, RR] pairs (ascending by conc).
 * @param {number} c - Concentration to look up.
 * @returns {number} Interpolated RR.
 */
function interpolateRR(table, c) {
  if (c <= table[0][0]) return table[0][1]
  if (c >= table[table.length - 1][0]) return table[table.length - 1][1]

  for (let i = 1; i < table.length; i++) {
    if (c <= table[i][0]) {
      const [c0, rr0] = table[i - 1]
      const [c1, rr1] = table[i]
      const t = (c - c0) / (c1 - c0)
      return rr0 + t * (rr1 - rr0)
    }
  }
  return table[table.length - 1][1]
}

// ────────────────────────────────────────────────────────────────────
//  3. GEMM SCHIF  (Global Exposure Mortality Model)
// ────────────────────────────────────────────────────────────────────

/**
 * GEMM — Shape-Constrained Health Impact Function.
 *
 * Burnett et al. (2018) parameterised the hazard ratio as:
 *
 *     HR(z) = exp( θ × z / (1 + exp(−(z − μ) / τ)) )
 *
 * where
 *   z  = max(0, C − TMREL)        exposure above the theoretical
 *                                  minimum risk exposure level
 *   θ  = shape parameter           (stored as `beta` in the CRF)
 *   μ  = inflection concentration  (default 20 μg/m³ if not provided)
 *   τ  = scale parameter           (default 8 if not provided)
 *   TMREL = 2.4 μg/m³             (the no-risk threshold for PM2.5)
 *
 * The PAF between two concentrations is:
 *
 *     PAF = (HR(z_base) − HR(z_ctrl)) / HR(z_base)
 *
 * @param {number} theta  - Shape parameter θ (the CRF beta).
 * @param {number} cBase  - Baseline concentration.
 * @param {number} cCtrl  - Control concentration.
 * @param {number} y0     - Baseline incidence rate.
 * @param {number} pop    - Exposed population.
 * @param {object} [params]           - Optional GEMM shape parameters.
 * @param {number} [params.tmrel=2.4] - Theoretical minimum risk exposure.
 * @param {number} [params.mu=20]     - Inflection concentration.
 * @param {number} [params.tau=8]     - Scale parameter.
 * @returns {{ cases: number, paf: number }}
 */
function gemm(theta, cBase, cCtrl, y0, pop, params = {}) {
  const tmrel = params.tmrel ?? 2.4
  const mu = params.mu ?? 20
  const tau = params.tau ?? 8

  const zBase = Math.max(0, cBase - tmrel)
  const zCtrl = Math.max(0, cCtrl - tmrel)

  const hrBase = gemmHR(theta, zBase, mu, tau)
  const hrCtrl = gemmHR(theta, zCtrl, mu, tau)

  const paf = hrBase > 1 ? (hrBase - hrCtrl) / hrBase : 0
  return { cases: paf * y0 * pop, paf }
}

/**
 * Compute the GEMM hazard ratio for a given z (exposure above TMREL).
 *
 * @param {number} theta - Shape parameter.
 * @param {number} z     - Exposure above TMREL, max(0, C − 2.4).
 * @param {number} mu    - Inflection point.
 * @param {number} tau   - Scale parameter.
 * @returns {number} HR(z).
 */
function gemmHR(theta, z, mu, tau) {
  if (z <= 0) return 1.0
  const sigmoid = 1 / (1 + Math.exp(-(z - mu) / tau))
  return Math.exp(theta * z * sigmoid)
}

// ────────────────────────────────────────────────────────────────────
//  4. FUSION — Tabulated marginal risk with trapezoidal integration
// ────────────────────────────────────────────────────────────────────

/**
 * Fusion hybrid concentration-response function.
 *
 * Weichenthal et al. (2022) estimate a non-parametric marginal risk
 * function MR(c) — the instantaneous per-unit excess risk at concentration
 * c.  The total excess risk between two concentrations is the integral:
 *
 *     Excess risk = ∫_{c_ctrl}^{c_base} MR(c) dc
 *
 * which is evaluated numerically via the trapezoidal rule over a
 * tabulated grid.
 *
 * **Current implementation**: Placeholder using synthetic MR values
 * derived from a log-linear approximation.  Once real tabulated data
 * is available, pass it via the `mrTable` parameter.
 *
 * @param {number} beta   - Central log-RR (used for placeholder MR).
 * @param {number} cBase  - Baseline concentration.
 * @param {number} cCtrl  - Control concentration.
 * @param {number} y0     - Baseline incidence rate.
 * @param {number} pop    - Exposed population.
 * @param {Array<[number, number]>|null} mrTable
 *   Optional tabulated [concentration, marginalRisk] pairs.
 * @returns {{ cases: number, paf: number }}
 */
let _fusionWarned = false

function fusion(beta, cBase, cCtrl, y0, pop, mrTable = null) {
  if (mrTable && mrTable.length >= 2) {
    const excessRisk = trapezoidalIntegrate(mrTable, cCtrl, cBase)
    const paf = 1 - Math.exp(-excessRisk)
    return { cases: paf * y0 * pop, paf }
  }

  // Placeholder: synthesise MR table from log-linear assumption
  if (!_fusionWarned) {
    console.warn(
      '[hia-engine] Fusion marginal-risk table not loaded — using ' +
      'log-linear approximation.  Results will differ from Fusion estimates.',
    )
    _fusionWarned = true
  }

  // Build a synthetic MR table: MR(c) ≈ beta for constant log-linear
  const lo = Math.min(cCtrl, cBase)
  const hi = Math.max(cCtrl, cBase)
  const steps = 50
  const dc = (hi - lo) / steps
  const syntheticTable = []
  for (let i = 0; i <= steps; i++) {
    syntheticTable.push([lo + i * dc, beta])
  }

  const excessRisk = trapezoidalIntegrate(syntheticTable, cCtrl, cBase)
  const paf = 1 - Math.exp(-excessRisk)
  return { cases: paf * y0 * pop, paf }
}

/**
 * Trapezoidal numerical integration of a tabulated function.
 *
 * Integrates f(x) from `a` to `b` given a sorted table of [x, f(x)]
 * pairs.  Values outside the table bounds are clamped to the nearest
 * tabulated value.
 *
 * @param {Array<[number, number]>} table - Sorted [x, y] pairs.
 * @param {number} a - Lower integration bound.
 * @param {number} b - Upper integration bound.
 * @returns {number} Approximate integral ∫_a^b f(x) dx.
 */
function trapezoidalIntegrate(table, a, b) {
  if (a >= b || table.length < 2) return 0

  // Clip bounds to table range
  const xMin = table[0][0]
  const xMax = table[table.length - 1][0]
  const lo = Math.max(a, xMin)
  const hi = Math.min(b, xMax)
  if (lo >= hi) return 0

  // Collect relevant segments
  let integral = 0
  let prevX = lo
  let prevY = interpolateY(table, lo)

  for (let i = 0; i < table.length; i++) {
    const [xi, yi] = table[i]
    if (xi <= lo) continue
    if (xi >= hi) {
      // Final segment
      const endY = interpolateY(table, hi)
      integral += 0.5 * (prevY + endY) * (hi - prevX)
      break
    }
    integral += 0.5 * (prevY + yi) * (xi - prevX)
    prevX = xi
    prevY = yi

    // If this is the last table point before hi
    if (i === table.length - 1) {
      const endY = interpolateY(table, hi)
      integral += 0.5 * (prevY + endY) * (hi - prevX)
    }
  }

  return integral
}

/**
 * Linear interpolation helper for a sorted [x, y] table.
 * @param {Array<[number, number]>} table
 * @param {number} x
 * @returns {number}
 */
function interpolateY(table, x) {
  if (x <= table[0][0]) return table[0][1]
  if (x >= table[table.length - 1][0]) return table[table.length - 1][1]
  for (let i = 1; i < table.length; i++) {
    if (x <= table[i][0]) {
      const [x0, y0] = table[i - 1]
      const [x1, y1] = table[i]
      const t = (x - x0) / (x1 - x0)
      return y0 + t * (y1 - y0)
    }
  }
  return table[table.length - 1][1]
}

// ────────────────────────────────────────────────────────────────────
//  Dispatcher — route a CRF to the correct functional form
// ────────────────────────────────────────────────────────────────────

/**
 * Compute attributable cases for a single CRF + a single sampled beta.
 *
 * @param {string} form   - Functional form identifier.
 * @param {number} beta   - (Possibly sampled) log-RR per unit.
 * @param {number} cBase  - Baseline concentration.
 * @param {number} cCtrl  - Control concentration.
 * @param {number} y0     - Baseline incidence rate.
 * @param {number} pop    - Exposed population.
 * @returns {{ cases: number, paf: number }}
 */
function computeSingleCRF(form, beta, cBase, cCtrl, y0, pop) {
  const deltaC = cBase - cCtrl

  switch (form) {
    case 'log-linear':
      return logLinear(beta, deltaC, y0, pop)

    case 'mr-brt':
      return mrBrt(beta, cBase, cCtrl, y0, pop, null)

    case 'gemm-nlt':
      return gemm(beta, cBase, cCtrl, y0, pop)

    case 'fusion-hybrid':
      return fusion(beta, cBase, cCtrl, y0, pop, null)

    default:
      // Unknown form — fall back to log-linear
      console.warn(`[hia-engine] Unknown functional form "${form}", using log-linear.`)
      return logLinear(beta, deltaC, y0, pop)
  }
}

// ────────────────────────────────────────────────────────────────────
//  Main entry point
// ────────────────────────────────────────────────────────────────────

/**
 * Run a full Health Impact Assessment computation.
 *
 * For each selected CRF the engine:
 *   1. Derives SE from the 95 % CI in the CRF record.
 *   2. Runs `monteCarloIterations` draws of beta ~ N(β̂, SE²).
 *   3. Computes attributable cases, PAF, and rate for each draw.
 *   4. Summarises across draws (mean, 2.5th, 97.5th percentiles).
 *
 * @param {object} config
 * @param {number}   config.baselineConcentration  - C_baseline.
 * @param {number}   config.controlConcentration   - C_control.
 * @param {number}   config.baselineIncidence       - y₀ (per person/yr).
 * @param {number}   config.population              - Exposed pop count.
 * @param {object[]} config.selectedCRFs            - Array of CRF objects
 *   from crf-library.json.  Each must include at minimum: id, source,
 *   endpoint, beta, betaLow, betaHigh, functionalForm.
 * @param {number}  [config.monteCarloIterations=1000]
 *   Number of MC draws per CRF.
 *
 * @returns {{
 *   results: Array<{
 *     crfId: string,
 *     study: string,
 *     endpoint: string,
 *     attributableCases:    { mean: number, lower95: number, upper95: number },
 *     attributableFraction: { mean: number, lower95: number, upper95: number },
 *     attributableRate:     { mean: number, lower95: number, upper95: number },
 *   }>,
 *   totalDeaths: { mean: number, lower95: number, upper95: number }
 * }}
 */
export function computeHIA(config) {
  const {
    baselineConcentration,
    controlConcentration,
    baselineIncidence,
    population,
    selectedCRFs,
    monteCarloIterations = 1000,
  } = config

  if (!selectedCRFs || selectedCRFs.length === 0) {
    return { results: [], totalDeaths: { mean: 0, lower95: 0, upper95: 0 } }
  }

  const PER_100K = 100_000

  const results = selectedCRFs.map((crf) => {
    const se = betaSE(crf.betaLow, crf.betaHigh)
    const form = crf.functionalForm || 'log-linear'

    // Use CRF-specific incidence rate if provided, else the global config rate
    const y0 = crf.defaultRate ?? baselineIncidence

    const caseSamples = []
    const pafSamples = []
    const rateSamples = []

    for (let i = 0; i < monteCarloIterations; i++) {
      const sampledBeta = sampleNormal(crf.beta, se)

      const { cases, paf } = computeSingleCRF(
        form,
        sampledBeta,
        baselineConcentration,
        controlConcentration,
        y0,
        population,
      )

      caseSamples.push(cases)
      pafSamples.push(paf)
      rateSamples.push(population > 0 ? (cases / population) * PER_100K : 0)
    }

    return {
      crfId: crf.id,
      study: crf.source,
      endpoint: crf.endpoint,
      attributableCases: summarise(caseSamples),
      attributableFraction: summarise(pafSamples),
      attributableRate: summarise(rateSamples),
    }
  })

  // Total deaths: sum of attributable cases across all *mortality* CRFs.
  // Non-mortality endpoints (hospitalisations, ED visits, incidence) are
  // excluded from the death total.
  const mortalityKeywords = ['mortality', 'death', 'deaths']
  const isMortality = (endpoint) =>
    mortalityKeywords.some((kw) => endpoint.toLowerCase().includes(kw))

  // We sum per-iteration across mortality CRFs to properly capture
  // correlated uncertainty.  Since each CRF is sampled independently,
  // we just sum the per-CRF means / bounds as an approximation.
  // (A full joint MC would re-sample all CRFs per iteration — deferred
  // to a future release.)
  const mortalityResults = results.filter((r) => isMortality(r.endpoint))
  const totalDeaths = {
    mean: mortalityResults.reduce((s, r) => s + r.attributableCases.mean, 0),
    lower95: mortalityResults.reduce((s, r) => s + r.attributableCases.lower95, 0),
    upper95: mortalityResults.reduce((s, r) => s + r.attributableCases.upper95, 0),
  }

  return { results, totalDeaths }
}

// ────────────────────────────────────────────────────────────────────
//  Exports for testing / advanced use
// ────────────────────────────────────────────────────────────────────

export {
  logLinear,
  mrBrt,
  gemm,
  fusion,
  interpolateRR,
  trapezoidalIntegrate,
  betaSE,
  summarise,
  gemmHR,
}
