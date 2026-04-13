import { describe, it, expect } from 'vitest'
import {
  computeHIA,
  logLinear,
  gemm,
  betaSE,
  gemmHR,
} from '../hia-engine.js'

// ── Shared test fixtures ───────────────────────────────────────────

/** Krewski-style CRF: RR = 1.06 per 10 µg/m³ → beta ≈ 0.005827 */
const KREWSKI_CRF = {
  id: 'test_krewski',
  source: 'Krewski et al. 2009',
  endpoint: 'All-cause mortality',
  beta: 0.005827,
  betaLow: 0.003922,
  betaHigh: 0.007716,
  functionalForm: 'log-linear',
  defaultRate: 0.008,
}

/** GEMM NCD+LRI CRF (Burnett et al. 2018) */
const GEMM_CRF = {
  id: 'test_gemm_acm',
  source: 'Burnett et al. 2018 (GEMM)',
  endpoint: 'All-cause mortality (non-accidental)',
  beta: 0.00700,
  betaLow: 0.00520,
  betaHigh: 0.00880,
  functionalForm: 'gemm-nlt',
  defaultRate: 0.008,
}

// ── Test 1: Single-value PM₂.₅, Krewski CRF ───────────────────────

describe('Log-linear (Krewski CRF)', () => {
  const baseline = 12
  const control = 5
  const deltaC = baseline - control // 7
  const pop = 1_000_000
  const y0 = 0.008
  const beta = 0.005827

  // Expected: deltaY = (1 - exp(-beta * deltaC)) * y0 * pop
  const expectedPAF = 1 - Math.exp(-beta * deltaC)
  const expectedDeaths = expectedPAF * y0 * pop

  it('computes correct point estimate via logLinear()', () => {
    const { cases, paf } = logLinear(beta, deltaC, y0, pop)

    // Verify ~321 deaths within 1%
    expect(cases).toBeCloseTo(expectedDeaths, 0)
    expect(Math.abs(cases - expectedDeaths) / expectedDeaths).toBeLessThan(0.01)
    expect(paf).toBeCloseTo(expectedPAF, 6)
  })

  it('computes correct result via computeHIA()', () => {
    const result = computeHIA({
      baselineConcentration: baseline,
      controlConcentration: control,
      baselineIncidence: y0,
      population: pop,
      selectedCRFs: [KREWSKI_CRF],
      monteCarloIterations: 5000,
    })

    expect(result.results).toHaveLength(1)
    const r = result.results[0]

    // Mean should be within 5% of analytic point estimate
    // (MC noise is larger with finite samples)
    const relError = Math.abs(r.attributableCases.mean - expectedDeaths) / expectedDeaths
    expect(relError).toBeLessThan(0.05)

    // Verify structure
    expect(r.crfId).toBe('test_krewski')
    expect(r.study).toBe('Krewski et al. 2009')
    expect(r.endpoint).toBe('All-cause mortality')
    expect(r.attributableCases).toHaveProperty('mean')
    expect(r.attributableCases).toHaveProperty('lower95')
    expect(r.attributableCases).toHaveProperty('upper95')
    expect(r.attributableFraction).toHaveProperty('mean')
    expect(r.attributableRate).toHaveProperty('mean')
  })

  it('separate (default) does not pool mortality endpoints', () => {
    const result = computeHIA({
      baselineConcentration: baseline,
      controlConcentration: control,
      baselineIncidence: y0,
      population: pop,
      selectedCRFs: [KREWSKI_CRF],
      monteCarloIterations: 2000,
    })

    // 'separate' (the default) keeps each CRF on its own — no pooled total
    expect(result.totalDeaths).toBeNull()
    expect(result.results[0].attributableCases.mean).toBeGreaterThan(0)
  })
})

// ── Test 2: GEMM NCD+LRI ──────────────────────────────────────────

describe('GEMM SCHIF', () => {
  const baseline = 50
  const control = 2.4 // TMREL
  const pop = 1_000_000
  const y0 = 0.008

  it('produces a PAF between 0.10 and 0.30 for high exposure', () => {
    const result = computeHIA({
      baselineConcentration: baseline,
      controlConcentration: control,
      baselineIncidence: y0,
      population: pop,
      selectedCRFs: [GEMM_CRF],
      monteCarloIterations: 3000,
    })

    const paf = result.results[0].attributableFraction.mean
    expect(paf).toBeGreaterThan(0.10)
    expect(paf).toBeLessThan(0.30)
  })

  it('gemmHR returns 1.0 when z = 0', () => {
    const hr = gemmHR(0.007, 0, 20, 8)
    expect(hr).toBe(1.0)
  })

  it('gemmHR increases with z', () => {
    const hr10 = gemmHR(0.007, 10, 20, 8)
    const hr40 = gemmHR(0.007, 40, 20, 8)
    expect(hr10).toBeGreaterThan(1.0)
    expect(hr40).toBeGreaterThan(hr10)
  })
})

// ── Test 3: Monte Carlo uncertainty propagation ────────────────────

describe('Monte Carlo uncertainty', () => {
  it('95% CI is narrower than point estimate ± 50%', () => {
    const result = computeHIA({
      baselineConcentration: 12,
      controlConcentration: 5,
      baselineIncidence: 0.008,
      population: 1_000_000,
      selectedCRFs: [KREWSKI_CRF],
      monteCarloIterations: 5000,
    })

    const r = result.results[0]
    const mean = r.attributableCases.mean
    const ciWidth = r.attributableCases.upper95 - r.attributableCases.lower95

    // CI width should be narrower than ± 50% of the mean (i.e., < mean)
    expect(ciWidth).toBeLessThan(mean)

    // lower95 should be positive (we're reducing concentration)
    expect(r.attributableCases.lower95).toBeGreaterThan(0)

    // upper95 > lower95
    expect(r.attributableCases.upper95).toBeGreaterThan(r.attributableCases.lower95)
  })

  it('betaSE derives correct SE from CI bounds', () => {
    const se = betaSE(0.003922, 0.007716)
    // (0.007716 - 0.003922) / (2 * 1.96) = 0.000968
    expect(se).toBeCloseTo(0.000968, 5)
  })
})

// ── Test 4: Analytical CIs (the new default) ──────────────────────

describe('Analytical CIs (monteCarloIterations = 0)', () => {
  const baseline = 12
  const control = 5
  const pop = 1_000_000
  const y0 = 0.008

  it('is fully deterministic across runs', () => {
    const cfg = {
      baselineConcentration: baseline,
      controlConcentration: control,
      baselineIncidence: y0,
      population: pop,
      selectedCRFs: [KREWSKI_CRF],
      monteCarloIterations: 0,
    }
    const a = computeHIA(cfg)
    const b = computeHIA(cfg)
    expect(a.results[0].attributableCases.mean).toBe(b.results[0].attributableCases.mean)
    expect(a.results[0].attributableCases.lower95).toBe(b.results[0].attributableCases.lower95)
    expect(a.results[0].attributableCases.upper95).toBe(b.results[0].attributableCases.upper95)
  })

  it('lower95 equals analytical cases at betaLow', () => {
    const result = computeHIA({
      baselineConcentration: baseline,
      controlConcentration: control,
      baselineIncidence: y0,
      population: pop,
      selectedCRFs: [KREWSKI_CRF],
      monteCarloIterations: 0,
    })

    const deltaC = baseline - control
    // Reference values straight from the formula
    const meanRef = (1 - Math.exp(-KREWSKI_CRF.beta * deltaC))     * y0 * pop
    const loRef   = (1 - Math.exp(-KREWSKI_CRF.betaLow * deltaC))  * y0 * pop
    const hiRef   = (1 - Math.exp(-KREWSKI_CRF.betaHigh * deltaC)) * y0 * pop

    const r = result.results[0]
    expect(r.attributableCases.mean).toBeCloseTo(meanRef, 3)
    expect(r.attributableCases.lower95).toBeCloseTo(loRef, 3)
    expect(r.attributableCases.upper95).toBeCloseTo(hiRef, 3)
  })

  it('reports analytical method in meta', () => {
    const result = computeHIA({
      baselineConcentration: baseline,
      controlConcentration: control,
      baselineIncidence: y0,
      population: pop,
      selectedCRFs: [KREWSKI_CRF],
      monteCarloIterations: 0,
    })
    expect(result.meta.uncertaintyMethod).toBe('analytical')
  })

  it('reports monte-carlo method when iterations > 0', () => {
    const result = computeHIA({
      baselineConcentration: baseline,
      controlConcentration: control,
      baselineIncidence: y0,
      population: pop,
      selectedCRFs: [KREWSKI_CRF],
      monteCarloIterations: 500,
    })
    expect(result.meta.uncertaintyMethod).toBe('monte-carlo')
  })

  it('defaults to analytical when monteCarloIterations is omitted', () => {
    const result = computeHIA({
      baselineConcentration: baseline,
      controlConcentration: control,
      baselineIncidence: y0,
      population: pop,
      selectedCRFs: [KREWSKI_CRF],
    })
    expect(result.meta.uncertaintyMethod).toBe('analytical')
  })
})

// ── Test 5: Pooling method ────────────────────────────────────────

describe('Pooling method', () => {
  const baseCfg = {
    baselineConcentration: 12,
    controlConcentration: 5,
    baselineIncidence: 0.008,
    population: 1_000_000,
    selectedCRFs: [KREWSKI_CRF],
    monteCarloIterations: 0,
  }

  it('separate (default) returns null pooled total', () => {
    const result = computeHIA({ ...baseCfg, poolingMethod: 'separate' })
    expect(result.totalDeaths).toBeNull()
  })

  it('fixed pools mortality endpoints into a numeric total', () => {
    const result = computeHIA({ ...baseCfg, poolingMethod: 'fixed' })
    expect(result.totalDeaths).not.toBeNull()
    expect(result.totalDeaths.mean).toBeGreaterThan(0)
  })

  it('none returns totalDeaths === null', () => {
    const result = computeHIA({ ...baseCfg, poolingMethod: 'none' })
    expect(result.totalDeaths).toBeNull()
    // Per-CRF results still populated
    expect(result.results).toHaveLength(1)
    expect(result.results[0].attributableCases.mean).toBeGreaterThan(0)
  })

  it('none with empty CRF list returns null without throwing', () => {
    const result = computeHIA({
      ...baseCfg,
      selectedCRFs: [],
      poolingMethod: 'none',
    })
    expect(result.totalDeaths).toBeNull()
    expect(result.results).toHaveLength(0)
  })

  it('reports pooling method in meta', () => {
    const result = computeHIA({ ...baseCfg, poolingMethod: 'none' })
    expect(result.meta.poolingMethod).toBe('none')
  })
})

describe('Edge cases', () => {
  it('deltaC = 0 returns 0 deaths', () => {
    const result = computeHIA({
      baselineConcentration: 12,
      controlConcentration: 12,
      baselineIncidence: 0.008,
      population: 1_000_000,
      selectedCRFs: [KREWSKI_CRF],
      monteCarloIterations: 500,
    })

    const r = result.results[0]
    // Mean should be very close to 0 (MC noise might make it non-exactly 0)
    expect(Math.abs(r.attributableCases.mean)).toBeLessThan(5)
  })

  it('very high concentration (200 µg/m³) computes without error', () => {
    const result = computeHIA({
      baselineConcentration: 200,
      controlConcentration: 5,
      baselineIncidence: 0.008,
      population: 1_000_000,
      selectedCRFs: [KREWSKI_CRF],
      monteCarloIterations: 500,
    })

    const r = result.results[0]
    expect(r.attributableCases.mean).toBeGreaterThan(0)
    expect(Number.isFinite(r.attributableCases.mean)).toBe(true)
    expect(Number.isFinite(r.attributableCases.upper95)).toBe(true)
  })

  it('very high concentration with GEMM computes without error', () => {
    const result = computeHIA({
      baselineConcentration: 200,
      controlConcentration: 2.4,
      baselineIncidence: 0.008,
      population: 1_000_000,
      selectedCRFs: [GEMM_CRF],
      monteCarloIterations: 500,
    })

    const r = result.results[0]
    expect(r.attributableCases.mean).toBeGreaterThan(0)
    expect(Number.isFinite(r.attributableCases.mean)).toBe(true)
  })

  it('empty CRF list returns empty results', () => {
    const result = computeHIA({
      baselineConcentration: 12,
      controlConcentration: 5,
      baselineIncidence: 0.008,
      population: 1_000_000,
      selectedCRFs: [],
    })

    expect(result.results).toHaveLength(0)
    // Default pooling is 'separate' → no pooled total
    expect(result.totalDeaths).toBeNull()
  })

  it('population = 0 returns 0 cases', () => {
    const result = computeHIA({
      baselineConcentration: 12,
      controlConcentration: 5,
      baselineIncidence: 0.008,
      population: 0,
      selectedCRFs: [KREWSKI_CRF],
      monteCarloIterations: 100,
    })

    expect(result.results[0].attributableCases.mean).toBe(0)
  })
})
