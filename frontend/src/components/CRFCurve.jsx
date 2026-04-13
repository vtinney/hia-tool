import { useMemo } from 'react'

// ── Curve math ────────────────────────────────────────────────
// Computes RR(c) for a CRF given its functional form. Uses a baseline
// concentration of 0 by default. For MR-BRT and other spline forms we
// fall back to log-linear at the central beta — close enough for a
// quick visual preview when the actual spline data isn't loaded.

function rr(form, beta, c) {
  switch (form) {
    case 'linear':
      return 1 + beta * c
    case 'power':
      // Power forms in HIA are typically RR = (c+1)^beta
      return Math.pow(c + 1, beta)
    case 'log-linear':
    case 'mr-brt':
    case 'gemm-nlt':
    default:
      return Math.exp(beta * c)
  }
}

// Choose an appropriate x-axis max for the pollutant.
function xMaxForPollutant(pollutant) {
  switch (pollutant) {
    case 'pm25':
      return 35
    case 'no2':
      return 60
    case 'ozone':
      return 80
    default:
      return 35
  }
}

function unitForPollutant(pollutant) {
  return 'µg/m³'
}

function labelForPollutant(pollutant) {
  switch (pollutant) {
    case 'pm25':  return 'PM₂.₅'
    case 'no2':   return 'NO₂'
    case 'ozone': return 'O₃'
    default:      return 'Concentration'
  }
}

// Pretty-print numbers with appropriate decimals
function fmt(n, d = 3) {
  if (n == null || isNaN(n)) return '—'
  return n.toFixed(d)
}

/**
 * <CRFCurve />
 *
 * Props:
 * - crf            { beta, betaLow, betaHigh, functionalForm, source, endpoint, pollutant }
 * - referenceConc  number — vertical reference line + read-out (default: midpoint)
 * - height         number — px (default 320)
 * - compact        boolean — hides the read-out strip and source row
 */
export default function CRFCurve({
  crf,
  referenceConc,
  height = 320,
  compact = false,
}) {
  const W = 560
  const H = height
  const PAD = compact
    ? { l: 48, r: 16, t: 14, b: 36 }
    : { l: 56, r: 24, t: 22, b: 44 }

  const innerW = W - PAD.l - PAD.r
  const innerH = H - PAD.t - PAD.b

  const pollutant = crf?.pollutant || 'pm25'
  const xMax = xMaxForPollutant(pollutant)
  const ref = referenceConc ?? Math.round(xMax / 3)

  // Sample the curve at 60 points for a smooth path
  const samples = useMemo(() => {
    if (!crf) return null
    const N = 60
    const xs = []
    const mean = []
    const lo = []
    const hi = []
    const form = crf.functionalForm || 'log-linear'
    for (let i = 0; i <= N; i++) {
      const x = (i / N) * xMax
      xs.push(x)
      mean.push(rr(form, crf.beta,     x))
      lo.push(rr(form,   crf.betaLow,  x))
      hi.push(rr(form,   crf.betaHigh, x))
    }
    return { xs, mean, lo, hi }
  }, [crf, xMax])

  if (!crf || !samples) {
    return (
      <div
        className="surface flex items-center justify-center text-center px-6"
        style={{ minHeight: height }}
      >
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.14em] text-zinc-400 mb-2">
            No CRF selected
          </p>
          <p className="text-[13px] text-zinc-500">
            Hover or check a row to preview its dose-response curve.
          </p>
        </div>
      </div>
    )
  }

  // Y-axis bounds — pad above the upper bound by 6%
  const allHigh = Math.max(...samples.hi)
  const yMin = 1.0
  const yMax = Math.max(1.05, allHigh * 1.06)

  const sx = (x) => PAD.l + (x / xMax) * innerW
  const sy = (y) => PAD.t + (1 - (y - yMin) / (yMax - yMin)) * innerH

  const meanPath =
    'M ' +
    samples.xs.map((x, i) => `${sx(x)} ${sy(samples.mean[i])}`).join(' L ')

  // Confidence envelope: forward along upper, back along lower
  const ciPath =
    'M ' +
    samples.xs.map((x, i) => `${sx(x)} ${sy(samples.hi[i])}`).join(' L ') +
    ' L ' +
    samples.xs
      .slice()
      .reverse()
      .map((x, i) => {
        const idx = samples.xs.length - 1 - i
        return `${sx(x)} ${sy(samples.lo[idx])}`
      })
      .join(' L ') +
    ' Z'

  // Y ticks — auto, 5 of them
  const yTicks = (() => {
    const span = yMax - yMin
    const step = span / 4
    return [0, 1, 2, 3, 4].map((k) => yMin + step * k)
  })()

  const xTicks = (() => {
    const step = xMax / 4
    return [0, 1, 2, 3, 4].map((k) => Math.round(step * k))
  })()

  // Read-out values at the reference concentration
  const refMean = rr(crf.functionalForm || 'log-linear', crf.beta,     ref)
  const refLow  = rr(crf.functionalForm || 'log-linear', crf.betaLow,  ref)
  const refHigh = rr(crf.functionalForm || 'log-linear', crf.betaHigh, ref)

  return (
    <figure className="surface p-6 lg:p-7">
      {!compact && (
        <div className="flex items-baseline justify-between mb-4">
          <p className="eyebrow">Dose–response preview</p>
          <p className="font-mono text-[10px] tracking-[0.12em] uppercase text-zinc-400 truncate ml-3">
            {(crf.functionalForm || 'log-linear').replace('-', '·')}
          </p>
        </div>
      )}

      <svg
        viewBox={`0 0 ${W} ${H}`}
        className="w-full h-auto"
        role="img"
        aria-label={`Dose-response curve for ${crf.endpoint}`}
      >
        <defs>
          <linearGradient id="ci-grad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%"  stopColor="#155852" stopOpacity="0.20" />
            <stop offset="100%" stopColor="#155852" stopOpacity="0.04" />
          </linearGradient>
        </defs>

        {/* Grid */}
        {yTicks.map((y, i) => (
          <line
            key={i}
            x1={PAD.l} x2={W - PAD.r}
            y1={sy(y)} y2={sy(y)}
            stroke="rgba(10,10,11,0.06)"
            strokeWidth="1"
          />
        ))}

        {/* Y labels */}
        {yTicks.map((y, i) => (
          <text
            key={i}
            x={PAD.l - 10}
            y={sy(y)}
            textAnchor="end"
            dominantBaseline="middle"
            fontFamily="Geist Mono, monospace"
            fontSize="10"
            fill="#a1a1aa"
          >
            {y.toFixed(2)}
          </text>
        ))}

        {/* X labels */}
        {xTicks.map((x, i) => (
          <text
            key={i}
            x={sx(x)}
            y={H - PAD.b + 16}
            textAnchor="middle"
            fontFamily="Geist Mono, monospace"
            fontSize="10"
            fill="#a1a1aa"
          >
            {x}
          </text>
        ))}

        {/* Axis titles */}
        <text
          x={PAD.l}
          y={H - 6}
          fontFamily="Geist Mono, monospace"
          fontSize="9.5"
          fill="#71717a"
          letterSpacing="0.04em"
        >
          {labelForPollutant(pollutant)} ({unitForPollutant(pollutant)})
        </text>
        <text
          x={PAD.l - 46}
          y={PAD.t + 4}
          fontFamily="Geist Mono, monospace"
          fontSize="9.5"
          fill="#71717a"
          letterSpacing="0.04em"
        >
          RR
        </text>

        {/* CI envelope */}
        <path d={ciPath} fill="url(#ci-grad)" />

        {/* Mean curve */}
        <path
          key={crf.id /* re-mount on CRF change so the draw replays */}
          d={meanPath}
          fill="none"
          stroke="#155852"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          className="hia-draw"
        />

        {/* Reference */}
        <line
          x1={sx(ref)} x2={sx(ref)}
          y1={PAD.t} y2={H - PAD.b}
          stroke="#155852"
          strokeWidth="1"
          strokeDasharray="3 3"
          opacity="0.4"
        />
        <circle
          cx={sx(ref)}
          cy={sy(refMean)}
          r="3.75"
          fill="#fafaf9"
          stroke="#155852"
          strokeWidth="1.75"
        />
      </svg>

      {!compact && (
        <div className="mt-4 grid grid-cols-3 gap-4 pt-4 border-t border-zinc-200/80">
          <div>
            <p className="font-mono text-[10px] uppercase tracking-[0.12em] text-zinc-400 mb-1">
              At {ref} {unitForPollutant(pollutant)}
            </p>
            <p className="font-mono text-[15px] text-ink tabular-nums">
              RR {fmt(refMean)}
            </p>
          </div>
          <div>
            <p className="font-mono text-[10px] uppercase tracking-[0.12em] text-zinc-400 mb-1">
              95% CI
            </p>
            <p className="font-mono text-[15px] text-zinc-600 tabular-nums">
              {fmt(refLow)} – {fmt(refHigh)}
            </p>
          </div>
          <div className="min-w-0">
            <p className="font-mono text-[10px] uppercase tracking-[0.12em] text-zinc-400 mb-1">
              Endpoint
            </p>
            <p className="text-[12.5px] text-zinc-700 truncate" title={crf.endpoint}>
              {crf.endpoint}
            </p>
          </div>
        </div>
      )}

      {!compact && crf.source && (
        <p className="mt-3 font-mono text-[10px] uppercase tracking-[0.12em] text-zinc-400 truncate">
          {crf.source}
        </p>
      )}
    </figure>
  )
}
