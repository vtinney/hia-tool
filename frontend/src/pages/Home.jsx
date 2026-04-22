import { Link, useNavigate } from 'react-router-dom'
import useAnalysisStore from '../stores/useAnalysisStore'

import tplUsNational from '../data/templates/us_national_pm25.json'
import tplUsTract from '../data/templates/us_tract_pm25_ej.json'
import tplGlobal from '../data/templates/global_pm25_gbd.json'
import tplCity from '../data/templates/single_city_pm25_who.json'

// Each pipeline node carries a distinct, muted color.
const PIPELINE = [
  { id: '01', node: 'Concentration', detail: 'Gridded or station PM₂.₅, NO₂, O₃',     swatch: 'bg-sky-600',    accent: 'text-sky-700',    border: 'border-sky-300' },
  { id: '02', node: 'Exposure',      detail: 'Population-weighted by admin unit',     swatch: 'bg-indigo-600', accent: 'text-indigo-700', border: 'border-indigo-300' },
  { id: '03', node: 'Response',      detail: 'Log-linear, linear, power, or spline',  swatch: 'bg-accent-700', accent: 'text-accent-700', border: 'border-accent-300' },
  { id: '04', node: 'Impact',        detail: 'Attributable cases with 95% CI',        swatch: 'bg-rose-700',   accent: 'text-rose-700',   border: 'border-rose-300' },
  { id: '05', node: 'Value',         detail: 'VSL or COI economic valuation',         swatch: 'bg-amber-700',  accent: 'text-amber-700',  border: 'border-amber-300' },
]

// Real annualized PM₂.₅ ranges (µg/m³) by city, used for the hero
// "Where the air is" panel and the bottom marquee. Source notes:
// IQAir 2023 World Air Quality Report and WHO ambient air database.
const CITIES = [
  { city: 'Delhi',        pm: 102 },
  { city: 'Lahore',       pm: 99  },
  { city: 'Beijing',      pm: 38  },
  { city: 'Jakarta',      pm: 36  },
  { city: 'Mexico City',  pm: 22  },
  { city: 'São Paulo',    pm: 17  },
  { city: 'London',       pm: 11  },
  { city: 'Los Angeles',  pm: 11  },
  { city: 'New York',     pm: 9   },
  { city: 'Tokyo',        pm: 9   },
  { city: 'Stockholm',    pm: 6   },
  { city: 'Reykjavík',    pm: 4   },
]

// Severity color (mapped against WHO guideline of 5 µg/m³).
function severity(pm) {
  if (pm >= 75) return { text: 'text-rose-700',   bar: 'bg-rose-700',   dot: 'bg-rose-600',   label: 'Hazardous'  }
  if (pm >= 35) return { text: 'text-orange-700', bar: 'bg-orange-700', dot: 'bg-orange-600', label: 'Unhealthy'  }
  if (pm >= 15) return { text: 'text-amber-700',  bar: 'bg-amber-700',  dot: 'bg-amber-600',  label: 'Moderate'   }
  if (pm >= 5)  return { text: 'text-accent-700', bar: 'bg-accent-700', dot: 'bg-accent-600', label: 'Acceptable' }
  return            { text: 'text-emerald-700',   bar: 'bg-emerald-700', dot: 'bg-emerald-600', label: 'Clean'     }
}

// ── Inline icons ─────────────────────────────────────────────
function MarkIcon({ className = '' }) {
  return (
    <svg className={className} viewBox="0 0 32 32" fill="none" aria-hidden="true">
      <circle cx="16" cy="16" r="14.5" stroke="currentColor" strokeWidth="1" opacity="0.4" />
      <rect x="10" y="17" width="2.5" height="6" rx="0.5" fill="currentColor" />
      <rect x="14.75" y="13" width="2.5" height="10" rx="0.5" fill="currentColor" />
      <rect x="19.5" y="9" width="2.5" height="14" rx="0.5" fill="currentColor" />
    </svg>
  )
}

// ── Hero data panel: city PM₂.₅ levels ───────────────────────
function CityPanel() {
  const featured = [
    CITIES.find((c) => c.city === 'Delhi'),
    CITIES.find((c) => c.city === 'Beijing'),
    CITIES.find((c) => c.city === 'Mexico City'),
    CITIES.find((c) => c.city === 'Los Angeles'),
    CITIES.find((c) => c.city === 'Stockholm'),
  ]
  // Scale bars relative to the highest featured city, with breathing room
  const maxPm = Math.max(...featured.map((c) => c.pm)) * 1.05

  return (
    <figure className="relative">
      <div className="surface p-7 lg:p-8">
        <div className="flex items-baseline justify-between mb-6">
          <p className="eyebrow">Where the air is</p>
          <p className="font-mono text-[10px] tracking-[0.12em] uppercase text-zinc-400">
            Annual mean PM₂.₅ ·{' '}
            <span className="font-sans normal-case tracking-normal">{'\u00B5g/m\u00B3'}</span>
          </p>
        </div>

        <ul className="space-y-5">
          {featured.map((c, i) => {
            const sev = severity(c.pm)
            const widthPct = (c.pm / maxPm) * 100
            return (
              <li key={c.city} className="hia-rise" style={{ '--i': i + 1 }}>
                <div className="flex items-baseline justify-between mb-1.5">
                  <span className="text-[15px] text-ink font-medium">{c.city}</span>
                  <span className={`font-mono text-[15px] tabular-nums ${sev.text}`}>
                    {c.pm}
                  </span>
                </div>
                <div className="relative h-2.5">
                  <div className="absolute inset-0 bg-zinc-100 rounded-full" />
                  <div
                    className={`absolute inset-y-0 left-0 ${sev.bar} rounded-full hia-bar-grow`}
                    style={{ width: `${widthPct}%` }}
                  />
                </div>
              </li>
            )
          })}
        </ul>

        {/* WHO guideline reference */}
        <div className="mt-7 pt-5 border-t border-zinc-200/80 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-emerald-600" />
            <span className="font-mono text-[11px] uppercase tracking-[0.12em] text-zinc-500">
              WHO guideline
            </span>
          </div>
          <span className="font-mono text-[14px] tabular-nums text-emerald-700">
            5 <span className="font-sans">{'\u00B5g/m\u00B3'}</span>
          </span>
        </div>
      </div>

      <div className="absolute -top-3 -right-3 hidden md:flex items-center gap-1.5 bg-ink text-paper px-2.5 py-1 rounded-md shadow-soft">
        <span className="relative flex h-1.5 w-1.5">
          <span className="absolute inline-flex h-full w-full rounded-full bg-rose-400 opacity-75 animate-ping" />
          <span className="relative inline-flex rounded-full h-1.5 w-1.5 bg-rose-300" />
        </span>
        <span className="font-mono text-[9.5px] uppercase tracking-[0.14em]">2023 data</span>
      </div>
    </figure>
  )
}

// ── Pipeline diagram with color-coded nodes, larger type ─────
function PipelineDiagram() {
  return (
    <div className="grid grid-cols-1 md:grid-cols-5 gap-px bg-zinc-200/80 border border-zinc-200/80 rounded-2xl overflow-hidden">
      {PIPELINE.map(({ id, node, detail, swatch, accent }, i) => (
        <div
          key={id}
          className="relative bg-paper p-8 lg:p-9 hia-rise group"
          style={{ '--i': i + 1 }}
        >
          {/* Color bar at top */}
          <div className={`absolute top-0 left-0 right-0 h-[4px] ${swatch}`} />

          <div className="flex items-start justify-between mb-7">
            <span className={`font-mono text-[40px] tabular-nums font-semibold ${accent} leading-none tracking-tight`}>
              {id}
            </span>
            {i < PIPELINE.length - 1 && (
              <svg
                className="hidden md:block w-4 h-4 text-zinc-300 group-hover:text-zinc-400 transition-colors duration-200"
                viewBox="0 0 12 12"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.5"
                strokeLinecap="round"
                strokeLinejoin="round"
                aria-hidden="true"
              >
                <path d="M4 2.5L7.5 6 4 9.5" />
              </svg>
            )}
          </div>

          <h3 className="text-[24px] font-semibold text-ink leading-[1.15] mb-3 tracking-tight">
            {node}
          </h3>
          <p className="text-[15.5px] text-zinc-600 leading-relaxed">{detail}</p>
        </div>
      ))}
    </div>
  )
}

// ── City marquee (replaces citation marquee) ─────────────────
function CityMarquee() {
  // Sort highest → lowest so the eye reads severity left-to-right
  const sorted = [...CITIES].sort((a, b) => b.pm - a.pm)
  const items = [...sorted, ...sorted]
  return (
    <div
      className="relative overflow-hidden border-y border-zinc-200/80 bg-white/40 py-6"
      style={{
        maskImage:
          'linear-gradient(to right, transparent, black 6%, black 94%, transparent)',
        WebkitMaskImage:
          'linear-gradient(to right, transparent, black 6%, black 94%, transparent)',
      }}
    >
      <div className="hia-marquee flex w-max gap-10 whitespace-nowrap items-center">
        {items.map((c, i) => {
          const sev = severity(c.pm)
          return (
            <span key={i} className="flex items-baseline gap-2.5">
              <span className={`w-1.5 h-1.5 rounded-full ${sev.dot} translate-y-[-2px]`} />
              <span className="font-mono text-[12px] uppercase tracking-[0.12em] text-zinc-700">
                {c.city}
              </span>
              <span className={`font-mono text-[13px] tabular-nums font-semibold ${sev.text}`}>
                {c.pm}
              </span>
              <span className="font-sans text-[11px] tracking-[0.04em] text-zinc-400">
                {'\u00B5g/m\u00B3'}
              </span>
            </span>
          )
        })}
      </div>
    </div>
  )
}

const TEMPLATES = [
  { data: tplUsNational, tag: 'PM₂.₅', color: 'bg-sky-600' },
  { data: tplUsTract,    tag: 'EJ',    color: 'bg-indigo-600' },
  { data: tplGlobal,     tag: 'GBD',   color: 'bg-rose-700' },
  { data: tplCity,       tag: 'WHO',   color: 'bg-emerald-600' },
]

function TemplateCard({ data, tag, color, onClick }) {
  return (
    <button
      onClick={onClick}
      className="group relative text-left bg-paper border border-zinc-200/80 rounded-xl p-6 lg:p-7 hover:border-zinc-300 hover:shadow-soft transition-all duration-200"
    >
      <div className="flex items-start justify-between mb-4">
        <span className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-mono uppercase tracking-[0.12em] text-white ${color}`}>
          {tag}
        </span>
        <svg
          className="w-4 h-4 text-zinc-300 group-hover:text-zinc-500 transition-colors duration-200"
          viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="1.5"
          strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"
        >
          <path d="M4 2.5L7.5 6 4 9.5" />
        </svg>
      </div>
      <h3 className="text-[16px] font-medium text-ink leading-snug mb-2 tracking-tight">
        {data.name}
      </h3>
      <p className="text-[14px] text-zinc-500 leading-relaxed line-clamp-3">
        {data.description}
      </p>
    </button>
  )
}

export default function Home() {
  const navigate = useNavigate()
  const loadFromTemplate = useAnalysisStore((s) => s.loadFromTemplate)

  function handleTemplate(tpl) {
    loadFromTemplate(tpl)
    // Templates pre-fill wizard state but don't run the analysis — send
    // the user into the wizard so they can review, fill in any steps the
    // template leaves incomplete (often step 2-4), and run from step 6.
    navigate('/analysis/1')
  }

  return (
    <div className="min-h-[100dvh] bg-paper">
      {/* ── Top bar ───────────────────────────────────────────── */}
      <header className="border-b border-zinc-200/80">
        <div className="max-w-[1320px] mx-auto px-6 lg:px-10 h-16 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <MarkIcon className="w-7 h-7 text-accent-700" />
            <span className="font-mono text-[13px] tracking-[0.14em] uppercase text-ink">
              Health Impact Assessment <span className="text-zinc-400">/ Walkthrough</span>
            </span>
          </div>
          <span className="hidden sm:inline font-mono text-[11px] tracking-[0.14em] uppercase text-zinc-400">
            v0.1 · research preview
          </span>
        </div>
      </header>

      {/* ── Hero ───────────────────────────────────────────────── */}
      <section className="max-w-[1320px] mx-auto px-6 lg:px-10 pt-16 lg:pt-24 pb-20 lg:pb-28">
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-12 lg:gap-16 items-start">
          {/* Left column */}
          <div className="lg:col-span-6 lg:pt-4">
            <p className="font-mono text-[13px] uppercase tracking-[0.14em] text-zinc-500 mb-6">
              A guided assessment
            </p>
            <h1 className="font-sans font-medium tracking-tightest leading-[1.02] text-ink text-[48px] md:text-[60px] lg:text-[64px]">
              The air people breathe,
              <br className="hidden md:block" />
              <span className="text-zinc-400"> rendered as deaths and dollars.</span>
            </h1>
            <p className="mt-8 text-[19px] leading-relaxed text-zinc-600 max-w-prose">
              Quantify how air pollution harms health in any community.
              Define a study area, select pollutants and dose-response
              relationships, and generate estimates
              of attributable deaths and economic costs.
            </p>

            <div className="mt-10 flex flex-wrap gap-4">
              <Link
                to="/analysis/1"
                className="inline-flex items-center gap-2 bg-emerald-800 text-white px-6 py-3 rounded-lg font-mono text-[13px] uppercase tracking-[0.12em] hover:bg-emerald-700 transition-colors duration-200"
              >
                Start analysis
                <svg className="w-4 h-4" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                  <path d="M4 2.5L7.5 6 4 9.5" />
                </svg>
              </Link>
            </div>

          </div>

          {/* Right column */}
          <div className="lg:col-span-6">
            <CityPanel />
          </div>
        </div>
      </section>

      {/* ── Stat band (pulled out of hero so it can breathe) ──── */}
      <section className="border-y border-zinc-200/80 bg-white/40">
        <div className="max-w-[1320px] mx-auto px-6 lg:px-10 py-16 lg:py-20">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-12 md:gap-16">
            <div>
              <p className="font-mono text-[56px] md:text-[72px] tracking-tightest text-rose-700 tabular-nums leading-none font-semibold">
                8.1M
              </p>
              <p className="mt-5 text-[18px] text-zinc-600 leading-snug max-w-[24ch]">
                Annual deaths linked to ambient air pollution
              </p>
              <p className="mt-2 font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-400">
                GBD 2023
              </p>
            </div>
            <div>
              <p className="font-mono text-[56px] md:text-[72px] tracking-tightest text-amber-700 tabular-nums leading-none font-semibold">
                99%
              </p>
              <p className="mt-5 text-[18px] text-zinc-600 leading-snug max-w-[24ch]">
                Of the world breathes air above WHO guidelines
              </p>
              <p className="mt-2 font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-400">
                WHO 2022
              </p>
            </div>
            <div>
              <p className="font-mono text-[56px] md:text-[72px] tracking-tightest text-accent-700 tabular-nums leading-none font-semibold">
                4
              </p>
              <p className="mt-5 text-[18px] text-zinc-600 leading-snug max-w-[24ch]">
                CRF functional forms, all with confidence intervals
              </p>
              <p className="mt-2 font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-400">
                Built in
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* ── Pipeline ───────────────────────────────────────────── */}
      <section>
        <div className="max-w-[1320px] mx-auto px-6 lg:px-10 py-24">
          <h2 className="text-[40px] md:text-[52px] lg:text-[56px] font-semibold tracking-tightest leading-[1.02] text-ink lg:whitespace-nowrap mb-16">
            Five moves from concentration to value.
          </h2>
          <PipelineDiagram />
        </div>
      </section>

      {/* ── Templates ──────────────────────────────────────────── */}
      <section className="border-t border-zinc-200/80">
        <div className="max-w-[1320px] mx-auto px-6 lg:px-10 py-20 lg:py-24">
          <h2 className="text-[32px] md:text-[40px] font-semibold tracking-tightest leading-[1.02] text-ink mb-4">
            Start from a template
          </h2>
          <p className="text-[17px] text-zinc-500 leading-relaxed mb-12 max-w-prose">
            Pre-configured analyses you can run immediately or customize for your study.
          </p>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-2 gap-4">
            {TEMPLATES.map(({ data, tag, color }) => (
              <TemplateCard
                key={data.name}
                data={data}
                tag={tag}
                color={color}
                onClick={() => handleTemplate(data)}
              />
            ))}
          </div>
        </div>
      </section>

      {/* ── City marquee ───────────────────────────────────────── */}
      <CityMarquee />

      {/* ── Footer ─────────────────────────────────────────────── */}
      <footer>
        <div className="max-w-[1320px] mx-auto px-6 lg:px-10 py-8 flex flex-col sm:flex-row gap-2 sm:gap-6 items-start sm:items-center justify-between">
          <p className="font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-400">
            Health Impact Assessment Walkthrough
          </p>
          <p className="text-[12px] text-zinc-500">
            Built with NumPy, FastAPI, and a quiet respect for confidence intervals.
          </p>
        </div>
      </footer>
    </div>
  )
}
