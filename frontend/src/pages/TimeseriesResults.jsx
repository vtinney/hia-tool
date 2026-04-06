import { useState } from 'react'
import { Link } from 'react-router-dom'
import {
  LineChart, Line, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import useTimeseriesStore from '../stores/useTimeseriesStore'

// ── Formatting helpers ──────────────────────────────────────────

function fmtNumber(n, decimals = 0) {
  if (n == null) return '—'
  return Number(n).toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  })
}

function fmtPercent(n) {
  if (n == null) return '—'
  return `${(Number(n) * 100).toFixed(2)}%`
}

// ── Summary card ────────────────────────────────────────────────

function SummaryCard({ label, value, ci, subtitle, bgClass }) {
  return (
    <div className={`rounded-2xl p-6 shadow-sm ${bgClass}`}>
      <p className="text-sm font-medium text-gray-500 mb-1">{label}</p>
      <p className="text-3xl font-bold text-gray-900 leading-tight">{value}</p>
      {ci && <p className="text-sm text-gray-500 mt-1">95% CI: {ci}</p>}
      {subtitle && <p className="text-xs text-gray-400 mt-2">{subtitle}</p>}
    </div>
  )
}

// ── Tabs ────────────────────────────────────────────────────────

const TABS = [
  { key: 'timeseries', label: 'Time Series' },
  { key: 'monthly', label: 'Monthly' },
  { key: 'table', label: 'Table' },
]

// ── Main component ──────────────────────────────────────────────

export default function TimeseriesResults() {
  const { results, step1 } = useTimeseriesStore()
  const [activeTab, setActiveTab] = useState('timeseries')

  if (!results) {
    return (
      <div className="min-h-screen bg-gradient-to-b from-slate-50 to-white flex items-center justify-center">
        <div className="text-center">
          <p className="text-gray-400 text-lg font-medium">No results yet</p>
          <p className="text-gray-300 text-sm mt-1">Complete the time-series wizard to see results.</p>
          <Link to="/timeseries/1" className="inline-block mt-6 px-6 py-2 rounded-lg bg-green-600 text-white hover:bg-green-700 text-sm font-medium">
            Start Analysis
          </Link>
        </div>
      </div>
    )
  }

  const { daily, monthly, totalCases } = results
  const nDays = daily?.length || 0

  // Prepare dual-axis chart data (subsample if > 365 days for perf)
  const stride = nDays > 730 ? Math.ceil(nDays / 365) : 1
  const chartData = daily
    ? daily.filter((_, i) => i % stride === 0).map((d) => ({
        date: d.date,
        concentration: d.concentration,
        cumulative: Math.round(d.cumulativeCases * 100) / 100,
      }))
    : []

  return (
    <div className="min-h-screen bg-gradient-to-b from-slate-50 to-white">
      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-10">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="text-3xl font-bold text-slate-900">Time-Series Results</h1>
            {step1?.analysisName && <p className="text-slate-500 mt-1">{step1.analysisName}</p>}
          </div>
          <div className="flex gap-3">
            <Link to="/timeseries/7" className="px-5 py-2 rounded-lg border border-gray-300 text-gray-700 hover:bg-gray-100 text-sm font-medium">
              Back to Wizard
            </Link>
            <Link to="/" className="px-5 py-2 rounded-lg bg-green-600 text-white hover:bg-green-700 text-sm font-medium">
              New Analysis
            </Link>
          </div>
        </div>

        {/* Summary Cards */}
        <div className="grid gap-4 sm:grid-cols-3 mb-8">
          <SummaryCard
            label="Total Attributable Cases"
            value={fmtNumber(totalCases?.mean)}
            ci={totalCases ? `${fmtNumber(totalCases.lower95)} – ${fmtNumber(totalCases.upper95)}` : null}
            subtitle={`Over ${nDays} days`}
            bgClass="bg-green-50"
          />
          <SummaryCard
            label="Average Daily Cases"
            value={nDays > 0 ? fmtNumber(totalCases?.mean / nDays, 2) : '—'}
            subtitle="Attributable cases per day"
            bgClass="bg-blue-50"
          />
          <SummaryCard
            label="Period"
            value={nDays > 0 ? `${daily[0].date} to ${daily[nDays - 1].date}` : '—'}
            subtitle={`${nDays} days analyzed`}
            bgClass="bg-teal-50"
          />
        </div>

        {/* Tabs */}
        <div className="bg-white rounded-2xl shadow-sm overflow-hidden">
          <div className="border-b border-gray-200">
            <nav className="flex -mb-px">
              {TABS.map((tab) => (
                <button
                  key={tab.key}
                  onClick={() => setActiveTab(tab.key)}
                  className={`px-6 py-3 text-sm font-medium border-b-2 transition-colors
                    ${activeTab === tab.key
                      ? 'border-green-600 text-green-600'
                      : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'}`}
                >
                  {tab.label}
                </button>
              ))}
            </nav>
          </div>

          <div className="p-6">
            {/* Time Series Chart — dual axis */}
            {activeTab === 'timeseries' && (
              <div>
                <h3 className="text-sm font-semibold text-gray-700 mb-4">Daily Concentration & Cumulative Attributable Cases</h3>
                <div className="h-80">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                      <XAxis dataKey="date" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
                      <YAxis yAxisId="left" tick={{ fontSize: 10 }} label={{ value: 'μg/m³', angle: -90, position: 'insideLeft', style: { fontSize: 11 } }} />
                      <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 10 }} label={{ value: 'Cumulative cases', angle: 90, position: 'insideRight', style: { fontSize: 11 } }} />
                      <Tooltip contentStyle={{ fontSize: 12 }} />
                      <Legend wrapperStyle={{ fontSize: 12 }} />
                      <Line yAxisId="left" type="monotone" dataKey="concentration" stroke="#3b82f6" dot={false} strokeWidth={1.5} name="Concentration (μg/m³)" />
                      <Line yAxisId="right" type="monotone" dataKey="cumulative" stroke="#10b981" dot={false} strokeWidth={2} name="Cumulative cases" />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
            )}

            {/* Monthly Bar Chart */}
            {activeTab === 'monthly' && (
              <div>
                <h3 className="text-sm font-semibold text-gray-700 mb-4">Monthly Attributable Cases</h3>
                <div className="h-80">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={monthly}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                      <XAxis dataKey="month" tick={{ fontSize: 10 }} />
                      <YAxis tick={{ fontSize: 10 }} />
                      <Tooltip contentStyle={{ fontSize: 12 }} formatter={(v) => fmtNumber(v, 1)} />
                      <Bar dataKey="cases" fill="#14b8a6" radius={[4, 4, 0, 0]} name="Attributable cases" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            )}

            {/* Results Table */}
            {activeTab === 'table' && (
              <div className="overflow-x-auto">
                <table className="w-full text-sm border border-gray-200 rounded-lg overflow-hidden">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="px-4 py-2 text-left font-medium text-gray-600 border-b border-gray-200">CRF Study</th>
                      <th className="px-4 py-2 text-left font-medium text-gray-600 border-b border-gray-200">Endpoint</th>
                      <th className="px-4 py-2 text-right font-medium text-gray-600 border-b border-gray-200">Total Cases</th>
                      <th className="px-4 py-2 text-right font-medium text-gray-600 border-b border-gray-200">95% CI</th>
                      <th className="px-4 py-2 text-right font-medium text-gray-600 border-b border-gray-200">Attr. Fraction</th>
                      <th className="px-4 py-2 text-right font-medium text-gray-600 border-b border-gray-200">Rate /100k</th>
                    </tr>
                  </thead>
                  <tbody>
                    {results.results?.map((r, i) => (
                      <tr key={r.crfId} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                        <td className="px-4 py-2 border-b border-gray-100 text-gray-700">{r.study}</td>
                        <td className="px-4 py-2 border-b border-gray-100 text-gray-900 font-medium">{r.endpoint}</td>
                        <td className="px-4 py-2 border-b border-gray-100 text-right font-semibold text-gray-900">{fmtNumber(r.totalCases.mean)}</td>
                        <td className="px-4 py-2 border-b border-gray-100 text-right text-gray-500 text-xs">
                          {fmtNumber(r.totalCases.lower95)} – {fmtNumber(r.totalCases.upper95)}
                        </td>
                        <td className="px-4 py-2 border-b border-gray-100 text-right text-gray-700">{fmtPercent(r.attributableFraction?.mean)}</td>
                        <td className="px-4 py-2 border-b border-gray-100 text-right text-gray-700">{fmtNumber(r.attributableRate?.mean, 1)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
