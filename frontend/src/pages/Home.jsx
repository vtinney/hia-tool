import { Link } from 'react-router-dom'

const TEMPLATES = [
  {
    name: 'U.S. National PM2.5 (EPA CRFs)',
    description: 'Nationwide long-term PM2.5 analysis using EPA concentration-response functions.',
  },
  {
    name: 'U.S. Census Tract PM2.5 (Environmental Justice)',
    description: 'Tract-level disparities analysis with demographic breakdowns.',
  },
  {
    name: 'Single City PM2.5 (WHO Guideline)',
    description: 'City-scale assessment benchmarked against WHO air quality guidelines.',
  },
  {
    name: 'Global PM2.5 Burden (GBD 2023 MR-BRT)',
    description: 'Global burden of disease estimates using the latest MR-BRT spline model.',
  },
  {
    name: 'Mexico NO2 (TROPOMI + Eum CRF)',
    description: 'Satellite-derived NO2 exposure with the Eum et al. risk function.',
  },
  {
    name: 'Brazil Amazonian Cities PM2.5',
    description: 'Fire-season PM2.5 health impacts across Amazonian urban areas.',
  },
]

export default function Home() {
  return (
    <div className="min-h-screen bg-gradient-to-b from-slate-50 to-white">
      {/* Header */}
      <header className="pt-16 pb-12 px-6 text-center">
        <h1 className="text-4xl md:text-5xl font-bold text-slate-900 tracking-tight">
          Health Impact Assessment Walkthrough
        </h1>
        <p className="mt-4 text-lg md:text-xl text-slate-500 max-w-2xl mx-auto leading-relaxed">
          Estimate the health and economic impacts of changes in air quality
        </p>
      </header>

      {/* Main entry points */}
      <section className="max-w-5xl mx-auto px-6 pb-16">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Long-term card */}
          <Link
            to="/analysis/1"
            className="group relative flex flex-col justify-between rounded-2xl border border-slate-200 bg-white p-8 shadow-sm transition-all hover:shadow-md hover:border-blue-300"
          >
            <div>
              <span className="inline-flex items-center justify-center w-10 h-10 rounded-lg bg-blue-50 text-blue-600 mb-5">
                <svg xmlns="http://www.w3.org/2000/svg" className="w-5 h-5" viewBox="0 0 20 20" fill="currentColor">
                  <path d="M2 11a1 1 0 011-1h2a1 1 0 011 1v5a1 1 0 01-1 1H3a1 1 0 01-1-1v-5zm6-4a1 1 0 011-1h2a1 1 0 011 1v9a1 1 0 01-1 1H9a1 1 0 01-1-1V7zm6-3a1 1 0 011-1h2a1 1 0 011 1v12a1 1 0 01-1 1h-2a1 1 0 01-1-1V4z" />
                </svg>
              </span>
              <h2 className="text-xl font-semibold text-slate-900 mb-2">
                Long-term Exposure Analysis
              </h2>
              <p className="text-slate-500 leading-relaxed">
                Chronic exposure assessment using annual average concentrations
              </p>
            </div>
            <div className="mt-6 flex items-center text-sm font-medium text-blue-600 group-hover:text-blue-700">
              Begin analysis
              <svg xmlns="http://www.w3.org/2000/svg" className="ml-1.5 w-4 h-4 transition-transform group-hover:translate-x-0.5" viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M10.293 3.293a1 1 0 011.414 0l6 6a1 1 0 010 1.414l-6 6a1 1 0 01-1.414-1.414L14.586 11H3a1 1 0 110-2h11.586l-4.293-4.293a1 1 0 010-1.414z" clipRule="evenodd" />
              </svg>
            </div>
          </Link>

          {/* Time-series card */}
          <Link
            to="/timeseries/1"
            className="group relative flex flex-col justify-between rounded-2xl border border-slate-200 bg-white p-8 shadow-sm transition-all hover:shadow-md hover:border-teal-300"
          >
            <div>
              <span className="inline-flex items-center justify-center w-10 h-10 rounded-lg bg-teal-50 text-teal-600 mb-5">
                <svg xmlns="http://www.w3.org/2000/svg" className="w-5 h-5" viewBox="0 0 20 20" fill="currentColor">
                  <path fillRule="evenodd" d="M3 3a1 1 0 000 2v8a2 2 0 002 2h2.586l-1.293 1.293a1 1 0 101.414 1.414L10 15.414l2.293 2.293a1 1 0 001.414-1.414L12.414 15H15a2 2 0 002-2V5a1 1 0 100-2H3zm11 4a1 1 0 10-2 0v4a1 1 0 102 0V7zm-3 1a1 1 0 10-2 0v3a1 1 0 102 0V8zM8 9a1 1 0 00-2 0v2a1 1 0 102 0V9z" clipRule="evenodd" />
                </svg>
              </span>
              <h2 className="text-xl font-semibold text-slate-900 mb-2">
                Time-series Analysis
              </h2>
              <p className="text-slate-500 leading-relaxed">
                Short-term daily exposure assessment
              </p>
            </div>
            <div className="mt-6 flex items-center text-sm font-medium text-teal-600 group-hover:text-teal-700">
              Begin analysis
              <svg xmlns="http://www.w3.org/2000/svg" className="ml-1.5 w-4 h-4 transition-transform group-hover:translate-x-0.5" viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M10.293 3.293a1 1 0 011.414 0l6 6a1 1 0 010 1.414l-6 6a1 1 0 01-1.414-1.414L14.586 11H3a1 1 0 110-2h11.586l-4.293-4.293a1 1 0 010-1.414z" clipRule="evenodd" />
              </svg>
            </div>
          </Link>
        </div>
      </section>

      {/* Quick-Start Templates */}
      <section className="max-w-5xl mx-auto px-6 pb-20">
        <div className="mb-8">
          <h2 className="text-2xl font-semibold text-slate-900">Quick-Start Templates</h2>
          <p className="mt-1 text-slate-500">
            Pre-configured analyses for common scenarios
          </p>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5">
          {TEMPLATES.map((template) => (
            <div
              key={template.name}
              className="flex flex-col justify-between rounded-xl border border-slate-200 bg-white p-6 shadow-sm"
            >
              <div>
                <h3 className="font-medium text-slate-900 leading-snug mb-2">
                  {template.name}
                </h3>
                <p className="text-sm text-slate-400 leading-relaxed">
                  {template.description}
                </p>
              </div>
              <div className="mt-5 relative group/btn inline-flex self-start">
                <button
                  disabled
                  className="px-4 py-1.5 text-sm font-medium rounded-lg bg-slate-100 text-slate-400 cursor-not-allowed"
                >
                  Start
                </button>
                <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-2.5 py-1 text-xs font-medium text-white bg-slate-800 rounded-md opacity-0 group-hover/btn:opacity-100 transition-opacity pointer-events-none whitespace-nowrap">
                  Coming soon
                </span>
              </div>
            </div>
          ))}
        </div>
      </section>
    </div>
  )
}
