import { useCallback } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import useAnalysisStore from '../stores/useAnalysisStore'

import usNationalPm25 from '../data/templates/us_national_pm25.json'
import usTractPm25Ej from '../data/templates/us_tract_pm25_ej.json'
import singleCityPm25Who from '../data/templates/single_city_pm25_who.json'
import globalPm25Gbd from '../data/templates/global_pm25_gbd.json'
import mexicoNo2 from '../data/templates/mexico_no2.json'
import brazilAmazonPm25 from '../data/templates/brazil_amazon_pm25.json'

const TEMPLATES = [
  { data: usNationalPm25, badge: 'PM2.5', badgeColor: 'bg-blue-100 text-blue-700' },
  { data: usTractPm25Ej, badge: 'Spatial', badgeColor: 'bg-purple-100 text-purple-700' },
  { data: singleCityPm25Who, badge: 'PM2.5', badgeColor: 'bg-blue-100 text-blue-700' },
  { data: globalPm25Gbd, badge: 'GBD', badgeColor: 'bg-teal-100 text-teal-700' },
  { data: mexicoNo2, badge: 'NO₂', badgeColor: 'bg-amber-100 text-amber-700' },
  { data: brazilAmazonPm25, badge: 'PM2.5', badgeColor: 'bg-green-100 text-green-700' },
]

export default function Home() {
  const navigate = useNavigate()
  const loadFromTemplate = useAnalysisStore((s) => s.loadFromTemplate)

  const handleTemplate = useCallback((template) => {
    loadFromTemplate(template)
    navigate('/analysis/1')
  }, [loadFromTemplate, navigate])

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
          {TEMPLATES.map(({ data, badge, badgeColor }) => (
            <div
              key={data.name}
              className="flex flex-col justify-between rounded-xl border border-slate-200 bg-white p-6 shadow-sm hover:shadow-md hover:border-slate-300 transition-all"
            >
              <div>
                <div className="flex items-center gap-2 mb-3">
                  <span className={`text-[10px] font-bold uppercase px-2 py-0.5 rounded-full ${badgeColor}`}>
                    {badge}
                  </span>
                </div>
                <h3 className="font-medium text-slate-900 leading-snug mb-2">
                  {data.name}
                </h3>
                <p className="text-sm text-slate-400 leading-relaxed">
                  {data.description}
                </p>
              </div>
              <button
                onClick={() => handleTemplate(data)}
                className="mt-5 self-start px-4 py-1.5 text-sm font-medium rounded-lg bg-blue-600 text-white hover:bg-blue-700 transition-colors"
              >
                Use Template
              </button>
            </div>
          ))}
        </div>
      </section>
    </div>
  )
}
