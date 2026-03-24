import { Link } from 'react-router-dom'
import useAnalysisStore from '../store/useAnalysisStore'

export default function AnalysisResults() {
  const { results } = useAnalysisStore()

  return (
    <div className="max-w-3xl mx-auto p-8">
      <h1 className="text-3xl font-bold text-gray-900 mb-6">
        Long-Term Analysis Results
      </h1>

      <div className="bg-white rounded-xl shadow p-8 mb-8 min-h-[300px]">
        {results ? (
          <pre className="text-sm text-gray-700">
            {JSON.stringify(results, null, 2)}
          </pre>
        ) : (
          <p className="text-gray-400">
            No results yet. Complete the analysis wizard to see results.
          </p>
        )}
      </div>

      <div className="flex gap-4">
        <Link
          to="/analysis/7"
          className="px-6 py-2 rounded-lg border border-gray-300 text-gray-700 hover:bg-gray-100"
        >
          Back to Step 7
        </Link>
        <Link
          to="/"
          className="px-6 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700"
        >
          Start Over
        </Link>
      </div>
    </div>
  )
}
