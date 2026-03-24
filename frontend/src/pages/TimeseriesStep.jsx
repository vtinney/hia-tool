import { useParams, useNavigate } from 'react-router-dom'
import useAnalysisStore from '../store/useAnalysisStore'

const STEP_TITLES = {
  1: 'Select Pollutant',
  2: 'Time Range',
  3: 'Monitor Data',
  4: 'Lag Structure',
  5: 'Health Endpoints',
  6: 'Concentration-Response Function',
  7: 'Review & Submit',
}

export default function TimeseriesStep() {
  const { step } = useParams()
  const navigate = useNavigate()
  const currentStep = Number(step)
  const { totalSteps } = useAnalysisStore()

  const goNext = () => {
    if (currentStep < totalSteps) {
      navigate(`/timeseries/${currentStep + 1}`)
    } else {
      navigate('/timeseries/results')
    }
  }

  const goBack = () => {
    if (currentStep > 1) {
      navigate(`/timeseries/${currentStep - 1}`)
    } else {
      navigate('/')
    }
  }

  return (
    <div className="max-w-3xl mx-auto p-8">
      <div className="mb-8">
        <p className="text-sm text-gray-500 mb-1">
          Step {currentStep} of {totalSteps}
        </p>
        <div className="w-full bg-gray-200 rounded-full h-2">
          <div
            className="bg-green-600 h-2 rounded-full transition-all"
            style={{ width: `${(currentStep / totalSteps) * 100}%` }}
          />
        </div>
      </div>

      <h1 className="text-3xl font-bold text-gray-900 mb-6">
        {STEP_TITLES[currentStep] || `Step ${currentStep}`}
      </h1>

      <div className="bg-white rounded-xl shadow p-8 mb-8 min-h-[200px]">
        <p className="text-gray-400">Step {currentStep} content goes here.</p>
      </div>

      <div className="flex justify-between">
        <button
          onClick={goBack}
          className="px-6 py-2 rounded-lg border border-gray-300 text-gray-700 hover:bg-gray-100"
        >
          Back
        </button>
        <button
          onClick={goNext}
          className="px-6 py-2 rounded-lg bg-green-600 text-white hover:bg-green-700"
        >
          {currentStep < totalSteps ? 'Next' : 'View Results'}
        </button>
      </div>
    </div>
  )
}
