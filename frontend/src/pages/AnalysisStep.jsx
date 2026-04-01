import { useParams } from 'react-router-dom'
import Step1StudyArea from './steps/Step1StudyArea'
import Step2AirQuality from './steps/Step2AirQuality'
import Step3Population from './steps/Step3Population'
import Step4HealthData from './steps/Step4HealthData'
import Step5CRFs from './steps/Step5CRFs'

const STEP_TITLES = {
  1: 'Study Area',
  2: 'Air Quality',
  3: 'Population',
  4: 'Health Data',
  5: 'Concentration-Response Functions',
  6: 'Run Analysis',
  7: 'Valuation',
}

const STEP_COMPONENTS = {
  1: Step1StudyArea,
  2: Step2AirQuality,
  3: Step3Population,
  4: Step4HealthData,
  5: Step5CRFs,
}

export default function AnalysisStep() {
  const { step } = useParams()
  const currentStep = Number(step)

  const StepComponent = STEP_COMPONENTS[currentStep]

  if (StepComponent) {
    return <StepComponent />
  }

  // Placeholder for steps not yet implemented
  return (
    <>
      <h1 className="text-3xl font-bold text-gray-900 mb-6">
        {STEP_TITLES[currentStep] || `Step ${currentStep}`}
      </h1>
      <div className="bg-white rounded-xl shadow p-8 min-h-[200px]">
        <p className="text-gray-400">Step {currentStep} content goes here.</p>
      </div>
    </>
  )
}
