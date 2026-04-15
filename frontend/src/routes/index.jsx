import Home from '../pages/Home'
import WizardLayout from '../components/WizardLayout'
import Step1StudyArea from '../pages/steps/Step1StudyArea'
import Step2AirQuality from '../pages/steps/Step2AirQuality'
import Step3Population from '../pages/steps/Step3Population'
import Step4HealthData from '../pages/steps/Step4HealthData'
import Step5CRFs from '../pages/steps/Step5CRFs'
import Step6Run from '../pages/steps/Step6Run'
import Step7Valuation from '../pages/steps/Step7Valuation'
import Results from '../pages/Results'
import NotFound from '../pages/NotFound'

const routes = [
  { path: '/', element: <Home /> },
  {
    path: '/analysis',
    element: <WizardLayout />,
    children: [
      { path: '1', element: <Step1StudyArea /> },
      { path: '2', element: <Step2AirQuality /> },
      { path: '3', element: <Step3Population /> },
      { path: '4', element: <Step4HealthData /> },
      { path: '5', element: <Step5CRFs /> },
      { path: '6', element: <Step6Run /> },
      { path: '7', element: <Step7Valuation /> },
    ],
  },
  { path: '/results', element: <Results /> },
  { path: '*', element: <NotFound /> },
]

export default routes
