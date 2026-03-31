import Home from '../pages/Home'
import WizardLayout from '../components/WizardLayout'
import AnalysisStep from '../pages/AnalysisStep'
import AnalysisResults from '../pages/AnalysisResults'
import TimeseriesStep from '../pages/TimeseriesStep'
import TimeseriesResults from '../pages/TimeseriesResults'

const routes = [
  { path: '/', element: <Home /> },
  {
    path: '/analysis',
    element: <WizardLayout />,
    children: [
      { path: ':step', element: <AnalysisStep /> },
    ],
  },
  { path: '/analysis/results', element: <AnalysisResults /> },
  { path: '/timeseries/:step', element: <TimeseriesStep /> },
  { path: '/timeseries/results', element: <TimeseriesResults /> },
]

export default routes
