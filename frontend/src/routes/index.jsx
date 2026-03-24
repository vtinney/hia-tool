import Home from '../pages/Home'
import AnalysisStep from '../pages/AnalysisStep'
import AnalysisResults from '../pages/AnalysisResults'
import TimeseriesStep from '../pages/TimeseriesStep'
import TimeseriesResults from '../pages/TimeseriesResults'

const routes = [
  { path: '/', element: <Home /> },
  { path: '/analysis/:step', element: <AnalysisStep /> },
  { path: '/analysis/results', element: <AnalysisResults /> },
  { path: '/timeseries/:step', element: <TimeseriesStep /> },
  { path: '/timeseries/results', element: <TimeseriesResults /> },
]

export default routes
