import { Navigate } from 'react-router-dom'
import Home from '../pages/Home'
import WizardLayout from '../components/WizardLayout'
import AnalysisStep from '../pages/AnalysisStep'
import Results from '../pages/Results'
import NotFound from '../pages/NotFound'

const routes = [
  { path: '/', element: <Home /> },
  { path: '/analysis', element: <Navigate to="/analysis/1" replace /> },
  {
    path: '/analysis/:step',
    element: <WizardLayout />,
    children: [
      { index: true, element: <AnalysisStep /> },
    ],
  },
  { path: '/results', element: <Results /> },
  { path: '*', element: <NotFound /> },
]

export default routes
