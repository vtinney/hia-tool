import Home from '../pages/Home'
import Results from '../pages/Results'
import NotFound from '../pages/NotFound'

const routes = [
  { path: '/', element: <Home /> },
  { path: '/results', element: <Results /> },
  { path: '*', element: <NotFound /> },
]

export default routes
