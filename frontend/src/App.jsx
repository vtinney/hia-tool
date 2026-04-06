import { useRoutes } from 'react-router-dom'
import routes from './routes'
import ErrorBoundary from './components/ErrorBoundary'

export default function App() {
  const element = useRoutes(routes)
  return (
    <ErrorBoundary fallbackMessage="The application encountered an unexpected error. Your analysis data is saved — try refreshing the page.">
      <div className="min-h-screen bg-gray-50">
        {element}
      </div>
    </ErrorBoundary>
  )
}
