import { useState, useEffect, useRef } from 'react'
import { useParams, useNavigate, Outlet, Link } from 'react-router-dom'
import Markdown from 'react-markdown'
import useAnalysisStore from '../stores/useAnalysisStore'
import stepContent from '../content/steps/index'
import ErrorBoundary from './ErrorBoundary'

const STEPS = [
  { num: 1, label: 'Study Area' },
  { num: 2, label: 'Air Quality' },
  { num: 3, label: 'Population' },
  { num: 4, label: 'Health Data' },
  { num: 5, label: 'CRFs' },
  { num: 6, label: 'Run' },
  { num: 7, label: 'Valuation' },
]

const SIDEBAR_TABS = [
  { key: 'help', label: 'Help' },
  { key: 'uncertainties', label: 'Uncertainties' },
  { key: 'practices', label: 'Best Practices' },
]

// Sidebar content is now loaded from markdown files via stepContent

function ProgressBar({ currentStep, completedSteps, onStepClick }) {
  return (
    <nav className="bg-white border-b border-gray-200 px-4 py-3 flex items-center gap-4">
      <ol className="flex items-center min-w-max gap-1 flex-1 overflow-x-auto">
        {STEPS.map(({ num, label }, idx) => {
          const isActive = num === currentStep
          const isCompleted = completedSteps.includes(num)
          const isClickable = !isActive

          return (
            <li key={num} className="flex items-center">
              {idx > 0 && (
                <div
                  className={`w-8 h-0.5 mx-1 transition-colors duration-300 ${
                    isCompleted || isActive ? 'bg-blue-400' : 'bg-gray-200'
                  }`}
                />
              )}
              <button
                onClick={() => isClickable && onStepClick(num)}
                disabled={!isClickable}
                className={`
                  flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium
                  transition-all duration-300 whitespace-nowrap
                  ${isActive
                    ? 'bg-blue-600 text-white shadow-md shadow-blue-200'
                    : isCompleted
                      ? 'bg-blue-50 text-blue-700 hover:bg-blue-100 cursor-pointer'
                      : 'bg-gray-100 text-gray-500 hover:bg-gray-200 cursor-pointer'
                  }
                `}
              >
                <span
                  className={`
                    flex items-center justify-center w-6 h-6 rounded-full text-xs font-bold
                    transition-all duration-300
                    ${isActive
                      ? 'bg-white text-blue-600'
                      : isCompleted
                        ? 'bg-blue-200 text-blue-700'
                        : 'bg-gray-200 text-gray-400'
                    }
                  `}
                >
                  {isCompleted && !isActive ? '✓' : num}
                </span>
                <span className="hidden sm:inline">{label}</span>
              </button>
            </li>
          )
        })}
      </ol>
      <Link
        to="/"
        className="shrink-0 px-3 py-1.5 rounded-lg border border-gray-300 text-sm font-medium
                   text-gray-700 hover:bg-gray-100 active:bg-gray-200 transition-colors whitespace-nowrap"
      >
        New analysis
      </Link>
    </nav>
  )
}

function SidebarPanel({ currentStep, isOpen, onToggle }) {
  const [activeTab, setActiveTab] = useState('help')

  const markdownContent = stepContent[activeTab]?.[currentStep] || ''

  return (
    <>
      {/* Toggle button — always visible */}
      <button
        onClick={onToggle}
        className="absolute top-3 -left-10 z-10 w-8 h-8 flex items-center justify-center
                   bg-white border border-gray-200 rounded-l-lg shadow-sm
                   text-gray-500 hover:text-gray-700 hover:bg-gray-50 transition-colors"
        aria-label={isOpen ? 'Close sidebar' : 'Open sidebar'}
      >
        {isOpen ? '›' : '‹'}
      </button>

      <div
        className={`
          h-full bg-white border-l border-gray-200 flex flex-col
          transition-all duration-300 ease-in-out overflow-hidden
          ${isOpen ? 'w-80 opacity-100' : 'w-0 opacity-0'}
        `}
      >
        {/* Tabs */}
        <div className="flex border-b border-gray-200 shrink-0">
          {SIDEBAR_TABS.map(({ key, label }) => (
            <button
              key={key}
              onClick={() => setActiveTab(key)}
              className={`
                flex-1 px-3 py-2.5 text-xs font-medium transition-colors duration-200
                ${activeTab === key
                  ? 'text-blue-600 border-b-2 border-blue-600 bg-blue-50/50'
                  : 'text-gray-500 hover:text-gray-700 hover:bg-gray-50'
                }
              `}
            >
              {label}
            </button>
          ))}
        </div>

        {/* Content — rendered from markdown */}
        <div className="flex-1 overflow-y-auto p-4 prose prose-sm prose-gray max-w-none
                        prose-headings:text-gray-800 prose-h2:text-base prose-h2:mt-0 prose-h3:text-sm
                        prose-p:text-gray-600 prose-p:leading-relaxed
                        prose-li:text-gray-600 prose-li:leading-relaxed
                        prose-strong:text-gray-700
                        prose-table:text-xs prose-th:px-2 prose-th:py-1 prose-td:px-2 prose-td:py-1
                        prose-a:text-blue-600">
          {markdownContent ? (
            <Markdown>{markdownContent}</Markdown>
          ) : (
            <p className="text-sm text-gray-400">No content available for this step.</p>
          )}
        </div>
      </div>
    </>
  )
}

function NavigationBar({ currentStep, totalSteps, isStepValid, onBack, onNext }) {
  const isFirst = currentStep === 1
  const isLast = currentStep === totalSteps

  return (
    <div className="bg-white border-t border-gray-200 px-6 py-4 flex justify-between items-center shrink-0">
      <button
        onClick={onBack}
        disabled={isFirst}
        className={`
          px-6 py-2.5 rounded-lg border text-sm font-medium transition-all duration-200
          ${isFirst
            ? 'border-gray-200 text-gray-300 cursor-not-allowed'
            : 'border-gray-300 text-gray-700 hover:bg-gray-100 active:bg-gray-200'
          }
        `}
      >
        Back
      </button>

      <span className="text-sm text-gray-400">
        {currentStep} / {totalSteps}
      </span>

      <button
        onClick={onNext}
        disabled={!isStepValid}
        className={`
          px-6 py-2.5 rounded-lg text-sm font-medium transition-all duration-200
          ${isStepValid
            ? 'bg-blue-600 text-white hover:bg-blue-700 active:bg-blue-800 shadow-sm'
            : 'bg-blue-300 text-white cursor-not-allowed'
          }
        `}
      >
        {isLast ? 'View Results' : 'Next'}
      </button>
    </div>
  )
}

export default function WizardLayout() {
  const { step } = useParams()
  const navigate = useNavigate()
  const currentStep = Number(step) || 1
  const contentRef = useRef(null)
  const [transitioning, setTransitioning] = useState(false)
  const prevStepRef = useRef(currentStep)

  const {
    totalSteps,
    completedSteps,
    stepValidity,
    setCurrentStep,
    markStepCompleted,
  } = useAnalysisStore()

  // Default sidebar: open on desktop (>=1200px), closed otherwise
  const [sidebarOpen, setSidebarOpen] = useState(() =>
    typeof window !== 'undefined' ? window.innerWidth >= 1200 : true
  )

  const isStepValid = stepValidity[currentStep] ?? false

  // Sync store with URL param
  useEffect(() => {
    setCurrentStep(currentStep)
  }, [currentStep, setCurrentStep])

  // Animate content on step change
  useEffect(() => {
    if (prevStepRef.current !== currentStep) {
      setTransitioning(true)
      const timer = setTimeout(() => setTransitioning(false), 50)
      prevStepRef.current = currentStep
      return () => clearTimeout(timer)
    }
  }, [currentStep])

  const handleStepClick = (stepNum) => {
    navigate(`/analysis/${stepNum}`)
  }

  const goBack = () => {
    if (currentStep > 1) {
      navigate(`/analysis/${currentStep - 1}`)
    }
  }

  const goNext = () => {
    markStepCompleted(currentStep)
    if (currentStep < totalSteps) {
      navigate(`/analysis/${currentStep + 1}`)
    } else {
      navigate('/results')
    }
  }

  return (
    <div className="h-screen flex flex-col bg-gray-50">
      {/* Top: Progress Bar */}
      <ProgressBar
        currentStep={currentStep}
        completedSteps={completedSteps}
        onStepClick={handleStepClick}
      />

      {/* Middle: Content + Sidebar */}
      <div className="flex-1 flex overflow-hidden">
        {/* Main content area */}
        <main className="flex-1 overflow-y-auto p-6 lg:p-8 pb-12">
          <div
            ref={contentRef}
            className={`
              max-w-6xl mx-auto
              transition-all duration-300 ease-in-out
              ${transitioning ? 'opacity-0 translate-y-2' : 'opacity-100 translate-y-0'}
            `}
          >
            <ErrorBoundary fallbackMessage="This step encountered an error. Try going back or refreshing.">
              <Outlet />
            </ErrorBoundary>
          </div>
        </main>

        {/* Right sidebar */}
        <aside className="relative shrink-0">
          <SidebarPanel
            currentStep={currentStep}
            isOpen={sidebarOpen}
            onToggle={() => setSidebarOpen((prev) => !prev)}
          />
        </aside>
      </div>

      {/* Bottom: Navigation */}
      <NavigationBar
        currentStep={currentStep}
        totalSteps={totalSteps}
        isStepValid={isStepValid}
        onBack={goBack}
        onNext={goNext}
      />
    </div>
  )
}
