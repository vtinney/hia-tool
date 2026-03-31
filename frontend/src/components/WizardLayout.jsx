import { useState, useEffect, useRef } from 'react'
import { useParams, useNavigate, Outlet } from 'react-router-dom'
import useAnalysisStore from '../stores/useAnalysisStore'

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

const SIDEBAR_CONTENT = {
  help: {
    1: 'Define the geographic boundaries and time period for your health impact analysis. The study area determines which population and environmental data will be used.',
    2: 'Specify baseline and scenario air quality concentrations. These values drive the exposure-response calculation at the core of the HIA.',
    3: 'Enter or upload population counts for the study area. Population size directly scales the estimated health impacts.',
    4: 'Select health endpoints and provide baseline incidence rates. These determine which health effects are quantified in the analysis.',
    5: 'Choose concentration-response functions (CRFs) from the epidemiological literature. CRFs link changes in air quality to changes in health outcomes.',
    6: 'Review your inputs and run the health impact calculation. The model applies the CRFs to your exposure and population data.',
    7: 'Assign economic values to the estimated health impacts using willingness-to-pay or cost-of-illness approaches.',
  },
  uncertainties: {
    1: 'Study area boundaries may not align perfectly with available air quality monitoring or population data grids.',
    2: 'Air quality concentrations are often interpolated from sparse monitoring networks. Spatial resolution affects accuracy.',
    3: 'Population estimates rely on census data that may be outdated. Subgroup distributions carry additional uncertainty.',
    4: 'Baseline incidence rates may vary by subpopulation and may not reflect current local conditions.',
    5: 'CRFs are derived from specific study populations and may not be fully transferable to your study area.',
    6: 'Model results are deterministic point estimates unless Monte Carlo uncertainty analysis is enabled.',
    7: 'Economic valuation estimates vary widely across methodologies and reflect societal willingness-to-pay at a point in time.',
  },
  practices: {
    1: 'Use the finest spatial resolution available. Align study boundaries with administrative units for easier interpretation.',
    2: 'Use monitored data where available. Document any spatial interpolation or modeling assumptions.',
    3: 'Use age-stratified population data when possible. Match population years to your study period.',
    4: 'Use local incidence rates when available. National rates are acceptable but note the limitation.',
    5: 'Prefer CRFs from meta-analyses or multi-city studies. Report the confidence interval alongside the central estimate.',
    6: 'Run sensitivity analyses on key inputs. Document all parameter choices for reproducibility.',
    7: 'Report a range of economic values rather than a single point estimate. Clearly state the valuation methodology used.',
  },
}

function ProgressBar({ currentStep, completedSteps, onStepClick }) {
  return (
    <nav className="bg-white border-b border-gray-200 px-4 py-3 overflow-x-auto">
      <ol className="flex items-center min-w-max gap-1">
        {STEPS.map(({ num, label }, idx) => {
          const isActive = num === currentStep
          const isCompleted = completedSteps.includes(num)
          const isClickable = isCompleted && !isActive

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
                      : 'bg-gray-100 text-gray-400 cursor-default'
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
    </nav>
  )
}

function SidebarPanel({ currentStep, isOpen, onToggle }) {
  const [activeTab, setActiveTab] = useState('help')

  const content = SIDEBAR_CONTENT[activeTab]?.[currentStep] || 'No content available for this step.'

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

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-4">
          <p className="text-sm text-gray-600 leading-relaxed">{content}</p>
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

  // Default sidebar: open on desktop (>=1024px), closed otherwise
  const [sidebarOpen, setSidebarOpen] = useState(() =>
    typeof window !== 'undefined' ? window.innerWidth >= 1024 : true
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
      navigate('/analysis/results')
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
        <main className="flex-1 overflow-y-auto p-6 lg:p-8">
          <div
            ref={contentRef}
            className={`
              max-w-6xl mx-auto
              transition-all duration-300 ease-in-out
              ${transitioning ? 'opacity-0 translate-y-2' : 'opacity-100 translate-y-0'}
            `}
          >
            <Outlet />
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
