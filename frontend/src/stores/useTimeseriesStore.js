import { create } from 'zustand'
import { persist } from 'zustand/middleware'

const DEFAULT_STATE = {
  currentStep: 1,
  totalSteps: 7,
  completedSteps: [],
  stepValidity: { 1: false, 2: false, 3: false, 4: false, 5: false, 6: false, 7: false },

  // Step 1: Study area (reuses same shape as main wizard)
  step1: {
    studyArea: { type: 'country', id: '', name: '' },
    pollutant: null,
    analysisName: '',
  },

  // Step 2: Time-series air quality
  step2: {
    baseline: {
      type: 'csv',       // 'csv' or 'manual'
      csvData: null,      // parsed array of { date, concentration }
      fileName: null,
    },
    control: {
      type: 'constant',   // 'constant' or 'csv'
      value: null,
      csvData: null,
      fileName: null,
    },
  },

  // Step 3: Population
  step3: {
    totalPopulation: null,
  },

  // Step 4: Health data
  step4: {
    baselineIncidence: null, // annual rate per person
  },

  // Step 5: Short-term CRFs
  step5: {
    selectedCRFs: [],
  },

  // Step 6: Run options
  step6: {
    monteCarloIterations: 1000,
  },

  // Step 7: Review & run (no additional state)
  step7: {},

  // Results
  results: null,
}

const useTimeseriesStore = create(
  persist(
    (set, get) => ({
      ...JSON.parse(JSON.stringify(DEFAULT_STATE)),

      setCurrentStep: (step) => set({ currentStep: step }),

      markStepCompleted: (step) =>
        set((state) => ({
          completedSteps: state.completedSteps.includes(step)
            ? state.completedSteps
            : [...state.completedSteps, step].sort((a, b) => a - b),
        })),

      setStepValidity: (step, valid) =>
        set((state) => {
          if (state.stepValidity[step] === valid) return state
          return { stepValidity: { ...state.stepValidity, [step]: valid } }
        }),

      setStep1: (patch) => set((state) => ({ step1: { ...state.step1, ...patch } })),
      setStep2: (patch) => set((state) => ({ step2: { ...state.step2, ...patch } })),
      setStep3: (patch) => set((state) => ({ step3: { ...state.step3, ...patch } })),
      setStep4: (patch) => set((state) => ({ step4: { ...state.step4, ...patch } })),
      setStep5: (patch) => set((state) => ({ step5: { ...state.step5, ...patch } })),
      setStep6: (patch) => set((state) => ({ step6: { ...state.step6, ...patch } })),
      setStep7: (patch) => set((state) => ({ step7: { ...state.step7, ...patch } })),

      setResults: (results) => set({ results }),

      reset: () => set(JSON.parse(JSON.stringify(DEFAULT_STATE))),

      exportConfig: () => {
        const { step1, step2, step3, step4, step5, step6, step7 } = get()
        return JSON.parse(JSON.stringify({ step1, step2, step3, step4, step5, step6, step7 }))
      },
    }),
    {
      name: 'hia-timeseries',
      version: 1,
      partialize: (state) => ({
        currentStep: state.currentStep,
        completedSteps: state.completedSteps,
        stepValidity: state.stepValidity,
        step1: state.step1,
        step2: state.step2,
        step3: state.step3,
        step4: state.step4,
        step5: state.step5,
        step6: state.step6,
        step7: state.step7,
      }),
    },
  ),
)

export default useTimeseriesStore
