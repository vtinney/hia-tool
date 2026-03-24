import { create } from 'zustand'

const useAnalysisStore = create((set) => ({
  // Current wizard state
  currentStep: 1,
  totalSteps: 7,

  // Long-term analysis data
  analysis: {
    pollutant: null,
    baselineConcentration: null,
    scenarioConcentration: null,
    populationSize: null,
    incidenceRate: null,
    healthEndpoints: [],
    crFunction: null,
  },

  // Time-series analysis data
  timeseries: {
    pollutant: null,
    timeRange: null,
    monitorData: null,
    lag: null,
    healthEndpoints: [],
    crFunction: null,
  },

  // Results
  results: null,

  // Actions
  setCurrentStep: (step) => set({ currentStep: step }),

  updateAnalysis: (field, value) =>
    set((state) => ({
      analysis: { ...state.analysis, [field]: value },
    })),

  updateTimeseries: (field, value) =>
    set((state) => ({
      timeseries: { ...state.timeseries, [field]: value },
    })),

  setResults: (results) => set({ results }),

  reset: () =>
    set({
      currentStep: 1,
      analysis: {
        pollutant: null,
        baselineConcentration: null,
        scenarioConcentration: null,
        populationSize: null,
        incidenceRate: null,
        healthEndpoints: [],
        crFunction: null,
      },
      timeseries: {
        pollutant: null,
        timeRange: null,
        monitorData: null,
        lag: null,
        healthEndpoints: [],
        crFunction: null,
      },
      results: null,
    }),
}))

export default useAnalysisStore
