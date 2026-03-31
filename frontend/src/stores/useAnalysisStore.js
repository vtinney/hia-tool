import { create } from 'zustand'
import { persist } from 'zustand/middleware'

// ── Default state for each step ──────────────────────────────────

const DEFAULT_STEP1 = {
  studyArea: { type: 'country', id: '', name: '', geometry: null },
  pollutant: null,
  years: null,
  analysisName: '',
  analysisDescription: '',
}

const DEFAULT_STEP2 = {
  baseline: { type: 'manual', value: null, datasetId: null, fileData: null },
  control: {
    type: 'none',
    value: null,
    benchmarkId: null,
    rollbackPercent: null,
  },
}

const DEFAULT_STEP3 = {
  populationType: 'manual',
  totalPopulation: null,
  ageGroups: null,
}

const DEFAULT_STEP4 = {
  incidenceType: 'manual',
  rates: null,
}

const DEFAULT_STEP5 = {
  selectedCRFs: [],
}

const DEFAULT_STEP6 = {
  poolingMethod: 'fixed',
  monteCarloIterations: 1000,
  spatialAggregation: null,
}

const DEFAULT_STEP7 = {
  runValuation: true,
  vsl: 7_400_000,
  currency: 'USD',
  dollarYear: 2024,
}

const STEP_DEFAULTS = {
  step1: DEFAULT_STEP1,
  step2: DEFAULT_STEP2,
  step3: DEFAULT_STEP3,
  step4: DEFAULT_STEP4,
  step5: DEFAULT_STEP5,
  step6: DEFAULT_STEP6,
  step7: DEFAULT_STEP7,
}

function defaultStepValidity() {
  return { 1: false, 2: false, 3: false, 4: false, 5: false, 6: false, 7: false }
}

function initialState() {
  return {
    // Wizard navigation
    currentStep: 1,
    totalSteps: 7,
    completedSteps: [],
    stepValidity: defaultStepValidity(),

    // Analysis configuration – one key per step
    step1: { ...DEFAULT_STEP1 },
    step2: { ...DEFAULT_STEP2, baseline: { ...DEFAULT_STEP2.baseline }, control: { ...DEFAULT_STEP2.control } },
    step3: { ...DEFAULT_STEP3 },
    step4: { ...DEFAULT_STEP4 },
    step5: { ...DEFAULT_STEP5, selectedCRFs: [] },
    step6: { ...DEFAULT_STEP6 },
    step7: { ...DEFAULT_STEP7 },

    // Results from the backend
    results: null,
  }
}

// ── Store ────────────────────────────────────────────────────────

const useAnalysisStore = create(
  persist(
    (set, get) => ({
      ...initialState(),

      // ── Navigation actions ───────────────────────────────────

      setCurrentStep: (step) => set({ currentStep: step }),

      markStepCompleted: (step) =>
        set((state) => ({
          completedSteps: state.completedSteps.includes(step)
            ? state.completedSteps
            : [...state.completedSteps, step].sort((a, b) => a - b),
        })),

      setStepValidity: (step, valid) =>
        set((state) => ({
          stepValidity: { ...state.stepValidity, [step]: valid },
        })),

      // ── Per-step setters (shallow-merge into the step key) ──

      setStep1: (patch) =>
        set((state) => ({ step1: { ...state.step1, ...patch } })),

      setStep2: (patch) =>
        set((state) => ({ step2: { ...state.step2, ...patch } })),

      setStep3: (patch) =>
        set((state) => ({ step3: { ...state.step3, ...patch } })),

      setStep4: (patch) =>
        set((state) => ({ step4: { ...state.step4, ...patch } })),

      setStep5: (patch) =>
        set((state) => ({ step5: { ...state.step5, ...patch } })),

      setStep6: (patch) =>
        set((state) => ({ step6: { ...state.step6, ...patch } })),

      setStep7: (patch) =>
        set((state) => ({ step7: { ...state.step7, ...patch } })),

      // ── Results ──────────────────────────────────────────────

      setResults: (results) => set({ results }),

      // ── Reset to defaults ────────────────────────────────────

      reset: () => set(initialState()),

      // ── Template support ─────────────────────────────────────

      loadFromTemplate: (config) => {
        const next = initialState()

        for (let i = 1; i <= 7; i++) {
          const key = `step${i}`
          if (config[key]) {
            next[key] = { ...STEP_DEFAULTS[key], ...config[key] }
          }
        }

        if (config.completedSteps) next.completedSteps = config.completedSteps
        if (config.stepValidity) next.stepValidity = { ...defaultStepValidity(), ...config.stepValidity }

        set(next)
      },

      // ── Export (pure read — no state mutation) ───────────────

      exportConfig: () => {
        const { step1, step2, step3, step4, step5, step6, step7 } = get()
        return JSON.parse(
          JSON.stringify({ step1, step2, step3, step4, step5, step6, step7 }),
        )
      },
    }),
    {
      name: 'hia-analysis',
      version: 1,
      partialize: (state) => ({
        // Persist only the data that matters for resume — skip transient UI state
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
      migrate: (persisted, version) => {
        // Future-proof: if the stored version is older, start fresh
        if (version === 0) return initialState()
        return persisted
      },
    },
  ),
)

export default useAnalysisStore
