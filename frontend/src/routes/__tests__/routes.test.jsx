import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { RouterProvider, createMemoryRouter } from 'react-router-dom'
import routes from '../index'

describe('routes config', () => {
  it('wizard parent path exposes :step so useParams can read the current step', () => {
    const wizard = routes.find((r) => r.children?.length > 0)
    expect(wizard?.path).toContain(':step')
  })
})

describe('WizardLayout under real routes', () => {
  it('highlights the step that matches the URL when navigating to /analysis/2', () => {
    const router = createMemoryRouter(routes, { initialEntries: ['/analysis/2'] })
    render(<RouterProvider router={router} />)

    // In ProgressBar, the active step's button is disabled (isClickable = !isActive).
    // After the fix, visiting /analysis/2 should mark "Air Quality" active (disabled)
    // and "Study Area" inactive (enabled).
    const airQuality = screen.getByRole('button', { name: /air quality/i })
    const studyArea = screen.getByRole('button', { name: /study area/i })
    expect(airQuality).toBeDisabled()
    expect(studyArea).not.toBeDisabled()
  })
})
