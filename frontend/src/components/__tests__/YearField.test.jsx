import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import YearField from '../YearField'

describe('YearField', () => {
  it('renders the current value in the select', () => {
    render(
      <YearField label="Year" value={2018} baselineYear={2018} onChange={() => {}} />,
    )
    expect(screen.getByRole('combobox')).toHaveValue('2018')
  })

  it('shows "differs from baseline" badge when year does not match baseline', () => {
    render(
      <YearField label="Year" value={2020} baselineYear={2018} onChange={() => {}} />,
    )
    expect(screen.getByText(/differs from baseline year/i)).toBeInTheDocument()
  })

  it('does not show badge when value equals baseline', () => {
    render(
      <YearField label="Year" value={2018} baselineYear={2018} onChange={() => {}} />,
    )
    expect(screen.queryByText(/differs from baseline year/i)).not.toBeInTheDocument()
  })

  it('does not show badge when baselineYear is null', () => {
    render(
      <YearField label="Year" value={2018} baselineYear={null} onChange={() => {}} />,
    )
    expect(screen.queryByText(/differs from baseline year/i)).not.toBeInTheDocument()
  })

  it('emits numeric value via onChange', () => {
    let captured = null
    render(
      <YearField
        label="Year"
        value={2018}
        baselineYear={2018}
        onChange={(v) => { captured = v }}
      />,
    )
    fireEvent.change(screen.getByRole('combobox'), { target: { value: '2019' } })
    expect(captured).toBe(2019)
  })

  it('restricts options to allowedYears when provided', () => {
    render(
      <YearField
        label="Year"
        value={null}
        baselineYear={null}
        allowedYears={[2015, 2016]}
        onChange={() => {}}
      />,
    )
    const options = screen.getAllByRole('option').map((o) => o.textContent)
    expect(options).toEqual(expect.arrayContaining(['2015', '2016']))
    expect(options).not.toContain('2017')
  })
})
