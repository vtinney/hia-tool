import { useMemo } from 'react'

const DEFAULT_MIN = 1990
const DEFAULT_MAX = new Date().getFullYear()

export default function YearField({
  label,
  value,
  baselineYear,
  allowedYears,
  onChange,
  id,
  required = false,
}) {
  const options = useMemo(() => {
    if (allowedYears && allowedYears.length > 0) return [...allowedYears].sort((a, b) => b - a)
    const years = []
    for (let y = DEFAULT_MAX; y >= DEFAULT_MIN; y--) years.push(y)
    return years
  }, [allowedYears])

  const showDiffers = baselineYear != null && value != null && value !== baselineYear

  return (
    <div>
      {label && (
        <label htmlFor={id} className="block text-xs text-gray-500 mb-1">
          {label}{required && <span className="text-red-500 ml-0.5">*</span>}
        </label>
      )}
      <select
        id={id}
        value={value ?? ''}
        onChange={(e) => onChange(e.target.value ? Number(e.target.value) : null)}
        className="w-full rounded-lg border border-gray-300 px-3 py-2.5 text-sm
                   focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
      >
        <option value="">Select a year…</option>
        {options.map((y) => (
          <option key={y} value={y}>{y}</option>
        ))}
      </select>
      {showDiffers && (
        <p className="mt-1 text-xs text-amber-600">
          Differs from baseline year ({baselineYear}).
        </p>
      )}
    </div>
  )
}
