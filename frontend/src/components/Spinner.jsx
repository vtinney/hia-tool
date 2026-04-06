export function Spinner({ size = 'md', className = '' }) {
  const sizes = { sm: 'h-4 w-4', md: 'h-6 w-6', lg: 'h-8 w-8' }
  return (
    <svg className={`animate-spin text-blue-500 ${sizes[size]} ${className}`} fill="none" viewBox="0 0 24 24">
      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
    </svg>
  )
}

export function SkeletonLine({ className = '' }) {
  return <div className={`h-4 bg-gray-200 rounded animate-pulse ${className}`} />
}

export function SkeletonCard() {
  return (
    <div className="rounded-2xl border border-gray-200 p-6 space-y-3">
      <SkeletonLine className="w-1/3 h-3" />
      <SkeletonLine className="w-2/3 h-8" />
      <SkeletonLine className="w-1/2 h-3" />
    </div>
  )
}

export function LoadingOverlay({ message = 'Loading...' }) {
  return (
    <div className="flex flex-col items-center justify-center py-16">
      <Spinner size="lg" />
      <p className="mt-4 text-sm text-gray-500">{message}</p>
    </div>
  )
}
