// Loading primitives — skeleton-first (Emil: avoid generic spinners
// for layout-aware loading), with a small spinner kept as fallback.

export function Spinner({ size = 'md', className = '' }) {
  const sizes = { sm: 'h-3.5 w-3.5', md: 'h-4 w-4', lg: 'h-5 w-5' }
  return (
    <svg
      className={`animate-spin text-accent-700 ${sizes[size]} ${className}`}
      viewBox="0 0 24 24"
      fill="none"
      aria-hidden="true"
    >
      <circle cx="12" cy="12" r="9" stroke="currentColor" strokeOpacity="0.18" strokeWidth="2" />
      <path
        d="M21 12a9 9 0 0 0-9-9"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
      />
    </svg>
  )
}

// Shimmer base — used by SkeletonLine / SkeletonCard.
// Animates background-position (off main thread, GPU-friendly).
const SHIMMER =
  'relative overflow-hidden bg-zinc-100 ' +
  "after:content-[''] after:absolute after:inset-0 " +
  'after:-translate-x-full after:animate-[shimmer_1.6s_infinite] ' +
  'after:bg-gradient-to-r after:from-transparent after:via-white/60 after:to-transparent'

export function SkeletonLine({ className = '' }) {
  return <div className={`h-3 rounded ${SHIMMER} ${className}`} />
}

export function SkeletonCard() {
  return (
    <div className="surface p-6 space-y-3">
      <SkeletonLine className="w-1/3 h-2.5" />
      <SkeletonLine className="w-2/3 h-6" />
      <SkeletonLine className="w-1/2 h-2.5" />
    </div>
  )
}

export function LoadingOverlay({ message = 'Loading…' }) {
  return (
    <div className="flex flex-col items-center justify-center py-20">
      <Spinner size="lg" />
      <p className="mt-4 font-mono text-[11px] uppercase tracking-[0.14em] text-zinc-500">
        {message}
      </p>
    </div>
  )
}
