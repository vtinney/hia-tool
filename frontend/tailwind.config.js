import typography from '@tailwindcss/typography'

/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Geist', 'ui-sans-serif', 'system-ui', '-apple-system', 'Segoe UI', 'sans-serif'],
        mono: ['"JetBrains Mono"', 'ui-monospace', 'SFMono-Regular', 'Menlo', 'monospace'],
      },
      colors: {
        // Single accent: a desaturated, scientific teal. One color, used sparingly.
        accent: {
          50:  '#edf7f6',
          100: '#d6ece9',
          200: '#aedad4',
          300: '#7fc1b9',
          400: '#4ea49b',
          500: '#2a8880',
          600: '#1c6d66',
          700: '#155852',
          800: '#114641',
          900: '#0d3835',
          950: '#072120',
        },
        ink: {
          DEFAULT: '#0a0a0b',
          soft: '#1a1a1c',
        },
        paper: '#fafaf9',
      },
      letterSpacing: {
        tightest: '-0.035em',
      },
      transitionTimingFunction: {
        'out-strong': 'cubic-bezier(0.23, 1, 0.32, 1)',
        'in-out-strong': 'cubic-bezier(0.77, 0, 0.175, 1)',
        drawer: 'cubic-bezier(0.32, 0.72, 0, 1)',
      },
      boxShadow: {
        soft: '0 1px 2px rgba(10, 10, 11, 0.04), 0 1px 1px rgba(10, 10, 11, 0.03)',
        lift: '0 2px 4px rgba(10, 10, 11, 0.04), 0 8px 24px -8px rgba(10, 10, 11, 0.08)',
        'inset-hairline': 'inset 0 1px 0 rgba(255, 255, 255, 0.6)',
      },
      borderRadius: {
        '4xl': '2rem',
      },
      maxWidth: {
        prose: '65ch',
      },
    },
  },
  plugins: [typography],
}
