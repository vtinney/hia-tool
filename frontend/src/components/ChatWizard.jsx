import { useState, useRef, useEffect, useCallback } from 'react'
import useAnalysisStore from '../stores/useAnalysisStore'
import suggestions from '../data/wizard-suggestions.json'

const API_BASE = '/api'

// ── Minimal markdown renderer ─────────────────────────────────────

function renderMarkdown(text) {
  if (!text) return ''
  return text
    // Bold
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    // Italic
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    // Inline code
    .replace(/`(.+?)`/g, '<code class="px-1 py-0.5 bg-gray-100 rounded text-xs font-mono">$1</code>')
    // Line breaks
    .replace(/\n/g, '<br />')
}

// ── Chat bubble ───────────────────────────────────────────────────

function ChatBubble({ role, content }) {
  const isUser = role === 'user'
  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}>
      <div
        className={`
          max-w-[85%] rounded-2xl px-4 py-2.5 text-sm leading-relaxed
          ${isUser
            ? 'bg-blue-600 text-white rounded-br-md'
            : 'bg-gray-100 text-gray-800 rounded-bl-md'
          }
        `}
        dangerouslySetInnerHTML={{ __html: renderMarkdown(content) }}
      />
    </div>
  )
}

// ── Typing indicator ──────────────────────────────────────────────

function TypingIndicator() {
  return (
    <div className="flex justify-start">
      <div className="bg-gray-100 rounded-2xl rounded-bl-md px-4 py-3 flex gap-1.5">
        <span className="w-2 h-2 rounded-full bg-gray-400 animate-bounce" style={{ animationDelay: '0ms' }} />
        <span className="w-2 h-2 rounded-full bg-gray-400 animate-bounce" style={{ animationDelay: '150ms' }} />
        <span className="w-2 h-2 rounded-full bg-gray-400 animate-bounce" style={{ animationDelay: '300ms' }} />
      </div>
    </div>
  )
}

// ── Main component ────────────────────────────────────────────────

export function ChatWizardToggle({ open, onToggle }) {
  return (
    <button
      onClick={onToggle}
      className={`
        flex items-center gap-2 px-5 py-2.5 rounded-full
        text-sm font-semibold transition-all duration-200
        ${open
          ? 'bg-gray-700 text-white hover:bg-gray-800 shadow-md'
          : 'bg-gradient-to-r from-blue-600 to-teal-500 text-white hover:from-blue-700 hover:to-teal-600 shadow-lg shadow-blue-200'
        }
      `}
    >
      {open ? (
        <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
        </svg>
      ) : (
        <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
            d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09zM18.259 8.715L18 9.75l-.259-1.035a3.375 3.375 0 00-2.455-2.456L14.25 6l1.036-.259a3.375 3.375 0 002.455-2.456L18 2.25l.259 1.035a3.375 3.375 0 002.455 2.456L21.75 6l-1.036.259a3.375 3.375 0 00-2.455 2.456z" />
        </svg>
      )}
      Ask HIA Wizard
    </button>
  )
}

export default function ChatWizard({ open }) {
  const [messages, setMessages] = useState([
    { role: 'assistant', content: 'Hi! I\'m the HIA Wizard. Ask me anything about health impact assessment methodology, data choices, or how to use this tool.' },
  ])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)

  const scrollRef = useRef(null)
  const inputRef = useRef(null)

  const { currentStep, exportConfig } = useAnalysisStore()
  const stepSuggestions = suggestions[String(currentStep)] || suggestions['1']

  // Auto-scroll to bottom when messages change
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [messages, loading])

  // Focus input when panel opens
  useEffect(() => {
    if (open && inputRef.current) {
      inputRef.current.focus()
    }
  }, [open])

  const sendMessage = useCallback(async (text) => {
    const trimmed = text.trim()
    if (!trimmed || loading) return

    const userMessage = { role: 'user', content: trimmed }
    const newHistory = [...messages, userMessage]
    setMessages(newHistory)
    setInput('')
    setLoading(true)

    try {
      const res = await fetch(`${API_BASE}/wizard/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: trimmed,
          conversationHistory: newHistory.map(({ role, content }) => ({ role, content })),
          context: {
            currentStep,
            analysisConfig: exportConfig(),
          },
        }),
      })

      if (!res.ok) {
        throw new Error(`Server error: ${res.status}`)
      }

      // Check if response is SSE stream
      const contentType = res.headers.get('content-type') || ''
      if (contentType.includes('text/event-stream')) {
        // Stream SSE response
        const reader = res.body.getReader()
        const decoder = new TextDecoder()
        let assistantContent = ''

        setMessages((prev) => [...prev, { role: 'assistant', content: '' }])

        while (true) {
          const { done, value } = await reader.read()
          if (done) break

          const chunk = decoder.decode(value, { stream: true })
          const lines = chunk.split('\n')
          for (const line of lines) {
            if (line.startsWith('data: ')) {
              const data = line.slice(6)
              if (data === '[DONE]') break
              try {
                const parsed = JSON.parse(data)
                assistantContent += parsed.content || parsed.text || ''
                setMessages((prev) => {
                  const updated = [...prev]
                  updated[updated.length - 1] = { role: 'assistant', content: assistantContent }
                  return updated
                })
              } catch {
                // Plain text chunk
                assistantContent += data
                setMessages((prev) => {
                  const updated = [...prev]
                  updated[updated.length - 1] = { role: 'assistant', content: assistantContent }
                  return updated
                })
              }
            }
          }
        }
      } else {
        // JSON response
        const data = await res.json()
        const reply = data.response || data.message || data.content || 'Sorry, I could not generate a response.'
        setMessages((prev) => [...prev, { role: 'assistant', content: reply }])
      }
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: `I wasn't able to reach the server. Please check your connection and try again.\n\n*Error: ${err.message}*` },
      ])
    } finally {
      setLoading(false)
    }
  }, [messages, loading, currentStep, exportConfig])

  const handleSubmit = (e) => {
    e.preventDefault()
    sendMessage(input)
  }

  const handleSuggestion = (text) => {
    sendMessage(text)
  }

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage(input)
    }
  }

  return (
    <>
      {/* Chat panel */}
      <div
        className={`
          fixed bottom-16 right-6 z-50
          w-96 max-h-[32rem] bg-white rounded-2xl shadow-2xl border border-gray-200
          flex flex-col overflow-hidden
          transition-all duration-300 origin-bottom-right
          ${open
            ? 'scale-100 opacity-100 pointer-events-auto'
            : 'scale-90 opacity-0 pointer-events-none'
          }
        `}
      >
        {/* Header */}
        <div className="bg-gradient-to-r from-blue-600 to-teal-500 px-4 py-3 flex items-center gap-3 shrink-0">
          <div className="w-8 h-8 rounded-full bg-white/20 flex items-center justify-center">
            <svg className="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
                d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09z" />
            </svg>
          </div>
          <div>
            <p className="text-white font-semibold text-sm">HIA Wizard</p>
            <p className="text-white/70 text-xs">Ask about methodology, data, or this tool</p>
          </div>
        </div>

        {/* Messages */}
        <div ref={scrollRef} className="flex-1 overflow-y-auto p-4 space-y-3 min-h-0">
          {messages.map((msg, i) => (
            <ChatBubble key={i} role={msg.role} content={msg.content} />
          ))}
          {loading && <TypingIndicator />}
        </div>

        {/* Suggestions */}
        <div className="px-4 pb-2 flex flex-wrap gap-1.5 shrink-0">
          {stepSuggestions.map((text, i) => (
            <button
              key={i}
              onClick={() => handleSuggestion(text)}
              disabled={loading}
              className="px-2.5 py-1 text-xs rounded-full border border-blue-200 text-blue-600
                         hover:bg-blue-50 transition-colors disabled:opacity-40 disabled:cursor-not-allowed
                         truncate max-w-[180px]"
              title={text}
            >
              {text}
            </button>
          ))}
        </div>

        {/* Input */}
        <form onSubmit={handleSubmit} className="border-t border-gray-200 p-3 flex gap-2 shrink-0">
          <input
            ref={inputRef}
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask the HIA Wizard…"
            disabled={loading}
            className="flex-1 rounded-lg border border-gray-300 px-3 py-2 text-sm
                       focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none
                       disabled:bg-gray-50 disabled:text-gray-400"
          />
          <button
            type="submit"
            disabled={!input.trim() || loading}
            className="px-3 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium
                       hover:bg-blue-700 transition-colors
                       disabled:bg-gray-300 disabled:cursor-not-allowed"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8" />
            </svg>
          </button>
        </form>
      </div>
    </>
  )
}
