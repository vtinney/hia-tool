#!/usr/bin/env bash
set -e

echo "=== HIA Walkthrough - Development Server ==="

# Create data directory if needed
mkdir -p data

# Start backend
echo "Starting backend on http://localhost:8000 ..."
(cd backend && uvicorn main:app --reload --port 8000) &
BACKEND_PID=$!

# Start frontend
echo "Starting frontend on http://localhost:3000 ..."
(cd frontend && npm run dev) &
FRONTEND_PID=$!

echo ""
echo "Backend:  http://localhost:8000"
echo "Frontend: http://localhost:3000"
echo "Health:   http://localhost:8000/health"
echo ""
echo "Press Ctrl+C to stop both servers."

trap "kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit" INT TERM
wait
