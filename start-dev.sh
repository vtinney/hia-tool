#!/usr/bin/env bash
set -e

echo "=== HIA Walkthrough - Development Server ==="

cd "$(dirname "$0")"
mkdir -p data

# Backend must run from the repo root (the app imports as backend.main),
# using the project venv's Python — uvicorn is not on the system PATH.
echo "Starting backend on http://localhost:8000 ..."
./venv/Scripts/python.exe -m uvicorn backend.main:app --reload --port 8000 &
BACKEND_PID=$!

# npm lives in the user-local Node install (not on PATH on this machine).
echo "Starting frontend on http://localhost:3000 ..."
(cd frontend && PATH="/c/Users/vsoutherland/nodejs:$PATH" npm run dev) &
FRONTEND_PID=$!

echo ""
echo "Backend:  http://localhost:8000"
echo "Frontend: http://localhost:3000"
echo "Health:   http://localhost:8000/health"
echo ""
echo "Press Ctrl+C to stop both servers."

trap "kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit" INT TERM
wait
