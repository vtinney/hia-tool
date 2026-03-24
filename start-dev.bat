@echo off
echo === HIA Walkthrough - Development Server ===

if not exist data mkdir data

echo Starting backend on http://localhost:8000 ...
start "HIA Backend" cmd /c "cd backend && uvicorn main:app --reload --port 8000"

echo Starting frontend on http://localhost:3000 ...
start "HIA Frontend" cmd /c "cd frontend && npm run dev"

echo.
echo Backend:  http://localhost:8000
echo Frontend: http://localhost:3000
echo Health:   http://localhost:8000/health
echo.
echo Close the spawned terminal windows to stop the servers.
pause
