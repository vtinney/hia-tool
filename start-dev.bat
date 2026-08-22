@echo off
echo === HIA Walkthrough - Development Server ===

cd /d "%~dp0"
if not exist data mkdir data

rem Backend must run from the repo root (the app imports as backend.main),
rem using the project venv's Python — uvicorn is not on the system PATH.
echo Starting backend on http://localhost:8000 ...
start "HIA Backend" cmd /c "cd /d %~dp0 && venv\Scripts\python.exe -m uvicorn backend.main:app --reload --port 8000"

rem Node/npm live in the user-local install (not on PATH on this machine);
rem vite re-invokes `node`, so the directory must be on PATH, not just npm.
echo Starting frontend on http://localhost:3000 ...
start "HIA Frontend" cmd /c "set PATH=C:\Users\vsoutherland\nodejs;%%PATH%% && cd /d %~dp0frontend && npm run dev"

echo.
echo Backend:  http://localhost:8000
echo Frontend: http://localhost:3000
echo Health:   http://localhost:8000/health
echo.
echo Close the spawned terminal windows to stop the servers.
pause
