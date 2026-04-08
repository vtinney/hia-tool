# HIA Walkthrough

A web-based Health Impact Assessment tool that estimates the health and economic impacts of changes in air quality. It walks users through a 7-step wizard — from defining a study area and selecting concentration-response functions to computing attributable deaths with Monte Carlo uncertainty and exporting publication-ready results. Built with React + Tailwind CSS on the frontend and FastAPI + NumPy on the backend.

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Node.js | 20+ | Frontend dev server and build |
| Python | 3.11+ | Backend API server |
| npm | 10+ | Frontend package management |

### Windows (no admin rights)

If you cannot install Node.js via a system installer, use the **nvm-noinstall** portable approach:

1. Download the Node.js 20 LTS **zip** (not the `.msi`) from https://nodejs.org/en/download
2. Extract to a folder you own, e.g. `C:\Users\<you>\nodejs`
3. Set the PATH for the current terminal session:

```cmd
set PATH=C:\Users\<you>\nodejs;%PATH%
```

4. Verify: `node --version` should print `v20.x.x`.

For Python, use **Miniconda** (user-space install):

```cmd
:: Download Miniconda from https://docs.conda.io/en/latest/miniconda.html
:: Run the installer — choose "Install for me only"
conda create -n hia python=3.11 -y
conda activate hia
```

---

## Quick Start

Get the app running in under 10 minutes.

```bash
# 1. Clone the repo
git clone <repo-url> hia-tool
cd hia-tool

# 2. Set up Python environment
conda create -n hia python=3.11 -y
conda activate hia
pip install -r requirements.txt

# 3. Set up frontend
cd frontend
npm install
cd ..

# 4. Create the data directory
mkdir data

# 5. Copy .env.example to .env and fill in your keys (see next section)
cp .env.example .env

# 6. Start both servers
#    Option A — use the start script:
./start-dev.sh          # Linux/macOS
start-dev.bat           # Windows

#    Option B — start manually in two terminals:
# Terminal 1 (backend):
python -m uvicorn backend.main:app --reload --port 8000

# Terminal 2 (frontend):
cd frontend && npm run dev
```

Open http://localhost:3000 in your browser.

---

## Environment Variables

Create a `.env` file in the project root. All variables are optional except where noted.

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `sqlite:///./data/hia.db` | SQLAlchemy database URL. SQLite for development. |
| `STORAGE_BACKEND` | `local` | `local` or `s3` (S3 not yet implemented). |
| `STORAGE_PATH` | `./data` | Root directory for uploaded files and processed data. |
| `DATA_ROOT` | `./data/processed` | Root for built-in processed datasets (ETL output). |
| `ANTHROPIC_API_KEY` | *(none)* | Anthropic API key for the HIA Wizard chat assistant. Optional — the wizard shows a setup message if missing. Get one at https://console.anthropic.com/settings/keys |
| `MAPBOX_TOKEN` | *(none)* | Mapbox GL JS access token for map rendering. Optional — maps show a placeholder without it. |
| `CENSUS_API_KEY` | *(none)* | Census Bureau API key. Required when running `backend/etl/process_acs.py`. Not read by the backend at runtime. Free at https://api.census.gov/data/key_signup.html |
| `BASE_URL` | `http://localhost:3000` | Frontend origin for CORS configuration. |

---

## Project Structure

```
hia-tool/
├── frontend/                 React + Vite + Tailwind CSS
│   ├── src/
│   │   ├── components/       Shared UI (WizardLayout, ChatWizard, ResultsTable, ErrorBoundary)
│   │   ├── content/steps/    Markdown sidebar content (help, uncertainties, best practices)
│   │   ├── data/             Static data (crf-library.json, world-bank-gni.json, templates/)
│   │   ├── lib/              Core logic (hia-engine.js, api.js)
│   │   ├── pages/            Page components (Home, Results, steps/Step1–Step7)
│   │   ├── routes/           React Router config
│   │   └── stores/           Zustand state stores
│   └── package.json
│
├── backend/                  FastAPI + NumPy
│   ├── main.py               App entry point, router registration
│   ├── models/               SQLAlchemy models (Analysis, FileUpload, Template)
│   ├── routers/              API route handlers
│   │   ├── compute.py        POST /api/compute (scalar + spatial)
│   │   ├── data.py           GET /api/data/* (built-in datasets)
│   │   ├── uploads.py        File upload management
│   │   ├── templates.py      Analysis template CRUD
│   │   └── wizard.py         POST /api/wizard/chat (AI assistant)
│   ├── services/
│   │   ├── hia_engine.py     NumPy HIA computation engine
│   │   └── geo_processor.py  Raster/vector I/O, zonal statistics
│   ├── etl/
│   │   └── process_pm25.py   NetCDF → Parquet ETL pipeline
│   ├── wizard/
│   │   └── system_prompt.txt
│   └── tests/
│
├── data/                     Runtime data (gitignored)
│   ├── uploads/              User-uploaded files
│   └── processed/            ETL output (Parquet files by pollutant/country/year)
│
├── requirements.txt          Python dependencies
├── start-dev.sh              Linux/macOS dev launcher
├── start-dev.bat             Windows dev launcher
└── .env                      Environment variables (not committed)
```

---

## Running Tests

### Frontend (Vitest)

```bash
cd frontend

# Run once
npm test

# Watch mode
npm run test:watch
```

Key test file: `src/lib/__tests__/hia-engine.test.js` — validates all four CRF functional forms, Monte Carlo uncertainty, and edge cases.

### Backend (pytest)

```bash
# From project root, with conda env active
conda activate hia
python -m pytest backend/tests/ -v
```

Key test file: `backend/tests/test_hia_engine.py` — mirrors the frontend tests to ensure cross-engine consistency.

---

## Adding Built-in Data

The ETL pipeline converts raw geospatial files into Parquet datasets that the app serves via `GET /api/data/*`.

### PM2.5 example

```bash
conda activate hia

python -m backend.etl.process_pm25 \
  --input data/raw/pm25_2020.nc \
  --boundaries data/boundaries/mexico_admin1.geojson \
  --output data/processed/pm25/mexico/2020.parquet \
  --verbose
```

The output Parquet file contains: `admin_id`, `admin_name`, `mean_pm25`, `pixel_count`, `geometry` (WKT).

### Directory convention

Place ETL output under `data/processed/` following this structure:

```
data/processed/
├── pm25/
│   └── mexico/
│       └── 2020.parquet
├── population/
│   └── mexico/
│       └── 2020.parquet
└── incidence/
    └── mexico/
        └── all-cause-mortality/
            └── 2020.parquet
```

The `GET /api/data/datasets` endpoint auto-discovers all datasets in this tree.

---

## Sharing with Pilot Testers

To make the app accessible to others on your local network:

### 1. Bind the backend to all interfaces

Edit `backend/main.py`, line 40:

```python
uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
```

Or pass it on the command line:

```bash
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000
```

### 2. Bind the frontend to all interfaces

```bash
cd frontend
npx vite --host 0.0.0.0
```

### 3. Find your IP

```bash
# Windows
ipconfig    # Look for IPv4 Address under your active adapter

# macOS/Linux
hostname -I
```

Testers can access the app at `http://<your-ip>:3000`.

### 4. Update CORS

Set `BASE_URL=http://<your-ip>:3000` in `.env` so the backend allows requests from the tester's browser.

---

## Transition to Cloud

When moving beyond local development, the main changes are:

| Component | Local | Cloud |
|-----------|-------|-------|
| Database | SQLite file | PostgreSQL (via `DATABASE_URL`) |
| File storage | Local filesystem | S3 (set `STORAGE_BACKEND=s3`, add `AWS_*` env vars) |
| Compute | Synchronous in-process | Celery workers or AWS Lambda |
| Frontend | Vite dev server | Static build (`npm run build`) behind CDN/nginx |
| Backend | `uvicorn --reload` | Gunicorn + uvicorn workers behind a reverse proxy |
| Auth | None | OAuth2 / JWT middleware |

The app is designed so these transitions require configuration changes, not architectural rewrites. The `STORAGE_BACKEND` env var already switches between local and S3 readers. The `ProcessPoolExecutor` in the spatial compute endpoint can be swapped for a Celery task queue.
