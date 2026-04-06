import os

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.models.database import create_tables
from backend.models.analysis import Analysis  # noqa: F401 — register model
from backend.models.template import Template  # noqa: F401 — register model
from backend.models.file_upload import FileUpload  # noqa: F401 — register model
from backend.routers import health, compute, data, templates, uploads, wizard

load_dotenv()

app = FastAPI(title="HIA Walkthrough API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        os.getenv("BASE_URL", "http://localhost:3000"),
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(compute.router)
app.include_router(data.router)
app.include_router(templates.router)
app.include_router(uploads.router)
app.include_router(wizard.router)


@app.on_event("startup")
async def startup():
    await create_tables()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host="127.0.0.1", port=8000, reload=True)
