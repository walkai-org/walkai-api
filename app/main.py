from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import get_settings
from .database import ping_database

settings = get_settings()

app = FastAPI(title="walk:ai API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", tags=["health"])
def health_check():
    """Report service status and confirm database connectivity."""
    database_status = "ok" if ping_database() else "error"
    return {
        "status": "ok",
        "environment": settings.environment,
        "database": database_status,
    }
