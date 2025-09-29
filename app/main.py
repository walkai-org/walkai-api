from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Generator

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr

from .config import get_settings
from .database import get_connection, ping_database
from .email_sender import send_invitation_via_acs_smtp
from .security import generate_raw_token, hash_token

settings = get_settings()
INVITE_TTL_HOURS = 48


def _init_db() -> None:
    """Ensure the invitation table and indexes exist."""
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS user_invitations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                expires_at TEXT NOT NULL,
                used_at TEXT,
                invited_by TEXT
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_invitations_email ON user_invitations (email)"
        )
        connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_invitations_token_hash ON user_invitations (token_hash)"
        )


_init_db()

app = FastAPI(title="walk:ai API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class InviteIn(BaseModel):
    email: EmailStr


def get_db() -> Generator[sqlite3.Connection, None, None]:
    with get_connection() as connection:
        yield connection


def _require_base_url() -> str:
    base_url = os.getenv("INVITE_BASE_URL")
    if not base_url:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Invitation service is not configured",
        )
    return base_url.rstrip("/")


def _get_current_admin_email() -> str:
    email = True  # os.getenv("ADMIN_EMAIL_ENV")
    if not email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Admin authentication is not configured",
        )
    return email


@app.post("/admin/invitations", status_code=201)
def create_invitation(
    payload: InviteIn,
    bg: BackgroundTasks,
    db: sqlite3.Connection = Depends(get_db),
    current_admin_email: str = Depends(_get_current_admin_email),
):
    raw_token = generate_raw_token(32)
    token_hash = hash_token(raw_token)
    expires_at = datetime.utcnow() + timedelta(hours=INVITE_TTL_HOURS)

    db.execute(
        """
        INSERT INTO user_invitations (email, token_hash, expires_at, invited_by)
        VALUES (?, ?, ?, ?)
        """,
        (
            payload.email,
            token_hash,
            expires_at.replace(microsecond=0).isoformat(timespec="seconds") + "Z",
            current_admin_email,
        ),
    )

    invitation_link = f"{_require_base_url()}?token={raw_token}"
    bg.add_task(send_invitation_via_acs_smtp, payload.email, invitation_link)

    return {"message": "If the email address is valid, instructions will be sent."}


@app.get("/health", tags=["health"])
def health_check():
    """Report service status and confirm database connectivity."""
    database_status = "ok" if ping_database() else "error"
    return {
        "status": "ok",
        "environment": settings.environment,
        "database": database_status,
    }
