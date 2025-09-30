from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Generator

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from .config import get_settings
from .database import engine, get_session, ping_database
from .email_sender import send_invitation_via_acs_smtp
from .models import Base, Invitation, User
from .schemas import InviteIn, UserCreate, UserOut
from .security import generate_raw_token, hash_password, hash_token

settings = get_settings()
INVITE_TTL_HOURS = 48

Base.metadata.create_all(bind=engine)

app = FastAPI(title="walk:ai API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db() -> Generator[Session, None, None]:
    yield from get_session()


def _require_base_url() -> str:
    base_url = os.getenv("INVITE_BASE_URL")
    if not base_url:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Invitation service is not configured",
        )
    return base_url.rstrip("/")


def _get_current_admin_email() -> str:
    email = os.getenv("ADMIN_EMAIL")
    if not email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Admin authentication is not configured",
        )
    return email


@app.post("/users", response_model=UserOut, status_code=201)
def register(payload: UserCreate, db: Session = Depends(get_db)):
    existing_user = db.query(User).filter(User.email == payload.email).first()
    if existing_user:
        raise HTTPException(status_code=409, detail="Email ya registrado")

    user = User(
        email=payload.email,
        password_hash=hash_password(payload.password),
        role="admin",
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@app.post("/admin/invitations", status_code=201)
def create_invitation(
    payload: InviteIn,
    bg: BackgroundTasks,
    db: Session = Depends(get_db),
    current_admin_email: str = Depends(_get_current_admin_email),
):
    raw_token = generate_raw_token(32)
    token_hash = hash_token(raw_token)
    expires_at = datetime.utcnow() + timedelta(hours=INVITE_TTL_HOURS)

    invitation = Invitation(
        email=payload.email,
        token_hash=token_hash,
        expires_at=expires_at.replace(microsecond=0),
        invited_by=current_admin_email,
    )

    db.add(invitation)
    db.commit()

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
