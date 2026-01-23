import json
import re
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.database import SessionLocal
from app.core.security import generate_raw_token, hash_token
from app.models.users import Invitation, User
from app.services.email_service import send_invitation_via_acs_smtp

INVITE_TTL_HOURS = 48
_SECRET_EMAIL_KEY = "email"

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _normalize_email(value: str) -> str:
    return value.strip().lower()


def _get_bootstrap_email_from_secret(sm_client) -> str | None:
    settings = get_settings()
    secret_id = settings.bootstrap_email_secret_id
    if not secret_id:
        return None

    resp = sm_client.get_secret_value(SecretId=secret_id)
    secret_str = resp.get("SecretString")
    if not secret_str:
        return None

    data = json.loads(secret_str)
    if not isinstance(data, dict):
        return None

    email = data.get(_SECRET_EMAIL_KEY)
    if not isinstance(email, str):
        return None

    email = _normalize_email(email)
    if not _EMAIL_RE.match(email):
        return None

    return email


def _has_any_users(db: Session) -> bool:
    return db.execute(select(User.id).limit(1)).first() is not None


def _has_active_invitation_for(db: Session, email: str) -> bool:
    now = datetime.utcnow()
    stmt = (
        select(Invitation.id)
        .where(Invitation.email == email)
        .where(Invitation.used_at.is_(None))
        .where(Invitation.expires_at >= now)
        .limit(1)
    )
    return db.execute(stmt).first() is not None


def run_first_user_bootstrap(sm_client) -> bool:
    settings = get_settings()

    if not settings.invite_base_url:
        return False

    try:
        email = _get_bootstrap_email_from_secret(sm_client)
    except Exception:
        return False

    if not email:
        return False

    raw_token: str
    invitation_link: str

    db = SessionLocal()
    try:
        if _has_any_users(db):
            return False

        if _has_active_invitation_for(db, email):
            return False

        raw_token = generate_raw_token(32)
        token_hash = hash_token(raw_token)
        expires_at = (datetime.utcnow() + timedelta(hours=INVITE_TTL_HOURS)).replace(
            microsecond=0
        )

        inv = Invitation(
            email=email,
            token_hash=token_hash,
            expires_at=expires_at,
            invited_by="system-bootstrap",
        )

        db.add(inv)
        db.commit()

        invitation_link = f"{settings.invite_base_url.rstrip('/')}?token={raw_token}"

    except Exception:
        db.rollback()
        return False
    finally:
        db.close()

    try:
        send_invitation_via_acs_smtp(email, invitation_link)
    except Exception:
        pass

    return True
