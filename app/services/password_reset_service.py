from datetime import UTC, datetime, timedelta

from fastapi import HTTPException, status
from sqlalchemy import update
from sqlalchemy.orm import Session

from app.core.security import generate_raw_token, hash_token
from app.models.users import PasswordResetToken, User


def _ensure_unique_token_hash(db: Session, raw_token: str) -> tuple[str, str]:
    token_hash = hash_token(raw_token)
    existing = (
        db.query(PasswordResetToken)
        .filter(PasswordResetToken.token_hash == token_hash)
        .first()
    )
    if existing:
        return _ensure_unique_token_hash(db, generate_raw_token(32))
    return raw_token, token_hash


def create_password_reset_token(
    db: Session,
    user: User,
    ttl_minutes: int,
) -> tuple[PasswordResetToken, str]:
    now = datetime.now(UTC)
    db.execute(
        update(PasswordResetToken)
        .where(
            PasswordResetToken.user_id == user.id,
            PasswordResetToken.used_at.is_(None),
        )
        .values(used_at=now)
    )

    raw_token = generate_raw_token(32)
    raw_token, token_hash = _ensure_unique_token_hash(db, raw_token)
    expires_at = (now + timedelta(minutes=ttl_minutes)).replace(microsecond=0)

    token = PasswordResetToken(
        user_id=user.id,
        token_hash=token_hash,
        expires_at=expires_at,
    )
    db.add(token)
    db.commit()
    db.refresh(token)
    return token, raw_token


def validate_password_reset_token(db: Session, raw_token: str) -> PasswordResetToken:
    token_hash = hash_token(raw_token)
    token = (
        db.query(PasswordResetToken)
        .filter(PasswordResetToken.token_hash == token_hash)
        .first()
    )
    if token and (
        token.expires_at.tzinfo is None
        or token.expires_at.tzinfo.utcoffset(token.expires_at) is None
    ):
        now = datetime.utcnow()
    else:
        now = datetime.now(UTC)
    if not token or token.used_at is not None or token.expires_at < now:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset token",
        )
    return token


def consume_password_reset_token(db: Session, token: PasswordResetToken) -> None:
    token.used_at = datetime.now(UTC)
    db.add(token)
