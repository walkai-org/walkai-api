from urllib.parse import urlparse, urlunparse

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.database import get_db
from app.core.security import hash_password
from app.models.users import User
from app.schemas.password_reset import PasswordForgotIn, PasswordResetIn
from app.services import password_reset_service
from app.services.email_service import send_password_reset_via_acs_smtp

router = APIRouter(prefix="/password", tags=["auth"])
settings = get_settings()

RESET_TTL_MINUTES = 60


def _require_reset_base_url() -> str:
    invite_base_url = settings.invite_base_url
    if not invite_base_url:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Invitation service is not configured",
        )
    parsed = urlparse(invite_base_url.rstrip("/"))
    if not parsed.scheme or not parsed.netloc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Invitation service is not configured",
        )
    path = parsed.path or ""
    if "/invitations" in path:
        path = path[: path.find("/invitations")]
    path = path.rstrip("/")
    reset_path = f"{path}/reset-password" if path else "/reset-password"
    return urlunparse(
        parsed._replace(path=reset_path, params="", query="", fragment="")
    )


@router.post("/forgot")
def forgot_password(
    payload: PasswordForgotIn,
    bg: BackgroundTasks,
    db: Session = Depends(get_db),
):
    reset_base_url = _require_reset_base_url()
    email = payload.email.strip().lower()
    user = db.query(User).filter(User.email == email).first()
    if user:
        _, raw_token = password_reset_service.create_password_reset_token(
            db,
            user,
            RESET_TTL_MINUTES,
        )
        reset_link = f"{reset_base_url}?token={raw_token}"
        bg.add_task(send_password_reset_via_acs_smtp, user.email, reset_link)
    return {"message": "If the email address is valid, instructions will be sent."}


@router.post("/reset")
def reset_password(payload: PasswordResetIn, db: Session = Depends(get_db)):
    raw_token = payload.token.get_secret_value()
    token = password_reset_service.validate_password_reset_token(db, raw_token)
    user = token.user
    if not user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset token",
        )
    user.password_hash = hash_password(payload.password.get_secret_value())
    password_reset_service.consume_password_reset_token(db, token)
    db.commit()
    return {"message": "Password updated"}
