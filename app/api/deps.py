import jwt
from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.core.config import Settings, get_settings
from app.core.database import get_db
from app.core.security import hash_token
from app.models.users import PersonalAccessToken, User


def _extract_bearer_token(request: Request) -> str | None:
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return None

    scheme, _, param = auth_header.strip().partition(" ")
    if scheme.lower() != "bearer" or not param:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization header",
        )
    return param.strip()


def _get_user_from_pat(db: Session, raw_token: str) -> User | None:
    token_hash = hash_token(raw_token)
    pat = (
        db.query(PersonalAccessToken)
        .filter(PersonalAccessToken.token_hash == token_hash)
        .first()
    )
    if not pat:
        return None
    return pat.user


def _authenticate_token(
    raw_token: str,
    db: Session,
    settings: Settings,
) -> User | None:
    try:
        payload = jwt.decode(
            raw_token,
            settings.jwt_secret,
            algorithms=[settings.jwt_algo],
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session expired",
        )
    except jwt.InvalidTokenError:
        return _get_user_from_pat(db, raw_token)

    sub = payload.get("sub")
    try:
        user_id = int(sub)
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload",
        )

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
        )
    return user


def get_current_user(
    request: Request,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> User:
    last_error: HTTPException | None = None

    header_token = _extract_bearer_token(request)
    if header_token:
        try:
            user = _authenticate_token(header_token, db, settings)
            if user:
                return user
        except HTTPException as exc:
            last_error = exc

    cookie_token = request.cookies.get("access_token")
    if cookie_token:
        try:
            user = _authenticate_token(cookie_token, db, settings)
            if user:
                return user
        except HTTPException as exc:
            last_error = exc

    if last_error:
        raise last_error

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
    )


def require_admin(user: User = Depends(get_current_user)) -> str:
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="The user is not an admin")
    return user.email
