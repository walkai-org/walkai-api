from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.security import generate_raw_token, hash_token
from app.models.users import PersonalAccessToken, User


def _ensure_unique_token_hash(db: Session, raw_token: str) -> tuple[str, str]:
    """
    Generate a token and ensure the hash does not collide with existing records.
    Collisions are extremely unlikely, but this keeps things deterministic.
    """
    token_hash = hash_token(raw_token)
    exists = db.execute(
        select(PersonalAccessToken.id).where(
            PersonalAccessToken.token_hash == token_hash
        )
    ).first()
    if exists:
        # regenerate and recurse until unique
        return _ensure_unique_token_hash(db, generate_raw_token(32))
    return raw_token, token_hash


def create_personal_access_token(
    db: Session,
    user: User,
    name: str | None,
) -> tuple[PersonalAccessToken, str]:
    raw_token = generate_raw_token(32)
    raw_token, token_hash = _ensure_unique_token_hash(db, raw_token)

    cleaned_name = name.strip() if name else None
    token = PersonalAccessToken(
        user_id=user.id,
        name=cleaned_name,
        token_hash=token_hash,
        token_prefix=raw_token[:8],
    )
    db.add(token)
    db.commit()
    db.refresh(token)
    return token, raw_token
