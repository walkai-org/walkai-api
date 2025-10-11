from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.database import get_db
from app.models.users import PersonalAccessToken, User
from app.schemas.tokens import (
    PersonalAccessTokenCreate,
    PersonalAccessTokenCreated,
    PersonalAccessTokenOut,
)
from app.services import pat_service

router = APIRouter(prefix="/users/me/tokens", tags=["personal-access-tokens"])


@router.get("/", response_model=list[PersonalAccessTokenOut])
def list_personal_access_tokens(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[PersonalAccessToken]:
    tokens = db.execute(
        select(PersonalAccessToken)
        .where(PersonalAccessToken.user_id == current_user.id)
        .order_by(PersonalAccessToken.created_at.desc())
    ).scalars()
    return list(tokens)


@router.post(
    "/",
    response_model=PersonalAccessTokenCreated,
    status_code=status.HTTP_201_CREATED,
)
def create_personal_access_token(
    payload: PersonalAccessTokenCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> PersonalAccessTokenCreated:
    token, raw_token = pat_service.create_personal_access_token(
        db, current_user, payload.name
    )
    return PersonalAccessTokenCreated(
        id=token.id,
        name=token.name,
        token_prefix=token.token_prefix,
        created_at=token.created_at,
        last_used_at=token.last_used_at,
        token=raw_token,
    )


@router.delete(
    "/{token_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_personal_access_token(
    token_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Response:
    token = db.execute(
        select(PersonalAccessToken).where(
            PersonalAccessToken.id == token_id,
            PersonalAccessToken.user_id == current_user.id,
        )
    ).scalar_one_or_none()
    if not token:
        raise HTTPException(status_code=404, detail="Token not found")

    db.delete(token)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
