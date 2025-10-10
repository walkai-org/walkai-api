from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class PersonalAccessTokenCreate(BaseModel):
    name: str | None = Field(
        default=None,
        max_length=100,
        description="Display name to help identify the token",
    )


class PersonalAccessTokenOut(BaseModel):
    id: int
    name: str | None
    token_prefix: str
    created_at: datetime
    last_used_at: datetime | None

    model_config = ConfigDict(
        from_attributes=True,
    )


class PersonalAccessTokenCreated(PersonalAccessTokenOut):
    token: str
