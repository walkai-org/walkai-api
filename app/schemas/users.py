from pydantic import BaseModel, ConfigDict, EmailStr, Field, SecretStr


class InviteIn(BaseModel):
    email: EmailStr


class UserCreate(BaseModel):
    email: EmailStr
    password: str


class UserOut(BaseModel):
    id: int
    email: EmailStr
    role: str
    high_priority_quota_minutes: int

    model_config = ConfigDict(
        from_attributes=True,
    )


class UserQuotaUpdate(BaseModel):
    high_priority_quota_minutes: int = Field(ge=0)


class LoginIn(BaseModel):
    email: EmailStr
    password: str


class InvitationVerifyIn(BaseModel):
    token: SecretStr


class InvitationVerifyOut(BaseModel):
    email: EmailStr


class InvitationAcceptIn(BaseModel):
    token: SecretStr
    password: SecretStr
