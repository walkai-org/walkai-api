from pydantic import BaseModel, ConfigDict, EmailStr, SecretStr


class InviteIn(BaseModel):
    email: EmailStr


class UserCreate(BaseModel):
    email: EmailStr
    password: str


class UserOut(BaseModel):
    id: int
    email: EmailStr
    role: str

    model_config = ConfigDict(
        from_attributes=True,
    )


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
