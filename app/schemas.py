from pydantic import BaseModel, EmailStr, SecretStr


class InviteIn(BaseModel):
    email: EmailStr


class UserCreate(BaseModel):
    email: EmailStr
    password: str


class UserOut(BaseModel):
    id: int
    email: EmailStr
    role: str

    class Config:
        from_attributes = True


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"


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
