from pydantic import BaseModel, EmailStr


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
