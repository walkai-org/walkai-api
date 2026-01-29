from pydantic import BaseModel, EmailStr, SecretStr


class PasswordForgotIn(BaseModel):
    email: EmailStr


class PasswordResetIn(BaseModel):
    token: SecretStr
    password: SecretStr
