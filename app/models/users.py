import datetime

from sqlalchemy import (
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    email: Mapped[str] = mapped_column(unique=True)
    password_hash: Mapped[str | None] = mapped_column(default=None, repr=False)
    role: Mapped[str] = mapped_column(default="admin")
    created_at: Mapped[datetime.datetime] = mapped_column(
        default=lambda: datetime.datetime.now(datetime.UTC), init=False
    )
    jobs: Mapped[list["Job"]] = relationship(back_populates="created_by", init=False)  # type: ignore  # noqa: F821
    social_identities: Mapped[list["SocialIdentity"]] = relationship(
        back_populates="user",
        init=False,
    )


class Invitation(Base):
    __tablename__ = "user_invitations"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    email: Mapped[str] = mapped_column(index=True)
    token_hash: Mapped[str] = mapped_column(unique=True, repr=False)
    expires_at: Mapped[datetime.datetime]
    invited_by: Mapped[str | None] = mapped_column(default=None)
    used_at: Mapped[datetime.datetime | None] = mapped_column(default=None)


class SocialIdentity(Base):
    __tablename__ = "social_identities"
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    user: Mapped[User] = relationship(back_populates="social_identities", init=False)
    provider: Mapped[str] = mapped_column(index=True)
    provider_user_id: Mapped[str] = mapped_column(index=True)
    email_verified: Mapped[bool] = mapped_column(default=False)

    __table_args__ = (
        UniqueConstraint("provider", "provider_user_id", name="uq_provider_sub"),
    )
