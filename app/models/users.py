import datetime

from sqlalchemy import (
    DateTime,
    ForeignKey,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    email: Mapped[str] = mapped_column(unique=True)
    password_hash: Mapped[str | None] = mapped_column(default=None, repr=False)
    role: Mapped[str] = mapped_column(default="admin")
    high_priority_quota_minutes: Mapped[int] = mapped_column(
        default=180, server_default="180"
    )
    high_priority_minutes_used: Mapped[int] = mapped_column(
        default=0, server_default="0"
    )
    quota_resets_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True),
        default_factory=lambda: datetime.datetime.now(datetime.UTC)
        + datetime.timedelta(days=7),
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=func.now(),
        server_default=func.now(),
        init=False,
    )
    jobs: Mapped[list["Job"]] = relationship(back_populates="created_by", init=False)  # type: ignore  # noqa: F821
    social_identities: Mapped[list["SocialIdentity"]] = relationship(
        back_populates="user",
        init=False,
    )
    personal_access_tokens: Mapped[list["PersonalAccessToken"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
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


class PersonalAccessToken(Base):
    __tablename__ = "personal_access_tokens"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    user: Mapped[User] = relationship(
        back_populates="personal_access_tokens", init=False
    )
    token_hash: Mapped[str] = mapped_column(unique=True, index=True, repr=False)
    token_prefix: Mapped[str] = mapped_column(index=True)
    name: Mapped[str | None] = mapped_column(default=None)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=func.now(),
        server_default=func.now(),
        init=False,
    )
    last_used_at: Mapped[datetime.datetime | None] = mapped_column(default=None)
