from datetime import UTC, datetime, timedelta

import pytest
from fastapi import HTTPException

from app.models.users import User
from app.schemas.jobs import JobPriority
from app.services import quota_service


def test_enforce_quota_resets_when_due(db_session):
    user = User(
        email="reset@example.com",
        password_hash=None,
        role="admin",
        high_priority_quota_minutes=100,
        high_priority_minutes_used=50,
        quota_resets_at=datetime.now(UTC) - timedelta(minutes=1),
    )
    db_session.add(user)
    db_session.commit()

    quota_service.enforce_quota(db_session, user, JobPriority.high)

    assert user.high_priority_minutes_used == 0
    assert user.quota_resets_at is not None
    assert user.quota_resets_at > datetime.now(UTC)


def test_enforce_quota_raises_when_exceeded(db_session):
    user = User(
        email="exceeded@example.com",
        password_hash=None,
        role="admin",
        high_priority_quota_minutes=10,
        high_priority_minutes_used=10,
        quota_resets_at=datetime.now(UTC) + timedelta(days=1),
    )
    db_session.add(user)
    db_session.commit()

    with pytest.raises(HTTPException):
        quota_service.enforce_quota(db_session, user, JobPriority.high)


def test_reset_expired_updates_rows(db_session):
    now = datetime.now(UTC)
    expired = User(
        email="expired@example.com",
        password_hash=None,
        role="admin",
        high_priority_minutes_used=25,
        quota_resets_at=now - timedelta(minutes=1),
    )
    fresh = User(
        email="fresh@example.com",
        password_hash=None,
        role="admin",
        high_priority_minutes_used=10,
        quota_resets_at=now + timedelta(days=1),
    )
    db_session.add_all([expired, fresh])
    db_session.commit()

    updated = quota_service.reset_expired(db_session, now=now)

    db_session.refresh(expired)
    db_session.refresh(fresh)
    assert updated == 1
    assert expired.high_priority_minutes_used == 0
    assert expired.quota_resets_at.replace(tzinfo=UTC) > now
    assert fresh.high_priority_minutes_used == 10
