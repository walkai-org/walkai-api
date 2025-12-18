from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

from fastapi import HTTPException, status
from sqlalchemy import update
from sqlalchemy.orm import Session

from app.models.users import User
from app.schemas.jobs import JobPriority

_HIGH_PRIORITIES = {JobPriority.high, JobPriority.extra_high}
_RESET_PERIOD = timedelta(days=7)


def should_enforce_quota(priority: JobPriority) -> bool:
    return priority in _HIGH_PRIORITIES


def compute_billable_minutes(
    started_at: datetime | None, finished_at: datetime | None
) -> int:
    if started_at is None or finished_at is None:
        return 0
    seconds = (finished_at - started_at).total_seconds()
    return max(0, math.ceil(seconds / 60))


def ensure_reset(user: User, *, now: datetime | None = None) -> None:
    now = now or datetime.now(UTC)
    reset_at = user.quota_resets_at
    if reset_at is not None:
        if reset_at.tzinfo is None:
            reset_at = reset_at.replace(tzinfo=UTC)
        else:
            reset_at = reset_at.astimezone(UTC)
    if reset_at is None or reset_at <= now:
        user.high_priority_minutes_used = 0
        user.quota_resets_at = now + _RESET_PERIOD


def reset_expired(db: Session, *, now: datetime | None = None) -> int:
    """
    Reset usage for users whose quota window has expired.

    Returns the number of users updated.
    """
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    stmt = (
        update(User)
        .where(User.quota_resets_at.is_not(None), User.quota_resets_at <= now)
        .values(
            high_priority_minutes_used=0,
            quota_resets_at=now + _RESET_PERIOD,
        )
        .execution_options(synchronize_session=False)
    )
    result = db.execute(stmt)
    db.commit()
    return result.rowcount or 0


def enforce_quota(db: Session, user: User, priority: JobPriority) -> None:
    if not should_enforce_quota(priority):
        return

    ensure_reset(user)
    used = user.high_priority_minutes_used or 0
    remaining = user.high_priority_quota_minutes - used
    if remaining <= 0:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "High-priority quota exceeded "
                f"(used {used} of {user.high_priority_quota_minutes} minutes)"
            ),
        )
