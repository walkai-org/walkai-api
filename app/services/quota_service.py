from __future__ import annotations

import math
from datetime import datetime

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.jobs import Job, JobRun
from app.models.users import User
from app.schemas.jobs import JobPriority

_HIGH_PRIORITIES = {JobPriority.high, JobPriority.extra_high}


def should_enforce_quota(priority: JobPriority) -> bool:
    return priority in _HIGH_PRIORITIES


def compute_billable_minutes(
    started_at: datetime | None, finished_at: datetime | None
) -> int:
    if started_at is None or finished_at is None:
        return 0
    seconds = (finished_at - started_at).total_seconds()
    return max(0, math.ceil(seconds / 60))


def get_used_high_priority_minutes(db: Session, user_id: int) -> int:
    stmt = (
        select(func.coalesce(func.sum(JobRun.billable_minutes), 0))
        .join(Job, Job.id == JobRun.job_id)
        .where(
            JobRun.user_id == user_id,
            JobRun.is_scheduled.is_(False),
            Job.priority.in_(_HIGH_PRIORITIES),
        )
    )
    result = db.execute(stmt).scalar_one_or_none()
    return int(result or 0)


def enforce_quota(db: Session, user: User, priority: JobPriority) -> None:
    if not should_enforce_quota(priority):
        return

    used = get_used_high_priority_minutes(db, user.id)
    remaining = user.high_priority_quota_minutes - used
    if remaining <= 0:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "High-priority quota exceeded "
                f"(used {used} of {user.high_priority_quota_minutes} minutes)"
            ),
        )
