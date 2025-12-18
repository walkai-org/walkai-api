import logging
from collections.abc import Callable, Sequence
from contextlib import suppress
from datetime import UTC, datetime, timedelta

from croniter import croniter
from fastapi import HTTPException
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.models.jobs import Job, JobRun, JobSchedule
from app.schemas.jobs import RunStatus
from app.schemas.schedules import ScheduleCreate, ScheduleKind
from app.services import job_service, quota_service

logger = logging.getLogger(__name__)


def _supports_skip_locked(db: Session) -> bool:
    bind = db.get_bind()
    if bind is None:
        return False
    return bind.dialect.name != "sqlite"


def _normalize_to_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise HTTPException(status_code=400, detail="Datetime must include timezone")
    if value.utcoffset() != timedelta(0):
        raise HTTPException(status_code=400, detail="Datetime must be in UTC")
    return value.astimezone(UTC)


def _coerce_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _next_from_cron(expr: str, *, base: datetime) -> datetime:
    try:
        return croniter(expr, base).get_next(datetime)
    except Exception as exc:  # pragma: no cover - validated earlier
        raise HTTPException(status_code=400, detail="Invalid cron expression") from exc


def _job_has_active_run(db: Session, job_id: int) -> bool:
    active_statuses = (RunStatus.pending, RunStatus.scheduled, RunStatus.active)
    stmt = (
        select(JobRun.id)
        .where(JobRun.job_id == job_id, JobRun.status.in_(active_statuses))
        .limit(1)
    )
    return db.execute(stmt).scalar_one_or_none() is not None


def _due_schedule_query(now: datetime) -> Select[tuple[JobSchedule]]:
    return (
        select(JobSchedule)
        .where(
            JobSchedule.next_run_at.is_not(None),
            JobSchedule.next_run_at <= now,
        )
        .order_by(JobSchedule.next_run_at)
    )


def create_schedule(db: Session, job_id: int, payload: ScheduleCreate) -> JobSchedule:
    job = db.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    if job.latest_run is None:
        raise HTTPException(
            status_code=400,
            detail="Job must have at least one run before creating a schedule",
        )

    now = datetime.now(UTC)
    next_run_at: datetime | None
    run_at = _normalize_to_utc(payload.run_at)
    cron_expr = payload.cron

    if payload.kind is ScheduleKind.once:
        if run_at is None:
            raise HTTPException(status_code=400, detail="run_at is required")
        next_run_at = run_at
    else:
        if not cron_expr:
            raise HTTPException(status_code=400, detail="cron is required")
        next_run_at = _next_from_cron(cron_expr, base=now)
        run_at = None

    schedule = JobSchedule(
        job_id=job.id,
        kind=payload.kind,
        run_at=run_at,
        cron=cron_expr,
        next_run_at=next_run_at,
    )
    db.add(schedule)
    db.commit()
    db.refresh(schedule)
    _coerce_schedule_datetimes(schedule)
    return schedule


def _coerce_schedule_datetimes(schedule: JobSchedule) -> None:
    schedule.run_at = _coerce_utc(schedule.run_at)
    schedule.next_run_at = _coerce_utc(schedule.next_run_at)
    schedule.last_run_at = _coerce_utc(schedule.last_run_at)


def list_schedules(db: Session, job_id: int) -> Sequence[JobSchedule]:
    stmt = (
        select(JobSchedule)
        .where(JobSchedule.job_id == job_id)
        .order_by(JobSchedule.id.desc())
    )
    result = db.execute(stmt)
    schedules = result.scalars().all()
    for schedule in schedules:
        _coerce_schedule_datetimes(schedule)
    return schedules


def get_schedule(db: Session, job_id: int, schedule_id: int) -> JobSchedule:
    stmt = select(JobSchedule).where(
        JobSchedule.job_id == job_id, JobSchedule.id == schedule_id
    )
    schedule = db.execute(stmt).scalar_one_or_none()
    if schedule is None:
        raise HTTPException(status_code=404, detail="Schedule not found")
    _coerce_schedule_datetimes(schedule)
    return schedule


def delete_schedule(db: Session, job_id: int, schedule_id: int) -> None:
    schedule = get_schedule(db, job_id, schedule_id)
    db.delete(schedule)
    db.commit()


def process_due_schedules(
    db: Session,
    *,
    core,
    batch,
    ecr_client,
    now: datetime,
    max_triggers_per_schedule: int = 3,
    run_session_factory: Callable[[], Session] | None = None,
) -> int:
    """
    Trigger reruns for schedules whose next_run_at is due.

    Returns the number of runs started.
    """
    quota_service.reset_expired(db, now=now)

    run_session_factory = run_session_factory or SessionLocal
    stmt = _due_schedule_query(now)
    if _supports_skip_locked(db):
        stmt = stmt.with_for_update(skip_locked=True)

    schedules = db.execute(stmt).scalars().all()
    triggered = 0

    for schedule in schedules:
        occurrences = 0
        _coerce_schedule_datetimes(schedule)
        next_run = schedule.next_run_at

        while (
            next_run is not None
            and next_run <= now
            and occurrences < max_triggers_per_schedule
        ):
            run_session: Session | None = None
            if _job_has_active_run(db, schedule.job_id):
                logger.debug(
                    "Skip schedule %s for job %s; active run in progress",
                    schedule.id,
                    schedule.job_id,
                )
                break

            try:
                run_session = run_session_factory()
                job_service.rerun_job(
                    core,
                    batch,
                    ecr_client,
                    run_session,
                    schedule.job_id,
                    run_user=None,
                    is_scheduled=True,
                )
            except HTTPException as exc:
                logger.warning(
                    "Failed to trigger scheduled run for job %s (schedule %s): %s",
                    schedule.job_id,
                    schedule.id,
                    exc.detail,
                )
                break
            except Exception:
                logger.exception(
                    "Unexpected error triggering schedule %s for job %s",
                    schedule.id,
                    schedule.job_id,
                )
                break
            finally:
                with suppress(Exception):
                    if run_session is not None and run_session is not db:
                        run_session.close()

            schedule.last_run_at = next_run
            occurrences += 1
            triggered += 1

            if schedule.kind is ScheduleKind.once:
                schedule.next_run_at = None
                next_run = None
            else:
                next_run = _next_from_cron(schedule.cron or "", base=next_run)
                schedule.next_run_at = next_run

        db.commit()

    return triggered
