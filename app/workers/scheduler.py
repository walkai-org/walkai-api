import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime

from fastapi import FastAPI
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.database import SessionLocal
from app.services import schedule_service

logger = logging.getLogger(__name__)


def run_scheduler_tick(
    *,
    core,
    batch,
    ecr_client,
    session_factory: Callable[[], Session] = SessionLocal,
    close_session: bool = True,
    now: datetime | None = None,
) -> int:
    """
    Run a single scheduler tick synchronously.

    Returns the number of runs started.
    """
    if core is None or batch is None or ecr_client is None:
        logger.debug("Scheduler tick skipped: clients not ready")
        return 0

    session = session_factory()
    try:
        return schedule_service.process_due_schedules(
            session,
            core=core,
            batch=batch,
            ecr_client=ecr_client,
            now=now or datetime.now(UTC),
            run_session_factory=session_factory,
        )
    finally:
        if close_session:
            session.close()


async def scheduler_loop(app: FastAPI) -> None:
    settings = get_settings()
    interval = settings.schedule_interval_seconds

    while True:
        try:
            triggered = await asyncio.to_thread(
                run_scheduler_tick,
                core=getattr(app.state, "core", None),
                batch=getattr(app.state, "batch", None),
                ecr_client=getattr(app.state, "ecr_client", None),
            )
            if triggered:
                logger.info("Scheduled runs triggered: %s", triggered)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Scheduler loop tick failed")

        await asyncio.sleep(interval)
