from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ScheduleKind(StrEnum):
    once = "once"
    cron = "cron"


class ScheduleCreate(BaseModel):
    kind: ScheduleKind
    run_at: datetime | None = Field(
        default=None, description="UTC datetime for one-time schedules"
    )
    cron: str | None = Field(
        default=None,
        description="Cron expression for recurring schedules (evaluated in UTC)",
    )

    @field_validator("run_at")
    @classmethod
    def _require_timezone(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return value
        if value.tzinfo is None:
            raise ValueError("run_at must include a timezone; use UTC")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _validate_fields(self) -> "ScheduleCreate":
        if self.kind is ScheduleKind.once:
            if self.run_at is None:
                raise ValueError("run_at is required for one-time schedules")
        elif self.kind is ScheduleKind.cron and not self.cron:
            raise ValueError("cron is required for recurring schedules")
        return self


class ScheduleOut(BaseModel):
    id: int
    job_id: int
    kind: ScheduleKind
    run_at: datetime | None
    cron: str | None
    next_run_at: datetime | None
    last_run_at: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
