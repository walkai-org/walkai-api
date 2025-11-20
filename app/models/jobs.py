from __future__ import annotations

import datetime
from enum import StrEnum

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.config import get_settings
from app.core.database import Base
from app.models.users import User
from app.schemas.jobs import GPUProfile, RunStatus


def _normalize_started_at(
    value: datetime.datetime | None,
) -> datetime.datetime:
    if value is None:
        return datetime.datetime.min.replace(tzinfo=datetime.UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=datetime.UTC)
    return value.astimezone(datetime.UTC)


class VolumeState(StrEnum):
    pvc = "pvc"
    stored = "stored"
    deleted = "deleted"


class Volume(Base):
    __tablename__ = "volumes"
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    pvc_name: Mapped[str]
    size: Mapped[int]
    key_prefix: Mapped[str | None] = mapped_column(default=None)
    is_input: Mapped[bool] = mapped_column(default=False)

    @property
    def s3_uri(self) -> str | None:
        settings = get_settings()
        return f"s3://{settings.aws_s3_bucket}/{self.key_prefix}"


class Job(Base):
    __tablename__ = "jobs"
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    image: Mapped[str]
    gpu_profile: Mapped[GPUProfile] = mapped_column(
        Enum(
            GPUProfile,
            name="gpuprofile",
            values_callable=lambda enum_cls: [e.value for e in enum_cls],
        )
    )
    submitted_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=func.now(),
        server_default=func.now(),
        init=False,
    )

    created_by_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    created_by: Mapped[User] = relationship(back_populates="jobs", init=False)

    runs: Mapped[list[JobRun]] = relationship(
        "JobRun", back_populates="job", init=False
    )

    @property
    def latest_run(self) -> JobRun | None:
        if not self.runs:
            return None
        return max(
            self.runs,
            key=lambda run: (_normalize_started_at(run.started_at), run.id),
        )


class JobRun(Base):
    __tablename__ = "job_runs"
    id: Mapped[int] = mapped_column(primary_key=True, init=False)

    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id"))
    job: Mapped[Job] = relationship(Job, back_populates="runs", init=False)

    status: Mapped[RunStatus] = mapped_column(
        Enum(
            RunStatus,
            name="runstatus",
            values_callable=lambda enum_cls: [e.value for e in enum_cls],
        )
    )
    run_token: Mapped[str]
    k8s_job_name: Mapped[str]

    output_volume_id: Mapped[int] = mapped_column(ForeignKey("volumes.id"))
    output_volume: Mapped[Volume] = relationship(
        foreign_keys=[output_volume_id], init=False
    )

    k8s_pod_name: Mapped[str | None] = mapped_column(default=None)
    started_at: Mapped[datetime.datetime | None] = mapped_column(default=None)
    finished_at: Mapped[datetime.datetime | None] = mapped_column(default=None)

    input_volume_id: Mapped[int | None] = mapped_column(
        ForeignKey("volumes.id"), default=None
    )
    input_volume: Mapped[Volume | None] = relationship(
        foreign_keys=[input_volume_id], init=False
    )

    __table_args__ = (
        CheckConstraint(
            "input_volume_id IS NULL OR input_volume_id <> output_volume_id",
            name="ck_job_input_output_distinct",
        ),
        UniqueConstraint("k8s_job_name", name="uq_job_runs_k8s_job_name"),
        UniqueConstraint("k8s_pod_name", name="uq_job_runs_k8s_pod_name"),
    )
