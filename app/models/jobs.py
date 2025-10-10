import datetime
from enum import StrEnum

from sqlalchemy import CheckConstraint, DateTime, Enum, ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base
from app.models.users import User
from app.schemas.jobs import GPUProfile


class VolumeState(StrEnum):
    pvc = "pvc"
    stored = "stored"
    deleted = "deleted"


class RunStatus(StrEnum):
    pending = "pending"
    scheduled = "scheduled"
    active = "active"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


class Volume(Base):
    __tablename__ = "volumes"
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    pvc_name: Mapped[str]
    size: Mapped[int]
    key_prefix: Mapped[str | None] = mapped_column(default=None)
    is_input: Mapped[bool] = mapped_column(default=False)
    state: Mapped[VolumeState] = mapped_column(
        Enum(VolumeState), default=VolumeState.pvc
    )

    # TODO:
    # Optional: computed helpers (in Python model, not persisted)
    # @property
    # def s3_uri(self) -> str | None:
    #     if self.state == VolumeState.archived_s3 and self.key_prefix:
    #         # read from settings.BUCKET_NAME (and maybe REGION) at runtime
    #         from app.config import settings
    #         return f"s3://{settings.S3_BUCKET}/{self.key_prefix}"
    #     return None


class Job(Base):
    __tablename__ = "jobs"
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    image: Mapped[str]
    gpu_profile: Mapped[GPUProfile] = mapped_column(Enum(GPUProfile))
    submitted_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=func.now(),
        server_default=func.now(),
        init=False,
    )

    created_by_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    created_by: Mapped[User] = relationship(back_populates="jobs", init=False)

    k8s_job_name: Mapped[str]

    runs: Mapped[list["JobRun"]] = relationship(
        "JobRun", back_populates="job", init=False
    )


class JobRun(Base):
    __tablename__ = "job_runs"
    id: Mapped[int] = mapped_column(primary_key=True, init=False)

    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id"))
    job: Mapped[Job] = relationship(Job, back_populates="runs", init=False)

    status: Mapped[RunStatus] = mapped_column(Enum(RunStatus))
    k8s_pod_name: Mapped[str]

    output_volume_id: Mapped[int] = mapped_column(ForeignKey("volumes.id"))
    output_volume: Mapped[Volume] = relationship(
        foreign_keys=[output_volume_id], init=False
    )

    started_at: Mapped[datetime.datetime | None] = mapped_column(default=None)
    finished_at: Mapped[datetime.datetime | None] = mapped_column(default=None)
    exit_code: Mapped[int | None] = mapped_column(default=None)

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
    )
