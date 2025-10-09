import datetime
from enum import StrEnum

from sqlalchemy import CheckConstraint, ForeignKey
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
    id: Mapped[int] = mapped_column(primary_key=True)
    pvc_name: Mapped[str]
    size: Mapped[int]
    is_input: Mapped[bool] = mapped_column(default=False)
    state: Mapped[VolumeState] = mapped_column(default=VolumeState.pvc)

    key_prefix: Mapped[str | None]

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
    id: Mapped[int] = mapped_column(primary_key=True)
    image: Mapped[str]
    gpu_profile: Mapped[GPUProfile]
    submitted_at: Mapped[datetime.datetime] = mapped_column(
        default=datetime.datetime.now(datetime.UTC)
    )

    created_by_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    created_by: Mapped[User] = relationship(backref="jobs")

    k8s_job_name: Mapped[str]

    runs: Mapped[list["JobRun"]] = relationship("JobRun", back_populates="job")


class JobRun(Base):
    __tablename__ = "job_runs"
    id: Mapped[int] = mapped_column(primary_key=True)

    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id"))
    job: Mapped[Job] = relationship(Job, back_populates="runs")

    status: Mapped[RunStatus]
    k8s_pod_name: Mapped[str]

    started_at: Mapped[datetime.datetime | None]
    finished_at: Mapped[datetime.datetime | None]

    exit_code: Mapped[int | None]

    input_volume_id: Mapped[int | None] = mapped_column(ForeignKey("volumes.id"))
    input_volume: Mapped[Volume | None] = relationship(foreign_keys=[input_volume_id])

    output_volume_id: Mapped[int] = mapped_column(ForeignKey("volumes.id"))
    output_volume: Mapped[Volume] = relationship(foreign_keys=[output_volume_id])

    __table_args__ = (
        CheckConstraint(
            "input_volume_id IS NULL OR input_volume_id <> output_volume_id",
            name="ck_job_input_output_distinct",
        ),
    )
