from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.schemas.secrets import normalize_secret_name


class GPUProfile(StrEnum):
    g1_10 = "1g.10gb"
    g2_20 = "2g.20gb"
    g3_40 = "3g.40gb"
    g4_40 = "4g.40gb"
    g7_79 = "7g.79gb"


class RunStatus(StrEnum):
    pending = "pending"
    scheduled = "scheduled"
    active = "active"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


class JobCreate(BaseModel):
    image: str
    gpu: GPUProfile
    storage: int = 2
    secret_names: list[str] = Field(
        default_factory=list,
        description="Existing Kubernetes secrets to mount via envFrom",
    )
    volume_id: int | None = None

    @field_validator("secret_names")
    @classmethod
    def validate_secret_names(cls, values: list[str] | None) -> list[str]:
        if not values:
            return []
        normalized: list[str] = []
        seen: set[str] = set()
        for value in values:
            normalized_value = normalize_secret_name(value)
            if normalized_value in seen:
                raise ValueError("Secret names must be unique")
            seen.add(normalized_value)
            normalized.append(normalized_value)
        return normalized


class JobRunOut(BaseModel):
    job_id: int
    pod: str


class JobImage(BaseModel):
    image: str
    tag: str
    digest: str | None = None
    pushed_at: datetime | None = None


class JobRunBase(BaseModel):
    id: int
    status: RunStatus
    k8s_pod_name: str | None
    started_at: datetime | None
    finished_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class JobRunSummary(JobRunBase):
    k8s_job_name: str


class VolumeOut(BaseModel):
    id: int
    pvc_name: str
    size: int
    key_prefix: str | None
    is_input: bool

    model_config = ConfigDict(from_attributes=True)


class JobRunDetail(JobRunSummary):
    output_volume: VolumeOut
    input_volume: VolumeOut | None

    model_config = ConfigDict(from_attributes=True)


class JobRunByPodOut(JobRunDetail):
    job_id: int

    model_config = ConfigDict(from_attributes=True)


class JobOut(BaseModel):
    id: int
    image: str
    gpu_profile: GPUProfile
    submitted_at: datetime
    created_by_id: int
    latest_run: JobRunSummary | None

    model_config = ConfigDict(from_attributes=True)


class JobDetailOut(BaseModel):
    id: int
    image: str
    gpu_profile: GPUProfile
    submitted_at: datetime
    created_by_id: int
    runs: list[JobRunBase]

    model_config = ConfigDict(from_attributes=True)
