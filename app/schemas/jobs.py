from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict


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


class JobRunOut(BaseModel):
    job_id: int
    pod: str


class JobRunSummary(BaseModel):
    id: int
    status: RunStatus
    k8s_pod_name: str
    started_at: datetime | None
    finished_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class VolumeOut(BaseModel):
    id: int
    pvc_name: str
    size: int
    key_prefix: str | None
    is_input: bool
    state: str

    model_config = ConfigDict(from_attributes=True)


class JobRunDetail(JobRunSummary):
    output_volume: VolumeOut
    input_volume: VolumeOut | None

    model_config = ConfigDict(from_attributes=True)


class JobOut(BaseModel):
    id: int
    image: str
    gpu_profile: GPUProfile
    submitted_at: datetime
    created_by_id: int
    k8s_job_name: str
    runs: list[JobRunSummary]

    model_config = ConfigDict(from_attributes=True)


class JobDetailOut(JobOut):
    runs: list[JobRunDetail]

    model_config = ConfigDict(from_attributes=True)


class PodList(BaseModel):
    name: str
    namespace: str
    status: Literal["Running", "Pending", "ContainerCreating", "Succeeded"]
