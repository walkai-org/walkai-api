from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from app.schemas.jobs import GPUProfile, JobPriority


class GPUResources(BaseModel):
    gpu: GPUProfile
    allocated: int
    available: int


class PodStatus(StrEnum):
    running = "Running"
    pending = "Pending"
    pod_initializing = "PodInitializing"
    container_creating = "ContainerCreating"
    completed = "Completed"
    error = "Error"
    succeeded = "Succeeded"
    crash = "CrashLoopBackOff"
    failed = "Failed"
    terminating = "Terminating"


class Pod(BaseModel):
    name: str
    namespace: str
    status: PodStatus
    gpu: GPUProfile
    start_time: datetime | None
    finish_time: datetime | None
    priority: JobPriority | None = None


class ClusterInsightsIn(BaseModel):
    ts: datetime
    gpus: list[GPUResources]
    pods: list[Pod]

    model_config = ConfigDict(from_attributes=True)


class ClusterConfigUpdateIn(BaseModel):
    cluster_url: str
    cluster_token: str
