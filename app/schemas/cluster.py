from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from app.schemas.jobs import GPUProfile


class GPUResources(BaseModel):
    gpu: GPUProfile
    allocated: int
    available: int


class PodStatus(StrEnum):
    running = "Running"
    pending = "Pending"
    container_creating = "ContainerCreating"
    succeeded = "Succeeded"


class Pod(BaseModel):
    name: str
    namespace: str
    status: PodStatus
    gpu: GPUProfile


class ClusterInsightsIn(BaseModel):
    ts: datetime
    gpus: list[GPUResources]
    pods: list[Pod]

    model_config = ConfigDict(from_attributes=True)
