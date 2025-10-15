from enum import StrEnum
from typing import Literal

from pydantic import BaseModel


class GPUProfile(StrEnum):
    g1_10 = "1g.10gb"
    g2_20 = "2g.20gb"
    g3_40 = "3g.40gb"
    g4_40 = "4g.40gb"
    g7_79 = "7g.79gb"


class JobCreate(BaseModel):
    image: str
    gpu: GPUProfile
    storage: int = 2


class JobRunOut(BaseModel):
    job_id: int
    pod: str


class PodList(BaseModel):
    name: str
    namespace: str
    status: Literal["Running", "Pending", "ContainerCreating", "Succeeded"]
