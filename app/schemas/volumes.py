from datetime import datetime

from pydantic import BaseModel, ConfigDict


class VolumeObject(BaseModel):
    key: str
    size: int
    last_modified: datetime | None = None
    etag: str | None = None


class VolumeListingOut(BaseModel):
    prefix: str
    objects: list[VolumeObject]
    truncated: bool
    next_continuation_token: str | None = None

    model_config = ConfigDict(use_enum_values=True)
