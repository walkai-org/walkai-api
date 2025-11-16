from datetime import datetime

from pydantic import BaseModel, ConfigDict


class VolumeOut(BaseModel):
    id: int
    pvc_name: str
    size: int
    key_prefix: str | None
    is_input: bool

    model_config = ConfigDict(from_attributes=True)


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


class InputVolumeCreate(BaseModel):
    storage: int


class InputVolumeFileUpload(BaseModel):
    volume_id: int
    file_names: list[str]


class InputVolumeFileUploadOut(BaseModel):
    presigneds: list[str]


class InputVolumeCreateOut(BaseModel):
    volume: VolumeOut
