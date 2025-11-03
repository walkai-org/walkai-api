from botocore.client import BaseClient
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.aws import get_s3_client
from app.core.database import get_db
from app.schemas.volumes import VolumeListingOut
from app.services import job_service

router = APIRouter(prefix="/volumes", tags=["volumes"])


@router.get("/{volume_id}/objects", response_model=VolumeListingOut)
def list_volume_objects(
    volume_id: int,
    continuation_token: str | None = Query(
        default=None, description="S3 continuation token for pagination"
    ),
    max_keys: int | None = Query(
        default=None,
        ge=1,
        le=1000,
        description="Maximum number of objects returned by the S3 API call",
    ),
    db: Session = Depends(get_db),
    s3_client: BaseClient = Depends(get_s3_client),
    _: object = Depends(get_current_user),
):
    volume = job_service.get_volume(db, volume_id)
    listing = job_service.list_volume_objects(
        s3_client,
        volume,
        continuation_token=continuation_token,
        max_keys=max_keys,
    )
    return listing


@router.get("/{volume_id}/file")
def download_volume_file(
    volume_id: int,
    key: str = Query(..., description="Relative key within the volume prefix"),
    db: Session = Depends(get_db),
    s3_client: BaseClient = Depends(get_s3_client),
    _: object = Depends(get_current_user),
):
    volume = job_service.get_volume(db, volume_id)
    file_stream, metadata = job_service.stream_volume_file(s3_client, volume, key)

    headers: dict[str, str] = {}
    content_length = metadata.get("content_length")
    if content_length is not None:
        headers["Content-Length"] = str(content_length)

    etag = metadata.get("etag")
    if etag:
        headers["ETag"] = etag

    filename = metadata["path"].rsplit("/", 1)[-1].replace('"', "")
    headers["Content-Disposition"] = f'attachment; filename="{filename}"'

    media_type = metadata.get("content_type") or "application/octet-stream"
    return StreamingResponse(file_stream, media_type=media_type, headers=headers)
