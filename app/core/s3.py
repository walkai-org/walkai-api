import boto3
from botocore.config import Config

from app.core.config import get_settings

settings = get_settings()


def _client():
    session = boto3.Session(
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_region,
    )
    return session.client("s3", config=Config(signature_version="s3v4"))


def presign_put_url(key: str, expires: int = 3600) -> str:
    return _client().generate_presigned_url(
        ClientMethod="put_object",
        Params={"Bucket": settings.aws_s3_bucket, "Key": key},
        ExpiresIn=expires,
        HttpMethod="PUT",
    )
