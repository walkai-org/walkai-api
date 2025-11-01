import boto3
from boto3.session import Session
from botocore.client import BaseClient
from botocore.config import Config
from fastapi import Request

from app.core.config import get_settings

settings = get_settings()


def _build_session() -> Session:
    return boto3.Session(
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_region,
    )


def build_s3_client() -> BaseClient:
    session = _build_session()
    return session.client("s3", config=Config(signature_version="s3v4"))


def build_ecr_client() -> BaseClient:
    session = _build_session()
    return session.client("ecr")


def get_s3_client(request: Request) -> BaseClient:
    s3_client = getattr(request.app.state, "s3_client", None)
    if s3_client is None:
        raise RuntimeError("S3 client is not configured on application state")
    return s3_client


def get_ecr_client(request: Request) -> BaseClient:
    ecr_client = getattr(request.app.state, "ecr_client", None)
    if ecr_client is None:
        raise RuntimeError("ECR client is not configured on application state")
    return ecr_client


def presign_put_url(s3_client: BaseClient, key: str, expires: int = 3600) -> str:
    return s3_client.generate_presigned_url(
        ClientMethod="put_object",
        Params={"Bucket": settings.aws_s3_bucket, "Key": key},
        ExpiresIn=expires,
        HttpMethod="PUT",
    )


def _build_dynamodb_resource():
    session = _build_session()
    return session.resource("dynamodb")


def create_ddb_oauth_table():
    dynamodb = _build_dynamodb_resource()
    return dynamodb.Table(settings.ddb_table_oauth)


def create_ddb_cluster_cache_table():
    dynamodb = _build_dynamodb_resource()
    return dynamodb.Table(settings.ddb_table_cluster_cache)


def get_ddb_oauth_table(request: Request):
    table = getattr(request.app.state, "ddb_oauth_table", None)
    if table is None:
        raise RuntimeError("DynamoDB table is not configured on application state")
    return table


def get_ddb_cluster_cache_table(request: Request):
    table = getattr(request.app.state, "ddb_cluster_table", None)
    if table is None:
        raise RuntimeError("DynamoDB table is not configured on application state")
    return table
