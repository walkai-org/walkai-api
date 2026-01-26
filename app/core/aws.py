import json
from typing import Literal, TypedDict

from boto3.session import Session
from botocore.client import BaseClient
from botocore.config import Config
from fastapi import Request

from app.core.config import get_settings

settings = get_settings()


class K8sSecret(TypedDict):
    cluster_url: str
    cluster_token: str


def _build_session() -> Session:
    if settings.aws_access_key_id and settings.aws_secret_access_key:
        return Session(
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )
    return Session(region_name=settings.aws_region)


def build_s3_client() -> BaseClient:
    session = _build_session()
    return session.client("s3", config=Config(signature_version="s3v4"))


def build_ecr_client() -> BaseClient:
    session = _build_session()
    return session.client("ecr")


def build_secrets_manager_client() -> BaseClient:
    session = _build_session()
    return session.client("secretsmanager")


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


def get_secrets_manager_client(request: Request) -> BaseClient:
    sm = getattr(request.app.state, "secrets_manager_client", None)
    if sm is None:
        raise RuntimeError(
            "Secrets Manager client is not configured on application state"
        )
    return sm


def get_k8s_cluster_creds_from_secret(sm_client: BaseClient) -> K8sSecret:
    resp = sm_client.get_secret_value(SecretId=settings.k8s_secret_id)
    raw = resp.get("SecretString") or ""
    data = json.loads(raw)

    if "cluster_url" not in data or "cluster_token" not in data:
        raise RuntimeError("K8S secret must contain 'cluster_url' and 'cluster_token'")

    return {
        "cluster_url": data["cluster_url"],
        "cluster_token": data["cluster_token"],
    }


def get_k8s_cluster_creds_from_settings(settings) -> K8sSecret:
    return {
        "cluster_url": settings.cluster_url,
        "cluster_token": settings.cluster_token,
    }


def put_k8s_cluster_creds_to_secret(
    sm_client: BaseClient,
    *,
    cluster_url: str,
    cluster_token: str,
) -> None:
    sm_client.put_secret_value(
        SecretId=settings.k8s_secret_id,
        SecretString=json.dumps(
            {"cluster_url": cluster_url, "cluster_token": cluster_token}
        ),
    )


def presign_url(
    s3_client: BaseClient,
    key: str,
    method: Literal["GET", "PUT"] = "PUT",
    expires: int = 3600,
) -> str:
    if method == "PUT":
        client_method = "put_object"
    elif method == "GET":
        client_method = "get_object"
    else:
        raise ValueError(f"Unsupported method for presign: {method}")

    return s3_client.generate_presigned_url(
        ClientMethod=client_method,
        Params={"Bucket": settings.aws_s3_bucket, "Key": key},
        ExpiresIn=expires,
        HttpMethod=method,
    )


def list_s3_objects_with_prefix(
    s3_client: BaseClient,
    prefix: str,
) -> list[str]:
    """
    Devuelve una lista de keys en S3 bajo el prefix dado.
    """
    paginator = s3_client.get_paginator("list_objects_v2")

    keys: list[str] = []

    for page in paginator.paginate(
        Bucket=settings.aws_s3_bucket,
        Prefix=prefix,
    ):
        contents = page.get("Contents", [])
        for obj in contents:
            key: str = obj["Key"]
            keys.append(key)

    return keys


def _ensure_table_pk_only(
    ddb_resource, table_name: str, pk_name: str = "pk", pk_type: str = "S"
):
    if settings.app_env == "test":
        return ddb_resource.Table(table_name)

    client = ddb_resource.meta.client
    try:
        client.describe_table(TableName=table_name)
    except client.exceptions.ResourceNotFoundException:
        ddb_resource.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": pk_name, "AttributeType": pk_type}],
            KeySchema=[{"AttributeName": pk_name, "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        ).wait_until_exists()
    return ddb_resource.Table(table_name)


def _build_dynamodb_resource():
    session = _build_session()
    endpoint: str | None = settings.ddb_endpoint
    if endpoint:
        return session.resource("dynamodb", endpoint_url=endpoint)
    return session.resource("dynamodb")


def create_ddb_oauth_table():
    dynamodb = _build_dynamodb_resource()
    if settings.ddb_endpoint:
        return _ensure_table_pk_only(
            dynamodb, settings.ddb_table_oauth, pk_name="pk", pk_type="S"
        )

    return dynamodb.Table(settings.ddb_table_oauth)  # type: ignore


def create_ddb_cluster_cache_table():
    dynamodb = _build_dynamodb_resource()
    if settings.ddb_endpoint:
        return _ensure_table_pk_only(
            dynamodb, settings.ddb_table_cluster_cache, pk_name="pk", pk_type="S"
        )
    return dynamodb.Table(settings.ddb_table_cluster_cache)  # type: ignore


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
