from contextlib import asynccontextmanager
from functools import lru_cache

from fastapi import FastAPI
from kubernetes import client
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = Field(default="development", alias="APP_ENV")

    jwt_secret: str = Field(alias="JWT_SECRET")
    jwt_algo: str = Field(alias="JWT_ALGO")
    access_min: int = Field(alias="ACCESS_MIN", ge=1)

    github_client_id: str | None = Field(default=None, alias="GITHUB_CLIENT_ID")
    github_client_secret: str | None = Field(default=None, alias="GITHUB_CLIENT_SECRET")
    github_redirect_uri: str | None = Field(default=None, alias="GITHUB_REDIRECT_URI")
    frontend_home: str | None = Field(default=None, alias="FRONTEND_HOME")
    invite_base_url: str | None = Field(default=None, alias="INVITE_BASE_URL")

    acs_smtp_host: str = Field(default="smtp.azurecomm.net", alias="ACS_SMTP_HOST")
    acs_smtp_port: int = Field(default=587, alias="ACS_SMTP_PORT")
    acs_smtp_username: str | None = Field(default=None, alias="ACS_SMTP_USERNAME")
    acs_smtp_password: str | None = Field(default=None, alias="ACS_SMTP_PASSWORD")
    mail_from: str | None = Field(default=None, alias="MAIL_FROM")

    cluster_token: str = Field(alias="CLUSTER_TOKEN")
    cluster_url: str = Field(alias="CLUSTER_URL")
    namespace: str = Field(default="walkai", alias="JOB_NAMESPACE")
    api_base_url: str = Field(alias="API_BASE_URL")

    aws_access_key_id: str = Field(alias="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = Field(alias="AWS_SECRET_ACCESS_KEY")
    aws_region: str = Field(alias="AWS_REGION")
    aws_s3_bucket: str = Field(alias="AWS_S3_BUCKET")

    database_url: str = Field(alias="DATABASE_URL")
    ecr_arn: str = Field(alias="ECR_ARN")
    ddb_table_oauth = Field(alias="DYNAMODB_OAUTH_TABLE")


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore


@asynccontextmanager
async def lifespan(app: FastAPI):
    from app.core.aws import build_ecr_client, build_s3_client, create_ddb_oauth_table
    from app.core.k8s import build_kubernetes_api_client

    api_client = build_kubernetes_api_client()
    app.state.core = client.CoreV1Api(api_client)
    app.state.batch = client.BatchV1Api(api_client)

    s3_client = build_s3_client()
    app.state.s3_client = s3_client

    ecr_client = build_ecr_client()
    app.state.ecr_client = ecr_client

    ddb_oauth_table = create_ddb_oauth_table()
    app.state.ddb_oauth_table = ddb_oauth_table

    try:
        yield
    finally:
        api_client.close()
        close_s3 = getattr(s3_client, "close", None)
        if callable(close_s3):
            close_s3()
        close_ecr = getattr(ecr_client, "close", None)
        if callable(close_ecr):
            close_ecr()
