from functools import lru_cache

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

    redis_url: str = Field(alias="REDIS_URL")

    acs_smtp_host: str = Field(default="smtp.azurecomm.net", alias="ACS_SMTP_HOST")
    acs_smtp_port: int = Field(default=587, alias="ACS_SMTP_PORT")
    acs_smtp_username: str | None = Field(default=None, alias="ACS_SMTP_USERNAME")
    acs_smtp_password: str | None = Field(default=None, alias="ACS_SMTP_PASSWORD")
    mail_from: str | None = Field(default=None, alias="MAIL_FROM")

    cluster_token: str = Field(alias="CLUSTER_TOKEN")
    cluster_url: str = Field(alias="CLUSTER_URL")
    namespace: str = Field(default="walkai", alias="JOB_NAMESPACE")
    api_base_url: str = Field(alias="API_BASE_URL")

    aws_access_key_id: str = Field(alias="AWS_ACCES_KEY_ID")
    aws_secret_access_key: str = Field(alias="AWS_SECRET_ACCESS_KEY")
    aws_region: str = Field(alias="AWS_REGION")
    aws_s3_bucket: str = Field(alias="AWS_S3_BUCKET")

    database_url: str = Field(alias="DATABASE_URL")


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore
