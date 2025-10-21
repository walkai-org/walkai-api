from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from kubernetes import client

from app.core.config import get_settings
from app.core.redis import create_redis_client


def _build_api_client() -> client.ApiClient:
    settings = get_settings()
    cfg = client.Configuration()
    cfg.host = settings.cluster_url
    cfg.verify_ssl = False
    cfg.api_key = {"authorization": settings.cluster_token}
    cfg.api_key_prefix = {"authorization": "Bearer"}
    return client.ApiClient(configuration=cfg)


@asynccontextmanager
async def lifespan(app: FastAPI):
    api_client = _build_api_client()
    app.state.core = client.CoreV1Api(api_client)
    app.state.batch = client.BatchV1Api(api_client)
    redis_client = create_redis_client()
    app.state.redis = redis_client
    try:
        yield
    finally:
        api_client.close()
        redis_client.close()


def get_core(request: Request) -> client.CoreV1Api:
    return request.app.state.core


def get_batch(request: Request) -> client.BatchV1Api:
    return request.app.state.batch
