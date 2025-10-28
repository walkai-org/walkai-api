from fastapi import Request
from kubernetes import client

from app.core.config import get_settings


def build_kubernetes_api_client() -> client.ApiClient:
    settings = get_settings()
    cfg = client.Configuration()
    cfg.host = settings.cluster_url
    cfg.verify_ssl = False
    cfg.api_key = {"authorization": settings.cluster_token}
    cfg.api_key_prefix = {"authorization": "Bearer"}
    return client.ApiClient(configuration=cfg)


def get_core(request: Request) -> client.CoreV1Api:
    return request.app.state.core


def get_batch(request: Request) -> client.BatchV1Api:
    return request.app.state.batch
