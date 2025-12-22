from fastapi import FastAPI, Request
from kubernetes import client


def build_kubernetes_api_client(
    *, cluster_url: str, cluster_token: str
) -> client.ApiClient:
    cfg = client.Configuration()
    cfg.host = cluster_url
    cfg.verify_ssl = False
    cfg.api_key = {"authorization": cluster_token}
    cfg.api_key_prefix = {"authorization": "Bearer"}
    return client.ApiClient(configuration=cfg)


async def swap_kubernetes_clients(
    app: FastAPI, *, cluster_url: str, cluster_token: str
) -> None:
    lock = getattr(app.state, "k8s_lock", None)
    if lock is None:
        raise RuntimeError("k8s_lock is not configured on application state")

    async with lock:
        old_api_client = getattr(app.state, "k8s_api_client", None)

        new_api_client = build_kubernetes_api_client(
            cluster_url=cluster_url,
            cluster_token=cluster_token,
        )

        app.state.k8s_api_client = new_api_client
        app.state.core = client.CoreV1Api(new_api_client)
        app.state.batch = client.BatchV1Api(new_api_client)

        if old_api_client is not None:
            try:
                old_api_client.close()
            except Exception:
                pass


def get_core(request: Request) -> client.CoreV1Api:
    return request.app.state.core


def get_batch(request: Request) -> client.BatchV1Api:
    return request.app.state.batch
