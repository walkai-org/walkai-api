import json
from datetime import UTC, datetime

from app.core.redis import get_redis
from app.main import app
from app.schemas.cluster import ClusterInsightsIn, GPUResources, Pod, PodStatus
from app.schemas.jobs import GPUProfile
from app.services import cluster_service


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def set(self, key: str, value: str) -> None:
        self.store[key] = value

    def get(self, key: str) -> str | None:
        return self.store.get(key)


def _build_payload_json() -> dict:
    ts = datetime.now().isoformat()
    return {
        "ts": ts,
        "gpus": [
            {
                "gpu": GPUProfile.g1_10.value,
                "allocated": 2,
                "available": 4,
            }
        ],
        "pods": [
            {
                "name": "pod-123",
                "namespace": "walkai",
                "status": PodStatus.running.value,
                "gpu": GPUProfile.g1_10.value,
            }
        ],
    }


def test_submit_insights_endpoint_stores_snapshot_in_redis(auth_client):
    client, _ = auth_client
    fake_redis = FakeRedis()
    app.dependency_overrides[get_redis] = lambda: fake_redis

    payload = _build_payload_json()
    try:
        response = client.post("/cluster/insights", json=payload)
    finally:
        app.dependency_overrides.pop(get_redis, None)

    assert response.status_code == 204
    stored = fake_redis.store[cluster_service.INSIGHTS_KEY]
    parsed = json.loads(stored)
    assert parsed["ts"] == payload["ts"]
    assert parsed["gpus"] == payload["gpus"]
    assert parsed["pods"] == payload["pods"]


def test_save_and_load_cluster_insights_roundtrip():
    fake_redis = FakeRedis()
    payload = ClusterInsightsIn(
        ts=datetime.now(UTC),
        gpus=[
            GPUResources(gpu=GPUProfile.g2_20, allocated=1, available=5),
        ],
        pods=[
            Pod(
                name="pod-xyz",
                namespace="walkai",
                status=PodStatus.pending,
                gpu=GPUProfile.g2_20,
            )
        ],
    )

    cluster_service.save_cluster_insights(fake_redis, payload)
    snapshot = cluster_service.load_cluster_insights(fake_redis)

    assert snapshot == payload


def _store_snapshot(redis_client: FakeRedis) -> ClusterInsightsIn:
    snapshot = ClusterInsightsIn(
        ts=datetime.now(UTC),
        gpus=[
            GPUResources(gpu=GPUProfile.g1_10, allocated=3, available=1),
            GPUResources(gpu=GPUProfile.g2_20, allocated=0, available=2),
        ],
        pods=[
            Pod(
                name="pod-a",
                namespace="walkai",
                status=PodStatus.running,
                gpu=GPUProfile.g1_10,
            ),
            Pod(
                name="pod-b",
                namespace="walkai",
                status=PodStatus.pending,
                gpu=GPUProfile.g2_20,
            ),
        ],
    )
    redis_client.set(cluster_service.INSIGHTS_KEY, snapshot.model_dump_json())
    return snapshot


def test_get_resources_returns_latest_snapshot(auth_client):
    client, _ = auth_client
    fake_redis = FakeRedis()
    snapshot = _store_snapshot(fake_redis)
    app.dependency_overrides[get_redis] = lambda: fake_redis

    try:
        response = client.get("/cluster/resources")
    finally:
        app.dependency_overrides.pop(get_redis, None)

    assert response.status_code == 200
    assert response.json() == [
        {
            "gpu": res.gpu.value,
            "allocated": res.allocated,
            "available": res.available,
        }
        for res in snapshot.gpus
    ]


def test_get_pods_returns_404_without_snapshot(auth_client):
    client, _ = auth_client
    fake_redis = FakeRedis()
    app.dependency_overrides[get_redis] = lambda: fake_redis

    try:
        response = client.get("/cluster/pods")
    finally:
        app.dependency_overrides.pop(get_redis, None)

    assert response.status_code == 404
    assert response.json()["detail"] == "Cluster insights not available"
