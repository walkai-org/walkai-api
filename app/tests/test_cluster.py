import json
from datetime import UTC, datetime, timedelta

from app.core.redis import get_redis
from app.main import app
from app.models.jobs import Job, JobRun, RunStatus, Volume
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
    ts = datetime.now(UTC)
    start = datetime.now(UTC)
    return {
        "ts": ts.isoformat().replace("+00:00", "Z"),
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
                "start_time": start.isoformat().replace("+00:00", "Z"),
                "finish_time": None,
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


def test_save_and_load_cluster_insights_roundtrip(db_session):
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
                start_time=datetime.now(UTC),
                finish_time=None,
            )
        ],
    )

    cluster_service.save_cluster_insights(fake_redis, payload, db_session)
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
                start_time=datetime.now(UTC),
                finish_time=None,
            ),
            Pod(
                name="pod-b",
                namespace="walkai",
                status=PodStatus.pending,
                gpu=GPUProfile.g2_20,
                start_time=datetime.now(UTC),
                finish_time=None,
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


def _as_naive_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(UTC).replace(tzinfo=None)


def test_save_cluster_insights_updates_job_runs(db_session, test_user):
    fake_redis = FakeRedis()

    job = Job(
        image="repo/image:1.0",
        gpu_profile=GPUProfile.g1_10,
        created_by_id=test_user.id,
    )
    out_volume_one = Volume(pvc_name="pvc-1", size=10, is_input=False)
    out_volume_two = Volume(pvc_name="pvc-2", size=10, is_input=False)

    db_session.add_all([job, out_volume_one, out_volume_two])
    db_session.flush()

    first_run = JobRun(
        job_id=job.id,
        status=RunStatus.pending,
        run_token="token-1",
        k8s_job_name="job-1",
        k8s_pod_name="pod-123",
        output_volume_id=out_volume_one.id,
    )
    second_run_start = datetime.now(UTC) - timedelta(minutes=5)
    second_run = JobRun(
        job_id=job.id,
        status=RunStatus.active,
        run_token="token-2",
        k8s_job_name="job-2",
        k8s_pod_name="pod-456",
        output_volume_id=out_volume_two.id,
        started_at=second_run_start,
    )

    db_session.add_all([first_run, second_run])
    db_session.commit()

    first_pod_start = datetime.now(UTC)
    second_pod_finish = datetime.now(UTC)
    payload = ClusterInsightsIn(
        ts=datetime.now(UTC),
        gpus=[],
        pods=[
            Pod(
                name="pod-123",
                namespace="walkai",
                status=PodStatus.running,
                gpu=GPUProfile.g1_10,
                start_time=first_pod_start,
                finish_time=None,
            ),
            Pod(
                name="pod-456",
                namespace="walkai",
                status=PodStatus.completed,
                gpu=GPUProfile.g1_10,
                start_time=second_run_start,
                finish_time=second_pod_finish,
            ),
        ],
    )

    cluster_service.save_cluster_insights(fake_redis, payload, db_session)

    db_session.refresh(first_run)
    db_session.refresh(second_run)

    assert first_run.status == RunStatus.active
    assert first_run.started_at == _as_naive_utc(first_pod_start)
    assert first_run.finished_at is None

    assert second_run.status == RunStatus.succeeded
    assert second_run.started_at == _as_naive_utc(second_run_start)
    assert second_run.finished_at == _as_naive_utc(second_pod_finish)
