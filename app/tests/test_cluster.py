import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from kubernetes.client import ApiException

from app.core.aws import get_ddb_cluster_cache_table
from app.core.k8s import get_core
from app.main import app
from app.models.jobs import Job, JobRun, RunStatus, Volume
from app.schemas.cluster import ClusterInsightsIn, GPUResources, Pod, PodStatus
from app.schemas.jobs import GPUProfile, JobPriority
from app.services import cluster_service


class FakeDynamoTable:
    def __init__(self) -> None:
        self.items: dict[str, dict] = {}

    def put_item(self, *, Item: dict, ConditionExpression=None) -> dict:  # noqa: ARG002
        self.items[Item["pk"]] = Item
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def get_item(self, *, Key: dict, ConsistentRead=False) -> dict:  # noqa: ARG002
        item = self.items.get(Key["pk"])
        if item is None:
            return {}
        return {"Item": item}


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
                "priority": None,
            }
        ],
    }


def test_submit_insights_endpoint_stores_snapshot_in_dynamodb(auth_client):
    client, _ = auth_client
    fake_ddb = FakeDynamoTable()
    app.dependency_overrides[get_ddb_cluster_cache_table] = lambda: fake_ddb

    payload = _build_payload_json()
    try:
        response = client.post("/cluster/insights", json=payload)
    finally:
        app.dependency_overrides.pop(get_ddb_cluster_cache_table, None)

    assert response.status_code == 204
    stored = fake_ddb.items[cluster_service.INSIGHTS_PK]["data"]
    parsed = json.loads(stored)
    assert parsed["ts"] == payload["ts"]
    assert parsed["gpus"] == payload["gpus"]
    assert parsed["pods"] == payload["pods"]


def test_save_and_load_cluster_insights_roundtrip(db_session):
    fake_ddb = FakeDynamoTable()
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

    cluster_service.save_cluster_insights(fake_ddb, payload, db_session)
    snapshot = cluster_service.load_cluster_insights(fake_ddb)

    assert snapshot == payload


def _store_snapshot(ddb_table: FakeDynamoTable) -> ClusterInsightsIn:
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
    ddb_table.put_item(
        Item={
            "pk": cluster_service.INSIGHTS_PK,
            "data": snapshot.model_dump_json(),
            "updated_at": int(datetime.now(UTC).timestamp()),
        }
    )
    return snapshot


def test_get_resources_returns_latest_snapshot(auth_client):
    client, _ = auth_client
    fake_ddb = FakeDynamoTable()
    snapshot = _store_snapshot(fake_ddb)
    app.dependency_overrides[get_ddb_cluster_cache_table] = lambda: fake_ddb

    try:
        response = client.get("/cluster/resources")
    finally:
        app.dependency_overrides.pop(get_ddb_cluster_cache_table, None)

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
    fake_ddb = FakeDynamoTable()
    app.dependency_overrides[get_ddb_cluster_cache_table] = lambda: fake_ddb

    try:
        response = client.get("/cluster/pods")
    finally:
        app.dependency_overrides.pop(get_ddb_cluster_cache_table, None)

    assert response.status_code == 404
    assert response.json()["detail"] == "Cluster insights not available"


def test_get_pods_returns_priority_from_job_run(auth_client, db_session, test_user):
    client, _ = auth_client
    fake_ddb = FakeDynamoTable()
    app.dependency_overrides[get_ddb_cluster_cache_table] = lambda: fake_ddb

    job = Job(
        image="repo/priority:latest",
        gpu_profile=GPUProfile.g1_10,
        created_by_id=test_user.id,
        priority=JobPriority.extra_high,
    )
    out_volume = Volume(pvc_name="pvc-priority", size=10, is_input=False)
    db_session.add_all([job, out_volume])
    db_session.flush()

    job_run = JobRun(
        job_id=job.id,
        status=RunStatus.active,
        run_token="token-priority",
        k8s_job_name="job-priority",
        k8s_pod_name="pod-priority",
        output_volume_id=out_volume.id,
    )
    db_session.add(job_run)
    db_session.commit()

    pod_start = datetime.now(UTC)
    snapshot = ClusterInsightsIn(
        ts=datetime.now(UTC),
        gpus=[],
        pods=[
            Pod(
                name="pod-priority",
                namespace="walkai",
                status=PodStatus.running,
                gpu=GPUProfile.g1_10,
                start_time=pod_start,
                finish_time=None,
            )
        ],
    )
    fake_ddb.put_item(
        Item={
            "pk": cluster_service.INSIGHTS_PK,
            "data": snapshot.model_dump_json(),
            "updated_at": int(datetime.now(UTC).timestamp()),
        }
    )

    try:
        response = client.get("/cluster/pods")
    finally:
        app.dependency_overrides.pop(get_ddb_cluster_cache_table, None)

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["name"] == "pod-priority"
    assert body[0]["priority"] == JobPriority.extra_high.value


def test_get_pods_returns_null_priority_without_job_run(auth_client):
    client, _ = auth_client
    fake_ddb = FakeDynamoTable()
    app.dependency_overrides[get_ddb_cluster_cache_table] = lambda: fake_ddb

    pod_start = datetime.now(UTC)
    snapshot = ClusterInsightsIn(
        ts=datetime.now(UTC),
        gpus=[],
        pods=[
            Pod(
                name="pod-missing",
                namespace="walkai",
                status=PodStatus.completed,
                gpu=GPUProfile.g2_20,
                start_time=pod_start,
                finish_time=pod_start + timedelta(minutes=1),
            )
        ],
    )
    fake_ddb.put_item(
        Item={
            "pk": cluster_service.INSIGHTS_PK,
            "data": snapshot.model_dump_json(),
            "updated_at": int(datetime.now(UTC).timestamp()),
        }
    )

    try:
        response = client.get("/cluster/pods")
    finally:
        app.dependency_overrides.pop(get_ddb_cluster_cache_table, None)

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["name"] == "pod-missing"
    assert body[0]["priority"] is None


def _fake_pod(*container_names: str) -> SimpleNamespace:
    containers = [SimpleNamespace(name=name) for name in container_names]
    return SimpleNamespace(spec=SimpleNamespace(containers=containers))


def test_stream_pod_logs_returns_text(auth_client):
    client, _ = auth_client

    chunks = [b"line-1\n", b"line-2\n"]

    class FakeResponse:
        def __init__(self) -> None:
            self.closed = False

        def stream(self, amt: int = 0):  # noqa: ARG002 - interface compatibility
            yield from chunks

        def close(self) -> None:
            self.closed = True

    class FakeCore:
        def __init__(self) -> None:
            self.calls: list[dict] = []
            self.pod_calls: list[dict] = []

        def read_namespaced_pod(self, *, name: str, namespace: str):
            self.pod_calls.append({"name": name, "namespace": namespace})
            return _fake_pod(name, f"{name}-uploader")

        def read_namespaced_pod_log(
            self,
            *,
            name: str,
            namespace: str,
            container: str | None,
            follow: bool,
            tail_lines: int | None,
            timestamps: bool,
            _preload_content: bool,
        ) -> FakeResponse:
            self.calls.append(
                {
                    "name": name,
                    "namespace": namespace,
                    "container": container,
                    "follow": follow,
                    "tail_lines": tail_lines,
                    "timestamps": timestamps,
                    "_preload_content": _preload_content,
                }
            )
            assert _preload_content is False
            return FakeResponse()

    fake_core = FakeCore()
    app.dependency_overrides[get_core] = lambda: fake_core

    try:
        response = client.get("/cluster/pods/pod-42/logs")
    finally:
        app.dependency_overrides.pop(get_core, None)

    assert response.status_code == 200
    assert response.text == "line-1\nline-2\n"
    assert response.headers["content-type"].startswith("text/plain")

    assert fake_core.pod_calls == [{"name": "pod-42", "namespace": "walkai"}]
    assert fake_core.calls == [
        {
            "name": "pod-42",
            "namespace": "walkai",
            "container": "pod-42",
            "follow": True,
            "tail_lines": 200,
            "timestamps": True,
            "_preload_content": False,
        }
    ]


def test_stream_pod_logs_propagates_not_found(auth_client):
    client, _ = auth_client

    class FakeCore:
        def read_namespaced_pod(self, *, name: str, namespace: str):  # noqa: ARG002
            return _fake_pod(name, f"{name}-uploader")

        def read_namespaced_pod_log(self, *_, **__):
            raise ApiException(status=404)

    app.dependency_overrides[get_core] = lambda: FakeCore()

    try:
        response = client.get("/cluster/pods/missing/logs")
    finally:
        app.dependency_overrides.pop(get_core, None)

    assert response.status_code == 404
    assert response.json()["detail"] == "Pod missing not found"


def _as_naive_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(UTC).replace(tzinfo=None)


def test_save_cluster_insights_updates_job_runs(db_session, test_user):
    fake_ddb = FakeDynamoTable()

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
    second_pod_finish = second_run_start + timedelta(minutes=5)
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

    cluster_service.save_cluster_insights(fake_ddb, payload, db_session)

    db_session.refresh(first_run)
    db_session.refresh(second_run)

    assert first_run.status == RunStatus.active
    assert first_run.started_at == _as_naive_utc(first_pod_start)
    assert first_run.finished_at is None

    assert second_run.status == RunStatus.succeeded
    assert second_run.started_at == _as_naive_utc(second_run_start)
    assert second_run.finished_at == _as_naive_utc(second_pod_finish)
    assert second_run.billable_minutes == 5


def test_cluster_updates_usage_for_high_priority(db_session, test_user):
    job = Job(
        image="repo/image:tag",
        gpu_profile=GPUProfile.g1_10,
        created_by_id=test_user.id,
        priority=JobPriority.high,
    )
    out_volume = Volume(pvc_name="pvc-3", size=10, is_input=False)
    db_session.add_all([job, out_volume])
    db_session.flush()

    started_at = datetime.now(UTC) - timedelta(minutes=3)
    job_run = JobRun(
        job_id=job.id,
        status=RunStatus.active,
        run_token="token-usage",
        k8s_job_name="job-usage",
        k8s_pod_name="pod-usage",
        output_volume_id=out_volume.id,
        started_at=started_at,
        user_id=test_user.id,
    )
    db_session.add(job_run)
    db_session.commit()

    finish_time = started_at + timedelta(minutes=4)
    payload = ClusterInsightsIn(
        ts=datetime.now(UTC),
        gpus=[],
        pods=[
            Pod(
                name="pod-usage",
                namespace="walkai",
                status=PodStatus.completed,
                gpu=GPUProfile.g1_10,
                start_time=started_at,
                finish_time=finish_time,
            )
        ],
    )

    cluster_service.save_cluster_insights(FakeDynamoTable(), payload, db_session)

    db_session.refresh(job_run)
    db_session.refresh(test_user)
    assert job_run.billable_minutes == 4
    assert test_user.high_priority_minutes_used == 4


def test_cluster_updates_pod_and_attempts_on_recreation(db_session, test_user):
    fake_ddb = FakeDynamoTable()

    job = Job(
        image="repo/image:tag",
        gpu_profile=GPUProfile.g1_10,
        created_by_id=test_user.id,
    )
    volume = Volume(pvc_name="pvc-recreate", size=10, is_input=False)
    db_session.add_all([job, volume])
    db_session.flush()

    job_run = JobRun(
        job_id=job.id,
        status=RunStatus.pending,
        run_token="token-recreate",
        k8s_job_name="job-recreate",
        k8s_pod_name="job-recreate-aaaaa",
        output_volume_id=volume.id,
    )
    db_session.add(job_run)
    db_session.commit()

    first_start = datetime.now(UTC) - timedelta(minutes=10)
    first_payload = ClusterInsightsIn(
        ts=datetime.now(UTC),
        gpus=[],
        pods=[
            Pod(
                name="job-recreate-aaaaa",
                namespace="walkai",
                status=PodStatus.running,
                gpu=GPUProfile.g1_10,
                start_time=first_start,
                finish_time=None,
            )
        ],
    )

    cluster_service.save_cluster_insights(fake_ddb, first_payload, db_session)
    db_session.refresh(job_run)

    assert job_run.attempts == 1
    assert job_run.k8s_pod_name == "job-recreate-aaaaa"
    assert job_run.started_at == _as_naive_utc(first_start)
    assert job_run.first_started_at == _as_naive_utc(first_start)

    new_start = datetime.now(UTC)
    second_payload = ClusterInsightsIn(
        ts=datetime.now(UTC),
        gpus=[],
        pods=[
            Pod(
                name="job-recreate-aaaaa",
                namespace="walkai",
                status=PodStatus.failed,
                gpu=GPUProfile.g1_10,
                start_time=first_start,
                finish_time=new_start - timedelta(minutes=5),
            ),
            Pod(
                name="job-recreate-bbbbb",
                namespace="walkai",
                status=PodStatus.running,
                gpu=GPUProfile.g1_10,
                start_time=new_start,
                finish_time=None,
            ),
        ],
    )

    cluster_service.save_cluster_insights(fake_ddb, second_payload, db_session)
    db_session.refresh(job_run)

    assert job_run.k8s_pod_name == "job-recreate-bbbbb"
    assert job_run.attempts == 2
    assert job_run.started_at == _as_naive_utc(new_start)
    assert job_run.first_started_at == _as_naive_utc(first_start)
