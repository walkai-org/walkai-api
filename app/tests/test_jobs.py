import base64
import json
from datetime import UTC, datetime, timedelta
from io import BytesIO
from types import SimpleNamespace

import boto3
import pytest
from botocore.response import StreamingBody
from botocore.stub import Stubber
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError

import app.api.jobs as jobs_api
from app.core.aws import get_ecr_client, get_s3_client
from app.core.k8s import get_batch, get_core
from app.main import app
from app.models.jobs import JobRun, RunStatus
from app.schemas.jobs import GPUProfile, JobCreate, JobPriority
from app.services import job_service


def test_submit_job_returns_job_run(auth_client, db_session, monkeypatch):
    client, user = auth_client
    fake_core = object()
    fake_batch = object()
    fake_ecr = object()

    app.dependency_overrides[get_core] = lambda: fake_core
    app.dependency_overrides[get_batch] = lambda: fake_batch
    app.dependency_overrides[get_ecr_client] = lambda: fake_ecr
    captured: dict[str, object] = {}

    def fake_create(core, batch, ecr_client, db, payload, current_user):
        captured["core"] = core
        captured["batch"] = batch
        captured["ecr_client"] = ecr_client
        captured["db"] = db
        captured["payload"] = payload
        captured["user"] = current_user
        return SimpleNamespace(job_id=42, k8s_pod_name="pod-123")

    monkeypatch.setattr(job_service, "create_and_run_job", fake_create)

    try:
        response = client.post(
            "/jobs/",
            json={
                "image": "repo/image:tag",
                "gpu": GPUProfile.g1_10.value,
                "storage": 4,
                "secret_names": ["api-token", "db-secret"],
            },
        )
    finally:
        app.dependency_overrides.pop(get_core, None)
        app.dependency_overrides.pop(get_batch, None)
        app.dependency_overrides.pop(get_ecr_client, None)

    assert response.status_code == 200
    assert response.json() == {"job_id": 42, "pod": "pod-123"}
    assert captured["core"] is fake_core
    assert captured["batch"] is fake_batch
    assert captured["ecr_client"] is fake_ecr
    assert captured["db"] is db_session
    assert isinstance(captured["payload"], JobCreate)
    assert captured["payload"].image == "repo/image:tag"
    assert captured["payload"].gpu == GPUProfile.g1_10
    assert captured["payload"].storage == 4
    assert captured["payload"].secret_names == ["api-token", "db-secret"]
    assert captured["payload"].priority == JobPriority.medium
    assert captured["user"].id == user.id


def test_rerun_job_endpoint_calls_service(auth_client, db_session, monkeypatch):
    client, user = auth_client
    fake_core = object()
    fake_batch = object()
    fake_ecr = object()

    app.dependency_overrides[get_core] = lambda: fake_core
    app.dependency_overrides[get_batch] = lambda: fake_batch
    app.dependency_overrides[get_ecr_client] = lambda: fake_ecr

    captured: dict[str, object] = {}

    def fake_rerun(
        core, batch, ecr_client, db, job_id: int, run_user=None, is_scheduled=False
    ):  # noqa: ARG001
        captured["core"] = core
        captured["batch"] = batch
        captured["ecr_client"] = ecr_client
        captured["db"] = db
        captured["job_id"] = job_id
        captured["run_user"] = run_user
        captured["is_scheduled"] = is_scheduled
        return SimpleNamespace(job_id=job_id, k8s_pod_name="pod-rerun")

    monkeypatch.setattr(job_service, "rerun_job", fake_rerun)

    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g1_10, storage=2)
    job = job_service.create_job(db_session, payload, user.id)
    db_session.commit()

    try:
        response = client.post(f"/jobs/{job.id}/runs")
    finally:
        app.dependency_overrides.pop(get_core, None)
        app.dependency_overrides.pop(get_batch, None)
        app.dependency_overrides.pop(get_ecr_client, None)

    assert response.status_code == 200
    assert response.json() == {"job_id": job.id, "pod": "pod-rerun"}
    assert captured["core"] is fake_core
    assert captured["batch"] is fake_batch
    assert captured["ecr_client"] is fake_ecr
    assert captured["db"] is db_session
    assert captured["job_id"] == job.id
    assert captured["is_scheduled"] is False
    assert captured["run_user"] == user


def test_submit_job_blocks_when_quota_exhausted(auth_client, db_session, monkeypatch):
    client, user = auth_client
    payload = JobCreate(
        image="repo/image:tag",
        gpu=GPUProfile.g1_10,
        storage=2,
        priority=JobPriority.high,
    )
    job_service.create_job(db_session, payload, user.id)
    job_service.create_volume(db_session, storage=payload.storage, is_input=False)
    user.high_priority_minutes_used = user.high_priority_quota_minutes + 10
    user.quota_resets_at = datetime.now(UTC) + timedelta(days=1)
    db_session.flush()
    db_session.commit()

    app.dependency_overrides[get_core] = lambda: object()
    app.dependency_overrides[get_batch] = lambda: object()
    app.dependency_overrides[get_ecr_client] = lambda: object()

    calls = {"count": 0}

    def fake_create(*args, **kwargs):  # noqa: ARG001
        calls["count"] += 1
        return SimpleNamespace(job_id=999, k8s_pod_name="pod-should-not-run")

    monkeypatch.setattr(job_service, "create_and_run_job", fake_create)

    try:
        response = client.post(
            "/jobs/",
            json={
                "image": "repo/image:tag",
                "gpu": GPUProfile.g1_10.value,
                "storage": 2,
                "priority": JobPriority.high.value,
            },
        )
    finally:
        app.dependency_overrides.pop(get_core, None)
        app.dependency_overrides.pop(get_batch, None)
        app.dependency_overrides.pop(get_ecr_client, None)

    assert response.status_code == 403
    assert calls["count"] == 0


def test_rerun_job_blocks_when_quota_exhausted(auth_client, db_session, monkeypatch):
    client, user = auth_client
    payload = JobCreate(
        image="repo/image:tag",
        gpu=GPUProfile.g1_10,
        storage=2,
        priority=JobPriority.extra_high,
    )
    job = job_service.create_job(db_session, payload, user.id)
    job_service.create_volume(db_session, storage=payload.storage, is_input=False)
    user.high_priority_minutes_used = user.high_priority_quota_minutes + 1
    user.quota_resets_at = datetime.now(UTC) + timedelta(days=1)
    db_session.flush()
    db_session.commit()

    app.dependency_overrides[get_core] = lambda: object()
    app.dependency_overrides[get_batch] = lambda: object()
    app.dependency_overrides[get_ecr_client] = lambda: object()

    rerun_called = {"value": False}

    def fake_rerun(*args, **kwargs):  # noqa: ARG001
        rerun_called["value"] = True
        return SimpleNamespace(job_id=job.id, k8s_pod_name="pod-rerun")

    monkeypatch.setattr(job_service, "rerun_job", fake_rerun)

    try:
        response = client.post(f"/jobs/{job.id}/runs")
    finally:
        app.dependency_overrides.pop(get_core, None)
        app.dependency_overrides.pop(get_batch, None)
        app.dependency_overrides.pop(get_ecr_client, None)

    assert response.status_code == 403
    assert rerun_called["value"] is False


def test_list_jobs_returns_jobs(auth_client, db_session):
    client, user = auth_client
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g2_20, storage=6)
    job = job_service.create_job(db_session, payload, user.id)
    output_volume = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    run = job_service.create_job_run(db_session, job, output_volume)
    # Ensure k8s_pod_name is non-null to satisfy schema
    run.k8s_pod_name = "pod-abc"
    db_session.commit()

    response = client.get("/jobs/")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    job_item = data[0]
    assert job_item["id"] == job.id
    assert job_item["image"] == payload.image
    assert job_item["gpu_profile"] == payload.gpu.value
    assert job_item["priority"] == JobPriority.medium.value
    assert job_item["created_by_id"] == user.id
    assert job_item["submitted_at"]
    assert job_item["latest_run"] == {
        "id": run.id,
        "status": run.status.value,
        "k8s_job_name": run.k8s_job_name,
        "k8s_pod_name": run.k8s_pod_name,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
    }


def test_quota_usage_ignores_scheduled_runs(db_session, test_user):
    payload = JobCreate(
        image="repo/image:tag",
        gpu=GPUProfile.g1_10,
        storage=2,
        priority=JobPriority.high,
    )
    job = job_service.create_job(db_session, payload, test_user.id)
    output_volume = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    scheduled_run = job_service.create_job_run(
        db_session, job, output_volume, is_scheduled=True
    )
    scheduled_run.user_id = test_user.id
    scheduled_run.billable_minutes = 50
    scheduled_run.status = RunStatus.succeeded
    db_session.commit()

    assert test_user.high_priority_minutes_used == 0


def test_list_job_images_returns_available_registry_images(auth_client, monkeypatch):
    client, _ = auth_client
    monkeypatch.setattr(
        job_service.settings,
        "ecr_url",
        "https://registry.local/jobs",
        raising=False,
    )

    class FakePaginator:
        def paginate(self, **kwargs):
            return iter(
                [
                    {
                        "imageDetails": [
                            {
                                "imageTags": ["latest"],
                                "imageDigest": "sha256:aaa",
                                "imagePushedAt": datetime(2024, 1, 1, tzinfo=UTC),
                            }
                        ]
                    }
                ]
            )

    class FakeECR:
        def get_paginator(self, name):
            assert name == "describe_images"
            return FakePaginator()

    app.dependency_overrides[get_ecr_client] = lambda: FakeECR()
    try:
        response = client.get("/jobs/images")
    finally:
        app.dependency_overrides.pop(get_ecr_client, None)

    assert response.status_code == 200
    data = response.json()
    assert data == [
        {
            "image": "https://registry.local/jobs:latest",
            "tag": "latest",
            "digest": "sha256:aaa",
            "pushed_at": "2024-01-01T00:00:00Z",
        }
    ]


def test_list_jobs_picks_run_with_latest_started_time(auth_client, db_session):
    client, user = auth_client
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g3_40, storage=6)
    job = job_service.create_job(db_session, payload, user.id)

    vol_one = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    vol_two = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )

    earlier_run = job_service.create_job_run(db_session, job, vol_one)
    later_run = job_service.create_job_run(db_session, job, vol_two)

    earlier_start = datetime.now(UTC) - timedelta(minutes=10)
    later_start = earlier_start + timedelta(minutes=5)

    earlier_run.k8s_pod_name = "pod-earlier"
    later_run.k8s_pod_name = "pod-later"
    earlier_run.started_at = earlier_start
    later_run.started_at = later_start
    db_session.commit()

    response = client.get("/jobs/")

    assert response.status_code == 200
    latest_run = response.json()[0]["latest_run"]
    assert latest_run["id"] == later_run.id


def test_get_job_detail_returns_runs_without_volume_data(auth_client, db_session):
    client, user = auth_client
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g3_40, storage=8)
    job = job_service.create_job(db_session, payload, user.id)
    output_volume = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    run = job_service.create_job_run(db_session, job, output_volume)
    # set pod name after creation to reflect current API
    run.k8s_pod_name = "pod-xyz"
    input_volume = job_service.create_volume(db_session, storage=1, is_input=True)
    run.input_volume_id = input_volume.id
    db_session.commit()

    response = client.get(f"/jobs/{job.id}")

    assert response.status_code == 200
    data = response.json()
    assert data["id"] == job.id
    assert data["runs"]
    assert len(data["runs"]) == 1
    assert data["priority"] == JobPriority.medium.value
    run_data = data["runs"][0]
    assert run_data["id"] == run.id
    assert run_data["status"] == run.status.value
    assert run_data["k8s_pod_name"] == run.k8s_pod_name
    assert run.output_volume_id == output_volume.id
    assert run.input_volume_id == input_volume.id
    assert "k8s_job_name" not in run_data
    assert "output_volume" not in run_data
    assert "input_volume" not in run_data


def test_get_job_detail_not_found(auth_client):
    client, _ = auth_client

    response = client.get("/jobs/9999")

    assert response.status_code == 404
    assert response.json()["detail"] == "Job 9999 not found"


def test_get_job_run_detail_includes_volume_information(auth_client, db_session):
    client, user = auth_client
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g3_40, storage=8)
    job = job_service.create_job(db_session, payload, user.id)
    output_volume = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    run = job_service.create_job_run(db_session, job, output_volume)
    run.k8s_pod_name = "pod-run-detail"
    input_volume = job_service.create_volume(db_session, storage=1, is_input=True)
    run.input_volume_id = input_volume.id
    db_session.commit()

    response = client.get(f"/jobs/{job.id}/runs/{run.id}")

    assert response.status_code == 200
    run_data = response.json()
    assert run_data["id"] == run.id
    assert run_data["status"] == run.status.value
    assert run_data["attempts"] == run.attempts
    assert run_data["first_started_at"] == run.first_started_at
    assert run_data["k8s_job_name"] == run.k8s_job_name
    assert run_data["k8s_pod_name"] == run.k8s_pod_name
    assert run_data["secret_names"] == []
    assert run_data["output_volume"] == {
        "id": output_volume.id,
        "pvc_name": output_volume.pvc_name,
        "size": output_volume.size,
        "key_prefix": output_volume.key_prefix,
        "is_input": output_volume.is_input,
    }
    assert run_data["input_volume"] == {
        "id": input_volume.id,
        "pvc_name": input_volume.pvc_name,
        "size": input_volume.size,
        "key_prefix": input_volume.key_prefix,
        "is_input": input_volume.is_input,
    }


def test_get_job_run_by_pod_returns_job_and_run(auth_client, db_session):
    client, user = auth_client
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g4_40, storage=4)
    job = job_service.create_job(db_session, payload, user.id)
    output_volume = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    run = job_service.create_job_run(db_session, job, output_volume)
    run.k8s_pod_name = "pod-query"
    db_session.commit()

    response = client.get(f"/jobs/runs/by-pod/{run.k8s_pod_name}")

    assert response.status_code == 200
    data = response.json()
    assert data["job_id"] == job.id
    assert data["id"] == run.id
    assert data["status"] == run.status.value
    assert data["attempts"] == run.attempts
    assert data["first_started_at"] == run.first_started_at
    assert data["output_volume"]["id"] == output_volume.id
    assert data["secret_names"] == []


def test_get_job_run_by_pod_returns_404_when_missing(auth_client):
    client, _ = auth_client

    response = client.get("/jobs/runs/by-pod/non-existent-pod")

    assert response.status_code == 404
    assert response.json()["detail"] == "Job run not found"


def test_get_job_run_logs_streams_from_s3(auth_client, db_session):
    client, user = auth_client

    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g1_10, storage=2)
    job = job_service.create_job(db_session, payload, user.id)
    volume = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    run = job_service.create_job_run(db_session, job, volume)
    run.k8s_pod_name = "pod-logs"
    prefix = f"users/{user.id}/jobs/{job.id}/{run.id}/outputs"
    run.output_volume.key_prefix = prefix
    db_session.commit()

    log_key = f"{prefix}/logs/{run.k8s_job_name}.log"
    log_bytes = b"line-1\nline-2\n"
    s3_client = boto3.client(
        "s3",
        region_name="us-test-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    stubber = Stubber(s3_client)
    stubber.add_response(
        "get_object",
        {"Body": StreamingBody(BytesIO(log_bytes), len(log_bytes))},
        {"Bucket": "test-bucket", "Key": log_key},
    )
    stubber.activate()

    app.dependency_overrides[get_s3_client] = lambda: s3_client
    try:
        response = client.get(f"/jobs/{job.id}/runs/{run.id}/logs")
    finally:
        app.dependency_overrides.pop(get_s3_client, None)
        stubber.assert_no_pending_responses()
        stubber.deactivate()

    assert response.status_code == 200
    assert response.content == log_bytes
    assert response.headers["content-type"] == "text/plain; charset=utf-8"


def test_get_job_run_logs_returns_404_when_missing(auth_client, db_session):
    client, user = auth_client

    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g1_10, storage=2)
    job = job_service.create_job(db_session, payload, user.id)
    volume = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    run = job_service.create_job_run(db_session, job, volume)
    run.k8s_pod_name = "pod-missing"
    prefix = f"users/{user.id}/jobs/{job.id}/{run.id}/outputs"
    run.output_volume.key_prefix = prefix
    db_session.commit()

    log_key = f"{prefix}/logs/{run.k8s_job_name}.log"
    s3_client = boto3.client(
        "s3",
        region_name="us-test-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    stubber = Stubber(s3_client)
    stubber.add_client_error(
        "get_object",
        service_error_code="NoSuchKey",
        service_message="Not found",
        http_status_code=404,
        expected_params={"Bucket": "test-bucket", "Key": log_key},
    )
    stubber.activate()

    app.dependency_overrides[get_s3_client] = lambda: s3_client
    try:
        response = client.get(f"/jobs/{job.id}/runs/{run.id}/logs")
    finally:
        app.dependency_overrides.pop(get_s3_client, None)
        stubber.assert_no_pending_responses()
        stubber.deactivate()

    assert response.status_code == 404
    assert response.json()["detail"] == "Log file not found"


def test_presign_object_sets_output_prefix(auth_client, db_session, monkeypatch):
    client, user = auth_client
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g1_10, storage=2)
    job = job_service.create_job(db_session, payload, user.id)
    out_volume = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    run = job_service.create_job_run(db_session, job, out_volume)
    db_session.commit()

    calls: list[dict[str, object]] = []

    def _fake_presign(s3_client, key, method="PUT"):
        calls.append({"key": key, "method": method})
        return f"https://example.com/{key}"

    monkeypatch.setattr(jobs_api, "presign_url", _fake_presign)

    class _StubS3:
        pass

    app.dependency_overrides[get_s3_client] = lambda: _StubS3()
    try:
        response = client.get(
            f"/jobs/{job.id}/runs/{run.id}/presign",
            params={"path": "results/file.txt"},
            headers={"X-Run-Token": run.run_token},
        )
    finally:
        app.dependency_overrides.pop(get_s3_client, None)

    assert response.status_code == 200
    db_session.refresh(run.output_volume)
    expected_prefix = f"users/{user.id}/jobs/{job.id}/{run.id}/outputs"
    assert run.output_volume.key_prefix == expected_prefix
    assert calls == [{"key": f"{expected_prefix}/results/file.txt", "method": "PUT"}]
    assert response.json()["url"].endswith("results/file.txt")


def test_presign_object_for_input_enforces_get(auth_client, db_session, monkeypatch):
    client, user = auth_client
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g1_10, storage=2)
    job = job_service.create_job(db_session, payload, user.id)
    output_volume = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    input_volume = job_service.create_volume(db_session, storage=1, is_input=True)
    input_volume.key_prefix = f"users/{user.id}/inputs/input-vol"
    run = job_service.create_job_run(
        db_session, job, output_volume, input_pvc=input_volume
    )
    db_session.commit()

    calls: list[dict[str, object]] = []

    def _fake_presign(s3_client, key, method="PUT"):
        calls.append({"key": key, "method": method})
        return f"https://example.com/{key}"

    monkeypatch.setattr(jobs_api, "presign_url", _fake_presign)

    class _StubS3:
        pass

    app.dependency_overrides[get_s3_client] = lambda: _StubS3()
    try:
        bad_resp = client.get(
            f"/jobs/{job.id}/runs/{run.id}/presign",
            params={
                "path": "data/input.txt",
                "direction": "input",
                "method": "PUT",
            },
            headers={"X-Run-Token": run.run_token},
        )
        good_resp = client.get(
            f"/jobs/{job.id}/runs/{run.id}/presign",
            params={
                "path": "data/input.txt",
                "direction": "input",
                "method": "GET",
            },
            headers={"X-Run-Token": run.run_token},
        )
    finally:
        app.dependency_overrides.pop(get_s3_client, None)

    assert bad_resp.status_code == 400
    assert bad_resp.json()["detail"] == "Inputs only support method=GET for presign"

    assert good_resp.status_code == 200
    assert calls == [
        {
            "key": f"{input_volume.key_prefix}/data/input.txt",
            "method": "GET",
        }
    ]
    assert good_resp.json()["url"].endswith("data/input.txt")


def test_list_input_objects_returns_relative_paths(
    auth_client, db_session, monkeypatch
):
    client, user = auth_client
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g1_10, storage=2)
    job = job_service.create_job(db_session, payload, user.id)
    output_volume = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    input_volume = job_service.create_volume(db_session, storage=1, is_input=True)
    input_volume.key_prefix = f"users/{user.id}/inputs/input-items"
    run = job_service.create_job_run(
        db_session, job, output_volume, input_pvc=input_volume
    )
    db_session.commit()

    captured: dict[str, str] = {}

    def _fake_list(s3_client, prefix: str):
        captured["prefix"] = prefix
        return [
            f"{prefix}file-1.txt",
            f"{prefix}nested/file-2.bin",
        ]

    monkeypatch.setattr(jobs_api, "list_s3_objects_with_prefix", _fake_list)

    class _StubS3:
        pass

    app.dependency_overrides[get_s3_client] = lambda: _StubS3()
    try:
        response = client.get(
            f"/jobs/{job.id}/runs/{run.id}/inputs",
            headers={"X-Run-Token": run.run_token},
        )
    finally:
        app.dependency_overrides.pop(get_s3_client, None)

    assert response.status_code == 200
    assert captured["prefix"] == f"{input_volume.key_prefix}/"
    assert response.json() == {"files": ["file-1.txt", "nested/file-2.bin"]}


def test_render_pvc_manifest_shapes_storage():
    manifest = job_service._render_persistent_volume_claim(
        name="vol-123",
        storage=5,
    )

    assert manifest["apiVersion"] == "v1"
    assert manifest["kind"] == "PersistentVolumeClaim"
    assert manifest["metadata"]["name"] == "vol-123"
    assert manifest["spec"]["resources"]["requests"]["storage"] == "5Gi"
    assert manifest["spec"]["accessModes"] == ["ReadWriteOnce"]


def test_render_job_manifest_populates_gpu_limits():
    manifest = job_service._render_job_manifest(
        image="repo/image:tag",
        gpu=GPUProfile.g1_10,
        job_name="job-123",
        output_claim="claim-1",
        run_id=1,
        job_id=2,
        run_token="run-token-abc",
        api_base_url="https://api.example.com",
        input_volume=None,
    )

    container = manifest["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == "repo/image:tag"
    assert container["resources"] == {"limits": {"nvidia.com/mig-1g.10gb": 1}}
    assert (
        manifest["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"][
            "claimName"
        ]
        == "claim-1"
    )


def test_render_job_manifest_sets_priority_class():
    manifest = job_service._render_job_manifest(
        image="repo/image:tag",
        gpu=GPUProfile.g1_10,
        job_name="job-123",
        output_claim="claim-1",
        run_id=1,
        job_id=2,
        run_token="run-token-abc",
        api_base_url="https://api.example.com",
        input_volume=None,
        priority=JobPriority.high,
    )

    pod_spec = manifest["spec"]["template"]["spec"]
    assert pod_spec["priorityClassName"] == "nos-priority-high"


def test_render_job_manifest_includes_secret_env_from():
    manifest = job_service._render_job_manifest(
        image="repo/image:tag",
        gpu=GPUProfile.g1_10,
        job_name="job-123",
        output_claim="claim-1",
        run_id=1,
        job_id=2,
        run_token="run-token-abc",
        api_base_url="https://api.example.com",
        secret_names=["api-token", "db-secret"],
        input_volume=None,
    )

    container = manifest["spec"]["template"]["spec"]["containers"][0]
    assert container["envFrom"] == [
        {"secretRef": {"name": "api-token"}},
        {"secretRef": {"name": "db-secret"}},
    ]


def test_render_job_manifest_skips_gpu_limits_when_empty():
    manifest = job_service._render_job_manifest(
        image="repo/image:tag",
        gpu="",
        job_name="job-789",
        output_claim="claim-2",
        run_id=10,
        job_id=20,
        run_token="tok",
        api_base_url="https://api.example.com",
        input_volume=None,
    )

    container = manifest["spec"]["template"]["spec"]["containers"][0]
    assert "resources" not in container


def test_render_job_manifest_mounts_input_volume(db_session):
    input_volume = job_service.create_volume(db_session, storage=1, is_input=True)

    manifest = job_service._render_job_manifest(
        image="repo/image:tag",
        gpu=GPUProfile.g1_10,
        job_name="job-with-input",
        output_claim="claim-out",
        input_volume=input_volume,
        run_id=99,
        job_id=100,
        run_token="run-token",
        api_base_url="https://api.example.com",
    )

    pod_spec = manifest["spec"]["template"]["spec"]
    main = pod_spec["containers"][0]
    input_claim = {
        "name": "input",
        "persistentVolumeClaim": {"claimName": input_volume.pvc_name},
    }

    assert {"name": "input", "mountPath": "/opt/input"} in main["volumeMounts"]
    assert input_claim in pod_spec["volumes"]  # type: ignore[index]

    init_containers = pod_spec.get("initContainers")
    assert init_containers
    downloader = init_containers[0]
    assert {"name": "input", "mountPath": "/opt/input"} in downloader["volumeMounts"]
    env_names = {env["name"] for env in downloader["env"]}
    assert {"RUN_TOKEN", "INPUT_LIST_ENDPOINT", "PRESIGN_ENDPOINT"} <= env_names


def test_render_registry_secret_encodes_docker_config():
    token = base64.b64encode(b"AWS:test-password").decode("utf-8")

    manifest = job_service._render_registry_secret(
        name="test-secret",
        registry="https://registry.example.com/",
        token=token,
    )

    assert manifest["metadata"]["name"] == "test-secret"
    assert manifest["type"] == "kubernetes.io/dockerconfigjson"
    docker_config_raw = base64.b64decode(manifest["data"][".dockerconfigjson"]).decode(
        "utf-8"
    )
    docker_config = json.loads(docker_config_raw)
    assert "https://registry.example.com" in docker_config["auths"]
    entry = docker_config["auths"]["https://registry.example.com"]
    assert entry["username"] == "AWS"
    assert entry["password"] == "test-password"
    assert entry["auth"] == token


def test_list_available_images_returns_sorted_entries(monkeypatch):
    monkeypatch.setattr(
        job_service.settings,
        "ecr_url",
        "https://registry.local/jobs",
        raising=False,
    )

    captured: dict[str, object] = {}

    class FakePaginator:
        def paginate(self, **kwargs):
            captured["kwargs"] = kwargs
            return iter(
                [
                    {
                        "imageDetails": [
                            {
                                "imageTags": ["latest", "v1"],
                                "imageDigest": "sha256:abc",
                                "imagePushedAt": datetime(2024, 1, 2, tzinfo=UTC),
                            },
                            {
                                "imageTags": ["v0"],
                                "imageDigest": "sha256:def",
                                "imagePushedAt": datetime(2023, 12, 30, tzinfo=UTC),
                            },
                        ]
                    }
                ]
            )

    class FakeECR:
        def __init__(self):
            self._paginator = FakePaginator()

        def get_paginator(self, name):
            assert name == "describe_images"
            return self._paginator

    images = job_service.list_available_images(FakeECR())

    assert captured["kwargs"]["repositoryName"] == "jobs"
    assert captured["kwargs"]["filter"] == {"tagStatus": "TAGGED"}
    assert [img.tag for img in images] == ["v1", "latest", "v0"]
    assert images[0].image == "https://registry.local/jobs:v1"
    assert images[0].digest == "sha256:abc"


def test_create_volume_persists_volume(db_session):
    volume = job_service.create_volume(db_session, storage=8, is_input=True)

    assert volume.id is not None
    assert volume.size == 8
    assert volume.is_input is True


def test_job_run_requires_unique_k8s_job_name(db_session, test_user):
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g1_10, storage=2)
    job = job_service.create_job(db_session, payload, test_user.id)
    vol_one = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    vol_two = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )

    run_one = job_service.create_job_run(db_session, job, vol_one)
    run_two = job_service.create_job_run(db_session, job, vol_two)

    run_one.k8s_job_name = "duplicate-job-name"
    run_two.k8s_job_name = "duplicate-job-name"
    run_one.k8s_pod_name = "unique-pod-1"
    run_two.k8s_pod_name = "unique-pod-2"

    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_job_run_requires_unique_k8s_pod_name(db_session, test_user):
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g1_10, storage=2)
    job = job_service.create_job(db_session, payload, test_user.id)
    vol_one = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    vol_two = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )

    run_one = job_service.create_job_run(db_session, job, vol_one)
    run_two = job_service.create_job_run(db_session, job, vol_two)

    run_one.k8s_pod_name = "duplicate-pod-name"
    run_two.k8s_pod_name = "duplicate-pod-name"

    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_create_job_populates_fields(db_session, test_user):
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g2_20, storage=6)

    job = job_service.create_job(db_session, payload, test_user.id)

    assert job.id is not None
    assert job.image == payload.image
    assert job.gpu_profile == payload.gpu
    assert job.created_by_id == test_user.id
    db_session.expire_all()
    stored = db_session.get(job_service.Job, job.id)
    assert stored is not None


def test_create_job_run_links_job_and_volume(db_session, test_user):
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g3_40, storage=2)
    job = job_service.create_job(db_session, payload, test_user.id)
    volume = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )

    assert job.priority == JobPriority.medium

    run = job_service.create_job_run(db_session, job, volume)

    assert run.job_id == job.id
    assert run.k8s_job_name
    assert run.output_volume_id == volume.id
    assert run.k8s_pod_name is None
    assert run.status == RunStatus.pending


def test_job_create_normalizes_secret_names():
    payload = JobCreate(
        image="repo/image:tag",
        gpu=GPUProfile.g1_10,
        secret_names=["  api-secret  "],
    )

    assert payload.secret_names == ["api-secret"]


def test_job_create_rejects_duplicate_secret_names():
    with pytest.raises(ValidationError):
        JobCreate(
            image="repo/image:tag",
            gpu=GPUProfile.g1_10,
            secret_names=["dup-secret", "dup-secret"],
        )


def test_job_create_normalizes_priority_label():
    payload = JobCreate(
        image="repo/image:tag",
        gpu=GPUProfile.g1_10,
        priority="EXTRA_HIGH",
    )

    assert payload.priority == JobPriority.extra_high


def test_job_create_rejects_unknown_priority():
    with pytest.raises(ValidationError):
        JobCreate(image="repo/image:tag", gpu=GPUProfile.g1_10, priority="urgent")


def test_create_and_run_job_commits_job_run(monkeypatch, db_session, test_user):
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g4_40, storage=5)
    fake_core = object()
    fake_batch = object()
    raw_credentials = "AWS:super-secret-token"
    encoded_token = base64.b64encode(raw_credentials.encode("utf-8")).decode("utf-8")

    fake_ecr = SimpleNamespace(
        get_authorization_token=lambda: {
            "authorizationData": [{"authorizationToken": encoded_token}]
        }
    )

    captured: dict[str, tuple[object, dict]] = {}

    def fake_apply_pvc(core, manifest):
        captured["pvc"] = (core, manifest)

    def fake_apply_job(batch, manifest):
        captured["job"] = (batch, manifest)
        return SimpleNamespace(
            metadata=SimpleNamespace(name=manifest["metadata"]["name"], uid="job-uid")
        )

    def fake_apply_secret(core, manifest):
        captured["secret"] = (core, manifest)

    def fake_set_secret_owner(core, *, secret_name, job_manifest, job_resource):
        captured["secret_owner"] = (
            core,
            secret_name,
            job_manifest,
            job_resource,
        )

    pvc_owners: list[tuple[object, str, dict[str, object], object]] = []

    def fake_set_pvc_owner(core, pvc_name, job_manifest, job_resource):
        pvc_owners.append((core, pvc_name, job_manifest, job_resource))

    pod = SimpleNamespace(metadata=SimpleNamespace(name="pod-xyz"))

    monkeypatch.setattr(job_service, "apply_pvc", fake_apply_pvc)
    monkeypatch.setattr(job_service, "apply_job", fake_apply_job)
    monkeypatch.setattr(job_service, "apply_registry_secret", fake_apply_secret)
    monkeypatch.setattr(job_service, "set_registry_secret_owner", fake_set_secret_owner)
    monkeypatch.setattr(job_service, "set_pvc_owner", fake_set_pvc_owner)
    monkeypatch.setattr(job_service, "wait_for_first_pod_of_job", lambda *a, **k: pod)

    job_run = job_service.create_and_run_job(
        fake_core, fake_batch, fake_ecr, db_session, payload, test_user
    )

    assert job_run.status == RunStatus.pending
    assert job_run.k8s_pod_name == "pod-xyz"
    assert job_run.k8s_job_name
    assert captured["pvc"][0] is fake_core
    assert captured["job"][0] is fake_batch
    assert captured["secret"][0] is fake_core
    assert captured["pvc"][1]["metadata"]["name"] == job_run.output_volume.pvc_name
    assert captured["job"][1]["metadata"]["name"] == job_run.k8s_job_name
    assert captured["secret_owner"][0] is fake_core
    assert captured["secret_owner"][1] == f"{job_run.k8s_job_name}-registry"
    owner_job = captured["secret_owner"][3]
    assert owner_job.metadata.uid == "job-uid"
    assert pvc_owners == [
        (fake_core, job_run.output_volume.pvc_name, captured["job"][1], owner_job)
    ]
    secret_manifest = captured["secret"][1]
    assert secret_manifest["metadata"]["name"] == f"{job_run.k8s_job_name}-registry"
    docker_config_raw = base64.b64decode(
        secret_manifest["data"][".dockerconfigjson"]
    ).decode("utf-8")
    docker_config = json.loads(docker_config_raw)
    registry_key = job_service.settings.ecr_url.rstrip("/")
    assert registry_key in docker_config["auths"]
    registry_entry = docker_config["auths"][registry_key]
    assert registry_entry["auth"] == encoded_token
    assert registry_entry["username"] == "AWS"
    assert registry_entry["password"] == "super-secret-token"
    pod_spec = captured["job"][1]["spec"]["template"]["spec"]
    assert pod_spec["priorityClassName"] == "nos-priority-medium"
    assert pod_spec["imagePullSecrets"] == [
        {"name": f"{job_run.k8s_job_name}-registry"}
    ]

    db_session.expire_all()
    stored_run = db_session.get(JobRun, job_run.id)
    assert stored_run is not None
    assert stored_run.job_id == job_run.job_id
    assert stored_run.output_volume_id == job_run.output_volume_id


def test_create_and_run_job_persists_secret_names(monkeypatch, db_session, test_user):
    payload = JobCreate(
        image="repo/image:tag",
        gpu=GPUProfile.g1_10,
        storage=2,
        secret_names=["api-token", "db-secret"],
    )
    fake_core = object()
    fake_batch = object()
    encoded_token = base64.b64encode(b"AWS:token").decode("utf-8")
    fake_ecr = SimpleNamespace(
        get_authorization_token=lambda: {
            "authorizationData": [{"authorizationToken": encoded_token}]
        }
    )

    captured: dict[str, object] = {}

    monkeypatch.setattr(job_service, "apply_pvc", lambda *a, **k: None)
    monkeypatch.setattr(job_service, "apply_registry_secret", lambda *a, **k: None)
    monkeypatch.setattr(job_service, "set_registry_secret_owner", lambda *a, **k: None)
    monkeypatch.setattr(job_service, "set_pvc_owner", lambda *a, **k: None)

    def fake_apply_job(batch, manifest):
        captured["manifest"] = manifest
        return SimpleNamespace(
            metadata=SimpleNamespace(name=manifest["metadata"]["name"], uid="uid")
        )

    monkeypatch.setattr(job_service, "apply_job", fake_apply_job)
    monkeypatch.setattr(
        job_service,
        "wait_for_first_pod_of_job",
        lambda *a, **k: SimpleNamespace(metadata=SimpleNamespace(name="pod-secrets")),
    )

    job_run = job_service.create_and_run_job(
        fake_core, fake_batch, fake_ecr, db_session, payload, test_user
    )

    assert job_run.secret_names == ["api-token", "db-secret"]
    pod_spec = captured["manifest"]["spec"]["template"]["spec"]
    main = pod_spec["containers"][0]
    assert main["envFrom"] == [
        {"secretRef": {"name": "api-token"}},
        {"secretRef": {"name": "db-secret"}},
    ]


def test_create_and_run_job_mounts_input_volume(monkeypatch, db_session, test_user):
    input_volume = job_service.create_volume(db_session, storage=1, is_input=True)
    payload = JobCreate(
        image="repo/image:tag",
        gpu=GPUProfile.g2_20,
        storage=4,
        input_id=input_volume.id,
        priority=JobPriority.extra_high,
    )
    fake_core = object()
    fake_batch = object()
    encoded_token = base64.b64encode(b"AWS:token").decode("utf-8")
    fake_ecr = SimpleNamespace(
        get_authorization_token=lambda: {
            "authorizationData": [{"authorizationToken": encoded_token}]
        }
    )

    captured_pvcs: list[tuple[object, dict]] = []
    captured_job: dict[str, object] = {}
    pvc_owners: list[tuple[object, str]] = []

    def fake_apply_pvc(core, manifest):
        captured_pvcs.append((core, manifest))

    def fake_apply_job(batch, manifest):
        captured_job["value"] = (batch, manifest)
        return SimpleNamespace(
            metadata=SimpleNamespace(name=manifest["metadata"]["name"], uid="job-uid")
        )

    pod = SimpleNamespace(metadata=SimpleNamespace(name="pod-with-input"))

    monkeypatch.setattr(job_service, "apply_pvc", fake_apply_pvc)
    monkeypatch.setattr(job_service, "apply_job", fake_apply_job)
    monkeypatch.setattr(job_service, "apply_registry_secret", lambda *a, **k: None)
    monkeypatch.setattr(job_service, "set_registry_secret_owner", lambda *a, **k: None)
    monkeypatch.setattr(
        job_service,
        "set_pvc_owner",
        lambda core, pvc_name, job_manifest, job_resource: pvc_owners.append(
            (core, pvc_name)
        ),
    )
    monkeypatch.setattr(job_service, "wait_for_first_pod_of_job", lambda *a, **k: pod)

    job_run = job_service.create_and_run_job(
        fake_core, fake_batch, fake_ecr, db_session, payload, test_user
    )

    assert job_run.input_volume_id == input_volume.id
    pvc_names = {manifest["metadata"]["name"] for _, manifest in captured_pvcs}
    assert job_run.output_volume.pvc_name in pvc_names
    assert input_volume.pvc_name in pvc_names

    batch_obj, job_manifest = captured_job["value"]
    assert batch_obj is fake_batch
    pod_spec = job_manifest["spec"]["template"]["spec"]
    assert pod_spec["priorityClassName"] == "nos-priority-extra-high"
    input_claim = {
        "name": "input",
        "persistentVolumeClaim": {"claimName": input_volume.pvc_name},
    }
    assert input_claim in pod_spec["volumes"]  # type: ignore[index]
    assert pod_spec["initContainers"]
    assert pvc_owners == [
        (fake_core, job_run.output_volume.pvc_name),
        (fake_core, input_volume.pvc_name),
    ]


def test_rerun_job_uses_latest_run_defaults(monkeypatch, db_session, test_user):
    payload = JobCreate(
        image="repo/image:tag",
        gpu=GPUProfile.g2_20,
        storage=3,
        priority=JobPriority.high,
    )
    job = job_service.create_job(db_session, payload, test_user.id)
    initial_output = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    input_volume = job_service.create_volume(db_session, storage=1, is_input=True)
    initial_run = job_service.create_job_run(
        db_session,
        job,
        initial_output,
        input_pvc=input_volume,
        secret_names=["api-token", "db-secret"],
    )
    initial_run.k8s_pod_name = "pod-old"
    db_session.commit()

    fake_core = object()
    fake_batch = object()
    encoded_token = base64.b64encode(b"AWS:runtoken").decode("utf-8")
    fake_ecr = SimpleNamespace(
        get_authorization_token=lambda: {
            "authorizationData": [{"authorizationToken": encoded_token}]
        }
    )

    captured: dict[str, object] = {"pvcs": []}

    def fake_apply_pvc(core, manifest):
        captured["pvcs"].append((core, manifest))

    def fake_apply_job(batch, manifest):
        captured["manifest"] = manifest
        return SimpleNamespace(
            metadata=SimpleNamespace(name=manifest["metadata"]["name"], uid="uid-2")
        )

    monkeypatch.setattr(job_service, "apply_pvc", fake_apply_pvc)
    monkeypatch.setattr(job_service, "apply_job", fake_apply_job)
    monkeypatch.setattr(job_service, "apply_registry_secret", lambda *a, **k: None)
    monkeypatch.setattr(job_service, "set_registry_secret_owner", lambda *a, **k: None)
    monkeypatch.setattr(
        job_service,
        "set_pvc_owner",
        lambda *a, pvc_name, **k: captured.setdefault("owners", []).append(pvc_name),
    )
    monkeypatch.setattr(
        job_service,
        "wait_for_first_pod_of_job",
        lambda *a, **k: SimpleNamespace(metadata=SimpleNamespace(name="pod-rerun")),
    )

    new_run = job_service.rerun_job(fake_core, fake_batch, fake_ecr, db_session, job.id)

    assert new_run.job_id == job.id
    assert new_run.id != initial_run.id
    assert new_run.output_volume.size == initial_output.size
    assert new_run.input_volume_id == input_volume.id
    assert new_run.secret_names == ["api-token", "db-secret"]
    pod_spec = captured["manifest"]["spec"]["template"]["spec"]
    main = pod_spec["containers"][0]
    assert pod_spec["priorityClassName"] == "nos-priority-high"
    assert main["envFrom"] == [
        {"secretRef": {"name": "api-token"}},
        {"secretRef": {"name": "db-secret"}},
    ]
    pvc_names = {manifest["metadata"]["name"] for _, manifest in captured["pvcs"]}
    assert new_run.output_volume.pvc_name in pvc_names
    assert input_volume.pvc_name in pvc_names


def test_rerun_job_requires_previous_run(db_session, test_user):
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g7_79, storage=2)
    job = job_service.create_job(db_session, payload, test_user.id)

    with pytest.raises(job_service.HTTPException) as exc_info:
        job_service.rerun_job(object(), object(), object(), db_session, job.id)

    assert exc_info.value.status_code == 400
    assert "no previous runs" in exc_info.value.detail


def test_create_and_run_job_raises_when_no_pod(monkeypatch, db_session, test_user):
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g7_79, storage=3)
    fake_core = object()
    fake_batch = object()
    encoded_token = base64.b64encode(b"AWS:noop-token").decode("utf-8")
    fake_ecr = SimpleNamespace(
        get_authorization_token=lambda: {
            "authorizationData": [{"authorizationToken": encoded_token}]
        }
    )

    monkeypatch.setattr(job_service, "apply_pvc", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        job_service,
        "apply_job",
        lambda *args, **kwargs: SimpleNamespace(
            metadata=SimpleNamespace(name="failed-job", uid="uid-123")
        ),
    )
    monkeypatch.setattr(
        job_service, "apply_registry_secret", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        job_service, "set_registry_secret_owner", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(job_service, "set_pvc_owner", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        job_service, "wait_for_first_pod_of_job", lambda *args, **kwargs: None
    )

    with pytest.raises(job_service.HTTPException) as exc_info:
        job_service.create_and_run_job(
            fake_core, fake_batch, fake_ecr, db_session, payload, test_user
        )

    assert exc_info.value.status_code == 400
    assert "Could not create pod" in exc_info.value.detail
    assert db_session.query(JobRun).count() == 0
