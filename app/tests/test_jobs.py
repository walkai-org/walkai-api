from datetime import UTC, datetime, timedelta
from io import BytesIO
from types import SimpleNamespace

import boto3
import pytest
from botocore.response import StreamingBody
from botocore.stub import Stubber
from sqlalchemy.exc import IntegrityError

from app.core.aws import get_s3_client
from app.core.k8s import get_batch, get_core
from app.main import app
from app.models.jobs import JobRun, RunStatus
from app.models.users import User
from app.schemas.jobs import GPUProfile, JobCreate
from app.services import job_service


def test_submit_job_returns_job_run(auth_client, db_session, monkeypatch):
    client, user = auth_client
    fake_core = object()
    fake_batch = object()

    app.dependency_overrides[get_core] = lambda: fake_core
    app.dependency_overrides[get_batch] = lambda: fake_batch
    captured: dict[str, object] = {}

    def fake_create(core, batch, db, payload, current_user):
        captured["core"] = core
        captured["batch"] = batch
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
            },
        )
    finally:
        app.dependency_overrides.pop(get_core, None)
        app.dependency_overrides.pop(get_batch, None)

    assert response.status_code == 200
    assert response.json() == {"job_id": 42, "pod": "pod-123"}
    assert captured["core"] is fake_core
    assert captured["batch"] is fake_batch
    assert captured["db"] is db_session
    assert isinstance(captured["payload"], JobCreate)
    assert captured["payload"].image == "repo/image:tag"
    assert captured["payload"].gpu == GPUProfile.g1_10
    assert captured["payload"].storage == 4
    assert captured["user"].id == user.id


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


def test_get_job_detail_returns_runs_and_volumes(auth_client, db_session):
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
    run_data = data["runs"][0]
    assert run_data["id"] == run.id
    assert run_data["status"] == run.status.value
    assert run_data["k8s_job_name"] == run.k8s_job_name
    assert run_data["k8s_pod_name"] == run.k8s_pod_name
    assert run_data["output_volume"] == {
        "id": output_volume.id,
        "pvc_name": output_volume.pvc_name,
        "size": output_volume.size,
        "key_prefix": output_volume.key_prefix,
        "is_input": output_volume.is_input,
        "state": output_volume.state.value,
    }
    assert run_data["input_volume"] == {
        "id": input_volume.id,
        "pvc_name": input_volume.pvc_name,
        "size": input_volume.size,
        "key_prefix": input_volume.key_prefix,
        "is_input": input_volume.is_input,
        "state": input_volume.state.value,
    }


def test_get_job_detail_not_found(auth_client):
    client, _ = auth_client

    response = client.get("/jobs/9999")

    assert response.status_code == 404
    assert response.json()["detail"] == "Job 9999 not found"


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


def test_get_job_run_logs_forbidden_for_non_owner(auth_client, db_session):
    client, user = auth_client
    user.role = "member"
    db_session.commit()

    other_user = User(email="other@example.com", password_hash=None, role="admin")
    db_session.add(other_user)
    db_session.commit()
    db_session.refresh(other_user)

    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g2_20, storage=2)
    job = job_service.create_job(db_session, payload, other_user.id)
    volume = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    run = job_service.create_job_run(db_session, job, volume)
    run.k8s_pod_name = "pod-other"
    run.output_volume.key_prefix = (
        f"users/{other_user.id}/jobs/{job.id}/{run.id}/outputs"
    )
    db_session.commit()

    response = client.get(f"/jobs/{job.id}/runs/{run.id}/logs")

    assert response.status_code == 403
    assert response.json()["detail"] == "Forbidden"


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


def test_render_pvc_manifest_shapes_storage():
    manifest = job_service._render_persistent_volume_claim(
        name="vol-123", storage=5, read_only=False
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
    )

    container = manifest["spec"]["template"]["spec"]["containers"][0]
    assert "resources" not in container


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

    run = job_service.create_job_run(db_session, job, volume)

    assert run.job_id == job.id
    assert run.k8s_job_name
    assert run.output_volume_id == volume.id
    assert run.k8s_pod_name is None
    assert run.status == RunStatus.pending


def test_create_and_run_job_commits_job_run(monkeypatch, db_session, test_user):
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g4_40, storage=5)
    fake_core = object()
    fake_batch = object()

    captured: dict[str, tuple[object, dict]] = {}

    def fake_apply_pvc(core, manifest):
        captured["pvc"] = (core, manifest)

    def fake_apply_job(batch, manifest):
        captured["job"] = (batch, manifest)

    pod = SimpleNamespace(metadata=SimpleNamespace(name="pod-xyz"))

    monkeypatch.setattr(job_service, "apply_pvc", fake_apply_pvc)
    monkeypatch.setattr(job_service, "apply_job", fake_apply_job)
    monkeypatch.setattr(job_service, "wait_for_first_pod_of_job", lambda *a, **k: pod)

    job_run = job_service.create_and_run_job(
        fake_core, fake_batch, db_session, payload, test_user
    )

    assert job_run.status == RunStatus.pending
    assert job_run.k8s_pod_name == "pod-xyz"
    assert job_run.k8s_job_name
    assert captured["pvc"][0] is fake_core
    assert captured["job"][0] is fake_batch
    assert captured["pvc"][1]["metadata"]["name"] == job_run.output_volume.pvc_name
    assert captured["job"][1]["metadata"]["name"] == job_run.k8s_job_name

    db_session.expire_all()
    stored_run = db_session.get(JobRun, job_run.id)
    assert stored_run is not None
    assert stored_run.job_id == job_run.job_id
    assert stored_run.output_volume_id == job_run.output_volume_id


def test_create_and_run_job_raises_when_no_pod(monkeypatch, db_session, test_user):
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g7_79, storage=3)
    fake_core = object()
    fake_batch = object()

    monkeypatch.setattr(job_service, "apply_pvc", lambda *args, **kwargs: None)
    monkeypatch.setattr(job_service, "apply_job", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        job_service, "wait_for_first_pod_of_job", lambda *args, **kwargs: None
    )

    with pytest.raises(job_service.HTTPException) as exc_info:
        job_service.create_and_run_job(
            fake_core, fake_batch, db_session, payload, test_user
        )

    assert exc_info.value.status_code == 400
    assert "Could not create pod" in exc_info.value.detail
    assert db_session.query(JobRun).count() == 0
