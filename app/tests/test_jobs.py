from types import SimpleNamespace

import pytest

from app.core.config import get_settings
from app.core.k8s import get_batch, get_core
from app.main import app
from app.models.jobs import JobRun, RunStatus
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
    run = job_service.create_job_run(db_session, job, output_volume, "pod-123")
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
    assert job_item["k8s_job_name"] == job.k8s_job_name
    assert job_item["submitted_at"]
    assert job_item["runs"] == [
        {
            "id": run.id,
            "status": run.status.value,
            "k8s_pod_name": run.k8s_pod_name,
            "started_at": run.started_at,
            "finished_at": run.finished_at,
        }
    ]


def test_get_job_detail_returns_runs_and_volumes(auth_client, db_session):
    client, user = auth_client
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g3_40, storage=8)
    job = job_service.create_job(db_session, payload, user.id)
    output_volume = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    run = job_service.create_job_run(db_session, job, output_volume, "pod-xyz")
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


def test_list_pods_returns_pod_statuses(client):
    fake_core_calls: dict[str, object] = {}
    fake_core = SimpleNamespace(
        list_namespaced_pod=lambda namespace, watch: _fake_list_pods(
            fake_core_calls, namespace, watch
        )
    )
    fake_settings = SimpleNamespace(namespace="testing")

    app.dependency_overrides[get_core] = lambda: fake_core
    app.dependency_overrides[get_settings] = lambda: fake_settings

    try:
        response = client.get("/jobs/pods")
    finally:
        app.dependency_overrides.pop(get_core, None)
        app.dependency_overrides.pop(get_settings, None)

    assert response.status_code == 200
    assert response.json() == [
        {"name": "pod-a", "namespace": "testing", "status": "Running"},
        {"name": "pod-b", "namespace": "testing", "status": "Pending"},
    ]
    assert fake_core_calls["namespace"] == "testing"
    assert fake_core_calls["watch"] is False


def _fake_list_pods(store: dict[str, object], namespace: str, watch: bool):
    store["namespace"] = namespace
    store["watch"] = watch
    return SimpleNamespace(
        items=[
            SimpleNamespace(
                metadata=SimpleNamespace(name="pod-a", namespace=namespace),
                status=SimpleNamespace(phase="Running"),
            ),
            SimpleNamespace(
                metadata=SimpleNamespace(name="pod-b", namespace=namespace),
                status=SimpleNamespace(phase="Pending"),
            ),
        ]
    )


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
        image="repo/image:tag", gpu="", job_name="job-789", output_claim="claim-2"
    )

    container = manifest["spec"]["template"]["spec"]["containers"][0]
    assert "resources" not in container


def test_create_volume_persists_volume(db_session):
    volume = job_service.create_volume(db_session, storage=8, is_input=True)

    assert volume.id is not None
    assert volume.size == 8
    assert volume.is_input is True
    assert volume.state == job_service.VolumeState.pvc
    assert volume.pvc_name
    db_session.expire_all()
    stored = db_session.get(job_service.Volume, volume.id)
    assert stored is not None


def test_create_job_populates_fields(db_session, test_user):
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g2_20, storage=6)

    job = job_service.create_job(db_session, payload, test_user.id)

    assert job.id is not None
    assert job.image == payload.image
    assert job.gpu_profile == payload.gpu
    assert job.created_by_id == test_user.id
    assert job.k8s_job_name
    db_session.expire_all()
    stored = db_session.get(job_service.Job, job.id)
    assert stored is not None


def test_create_job_run_links_job_and_volume(db_session, test_user):
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g3_40, storage=2)
    job = job_service.create_job(db_session, payload, test_user.id)
    volume = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )

    run = job_service.create_job_run(db_session, job, volume, "pod-run-1")

    assert run.job_id == job.id
    assert run.output_volume_id == volume.id
    assert run.k8s_pod_name == "pod-run-1"
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
    assert captured["pvc"][0] is fake_core
    assert captured["job"][0] is fake_batch
    assert captured["pvc"][1]["metadata"]["name"] == job_run.output_volume.pvc_name
    assert captured["job"][1]["metadata"]["name"] == job_run.job.k8s_job_name

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
