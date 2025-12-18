from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from app.schemas.jobs import GPUProfile, JobCreate, RunStatus
from app.schemas.schedules import ScheduleCreate, ScheduleKind
from app.services import job_service, schedule_service
from app.workers.scheduler import run_scheduler_tick


def test_create_schedule_requires_existing_run(auth_client, db_session, test_user):
    client, user = auth_client
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g1_10, storage=2)
    job = job_service.create_job(db_session, payload, user.id)
    db_session.commit()

    run_at = datetime.now(UTC).isoformat()
    response = client.post(
        f"/jobs/{job.id}/schedules",
        json={"kind": ScheduleKind.once.value, "run_at": run_at},
    )

    assert response.status_code == 400
    assert "at least one run" in response.json()["detail"]


def test_create_and_list_schedules(auth_client, db_session, test_user):
    client, user = auth_client
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g1_10, storage=2)
    job = job_service.create_job(db_session, payload, user.id)
    output_volume = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    job_run = job_service.create_job_run(db_session, job, output_volume)
    job_run.k8s_pod_name = "pod-schedule"
    db_session.commit()

    run_at = datetime.now(UTC) + timedelta(minutes=5)
    create_resp = client.post(
        f"/jobs/{job.id}/schedules",
        json={"kind": ScheduleKind.once.value, "run_at": run_at.isoformat()},
    )

    assert create_resp.status_code == 201
    schedule = create_resp.json()
    assert schedule["job_id"] == job.id
    scheduled_dt = datetime.fromisoformat(schedule["next_run_at"])
    assert abs((scheduled_dt - run_at).total_seconds()) < 2

    list_resp = client.get(f"/jobs/{job.id}/schedules")
    assert list_resp.status_code == 200
    data = list_resp.json()
    assert len(data) == 1
    assert data[0]["id"] == schedule["id"]


def test_scheduler_tick_triggers_due_runs(db_session, test_user, monkeypatch):
    payload = JobCreate(image="repo/image:tag", gpu=GPUProfile.g1_10, storage=2)
    job = job_service.create_job(db_session, payload, test_user.id)
    output_volume = job_service.create_volume(
        db_session, storage=payload.storage, is_input=False
    )
    job_run = job_service.create_job_run(db_session, job, output_volume)
    job_run.k8s_pod_name = "pod-initial"
    job_run.status = RunStatus.succeeded
    db_session.commit()

    past_time = datetime.now(UTC) - timedelta(minutes=1)
    schedule = schedule_service.create_schedule(
        db_session,
        job.id,
        ScheduleCreate(kind=ScheduleKind.once, run_at=past_time),
    )

    calls: list[int] = []

    def fake_rerun(
        core, batch, ecr_client, db, job_id: int, run_user=None, is_scheduled=False
    ):  # noqa: ARG001
        calls.append((job_id, run_user, is_scheduled))
        return SimpleNamespace(job_id=job_id, k8s_pod_name="pod-scheduled")

    monkeypatch.setattr(job_service, "rerun_job", fake_rerun)

    triggered = run_scheduler_tick(
        core=object(),
        batch=object(),
        ecr_client=object(),
        session_factory=lambda: db_session,
        close_session=False,
        now=datetime.now(UTC),
    )

    assert triggered == 1
    assert calls == [(job.id, None, True)]
    db_session.refresh(schedule)
    assert schedule.last_run_at is not None
    assert schedule.next_run_at is None
