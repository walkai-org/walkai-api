from typing import Literal

from botocore.client import BaseClient
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from fastapi.responses import StreamingResponse
from kubernetes import client
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.aws import (
    get_ecr_client,
    get_s3_client,
    list_s3_objects_with_prefix,
    presign_url,
)
from app.core.database import get_db
from app.core.k8s import get_batch, get_core
from app.models.jobs import Job, JobRun
from app.models.users import User
from app.schemas.jobs import (
    JobCreate,
    JobDetailOut,
    JobImage,
    JobOut,
    JobRunByPodOut,
    JobRunDetail,
    JobRunOut,
)
from app.schemas.schedules import ScheduleCreate, ScheduleOut
from app.services import job_service, schedule_service

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("/", response_model=JobRunOut)
def submit_job(
    payload: JobCreate,
    db: Session = Depends(get_db),
    core: client.CoreV1Api = Depends(get_core),
    batch: client.BatchV1Api = Depends(get_batch),
    ecr_client: BaseClient = Depends(get_ecr_client),
    user: User = Depends(get_current_user),
):
    job_run = job_service.create_and_run_job(core, batch, ecr_client, db, payload, user)
    return JobRunOut(job_id=job_run.job_id, pod=job_run.k8s_pod_name)


@router.post("/{job_id}/runs", response_model=JobRunOut)
def rerun_job(
    job_id: int,
    db: Session = Depends(get_db),
    core: client.CoreV1Api = Depends(get_core),
    batch: client.BatchV1Api = Depends(get_batch),
    ecr_client: BaseClient = Depends(get_ecr_client),
    _: User = Depends(get_current_user),
):
    job_run = job_service.rerun_job(core, batch, ecr_client, db, job_id)
    return JobRunOut(job_id=job_run.job_id, pod=job_run.k8s_pod_name)


@router.post(
    "/{job_id}/schedules",
    response_model=ScheduleOut,
    status_code=201,
)
def create_job_schedule(
    job_id: int,
    payload: ScheduleCreate,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return schedule_service.create_schedule(db, job_id, payload)


@router.get("/{job_id}/schedules", response_model=list[ScheduleOut])
def list_job_schedules(
    job_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return schedule_service.list_schedules(db, job_id)


@router.get("/{job_id}/schedules/{schedule_id}", response_model=ScheduleOut)
def get_job_schedule(
    job_id: int,
    schedule_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return schedule_service.get_schedule(db, job_id, schedule_id)


@router.delete(
    "/{job_id}/schedules/{schedule_id}",
    status_code=204,
    response_class=Response,
)
def delete_job_schedule(
    job_id: int,
    schedule_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    schedule_service.delete_schedule(db, job_id, schedule_id)
    return Response(status_code=204)


@router.get("/", response_model=list[JobOut])
def list_jobs(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return job_service.list_jobs(db)


@router.get("/images", response_model=list[JobImage])
def list_job_images(
    ecr_client: BaseClient = Depends(get_ecr_client),
    _: User = Depends(get_current_user),
):
    return job_service.list_available_images(ecr_client)


@router.get("/runs/by-pod/{pod_name}", response_model=JobRunByPodOut)
def get_job_run_by_pod_name(
    pod_name: str,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return job_service.get_job_run_by_pod_name(db, pod_name)


@router.get("/{job_id}", response_model=JobDetailOut)
def get_job_detail(
    job_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return job_service.get_job(db, job_id)


@router.get("/{job_id}/runs/{run_id}", response_model=JobRunDetail)
def get_job_run_detail(
    job_id: int,
    run_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return job_service.get_job_run(db, job_id, run_id)


@router.get("/{job_id}/runs/{run_id}/presign")
def presign_object(
    job_id: int,
    run_id: int,
    path: str = Query(..., description="Ruta relativa dentro de /opt/output"),
    method: Literal["GET", "PUT"] = Query("PUT", description="Método HTTP a presignar"),
    direction: Literal["input", "output"] = Query(
        "output", description="Volumen sobre el que se hace el presign"
    ),
    run_token: str = Header(..., alias="X-Run-Token"),
    db: Session = Depends(get_db),
    s3_client: BaseClient = Depends(get_s3_client),
):
    run = db.query(JobRun).filter(JobRun.id == run_id, JobRun.job_id == job_id).first()
    if not run or run.run_token != run_token:
        raise HTTPException(status_code=401, detail="Invalid run token")

    job = db.query(Job).filter(Job.id == job_id).first()
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    if direction == "output":
        volume = run.output_volume
        if volume is None:
            raise HTTPException(status_code=400, detail="Run has no output volume")
        if volume.key_prefix is None:
            volume.key_prefix = (
                f"users/{job.created_by_id}/jobs/{job_id}/{run_id}/outputs"
            )
            db.commit()
        prefix = volume.key_prefix
        if method != "PUT":
            raise HTTPException(
                status_code=400,
                detail="Outputs only support method=PUT for presign",
            )
    elif direction == "input":
        volume = run.input_volume
        if volume is None:
            raise HTTPException(status_code=400, detail="Run has no input volume")

        if volume.key_prefix is None:
            raise HTTPException(
                status_code=500,
                detail="Input volume without key_prefix configured",
            )
        prefix = volume.key_prefix
        if method != "GET":
            raise HTTPException(
                status_code=400,
                detail="Inputs only support method=GET for presign",
            )
    else:
        raise HTTPException(status_code=400, detail="Invalid direction")

    key = f"{prefix.rstrip('/')}/{path.lstrip('/')}"

    url = presign_url(s3_client, key=key, method=method)
    return {"url": url}


@router.get("/{job_id}/runs/{run_id}/inputs")
def list_input_objects(
    job_id: int,
    run_id: int,
    run_token: str = Header(..., alias="X-Run-Token"),
    db: Session = Depends(get_db),
    s3_client: BaseClient = Depends(get_s3_client),
):
    run = db.query(JobRun).filter(JobRun.id == run_id, JobRun.job_id == job_id).first()
    if not run or run.run_token != run_token:
        raise HTTPException(status_code=401, detail="Invalid run token")

    volume = run.input_volume
    if volume is None:
        raise HTTPException(status_code=400, detail="Run has no input volume")

    if not volume.key_prefix:
        raise HTTPException(
            status_code=500,
            detail="Input volume has no key_prefix configured",
        )

    base_prefix = volume.key_prefix.rstrip("/")

    prefix = base_prefix + "/"
    keys = list_s3_objects_with_prefix(s3_client, prefix=prefix)

    files: list[str] = []

    for key in keys:
        rel = key[len(prefix) :] if key.startswith(prefix) else key
        if rel:
            files.append(rel)

    return {"files": files}


@router.get("/{job_id}/runs/{run_id}/logs")
def get_job_run_logs(
    job_id: int,
    run_id: int,
    db: Session = Depends(get_db),
    s3_client: BaseClient = Depends(get_s3_client),
    _: User = Depends(get_current_user),
):
    job_run = job_service.get_job_run(db, job_id, run_id)
    job = job_run.job
    if job is None:
        raise HTTPException(status_code=502, detail="Job data missing for run")

    log_stream = job_service.stream_job_run_logs(s3_client, job_run)
    return StreamingResponse(log_stream, media_type="text/plain; charset=utf-8")
