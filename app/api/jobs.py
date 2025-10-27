from botocore.client import BaseClient
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from kubernetes import client
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.aws import get_s3_client, presign_put_url
from app.core.database import get_db
from app.core.k8s import get_batch, get_core
from app.models.jobs import Job, JobRun
from app.models.users import User
from app.schemas.jobs import JobCreate, JobDetailOut, JobOut, JobRunOut
from app.services import job_service

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("/", response_model=JobRunOut)
def submit_job(
    payload: JobCreate,
    db: Session = Depends(get_db),
    core: client.CoreV1Api = Depends(get_core),
    batch: client.BatchV1Api = Depends(get_batch),
    user: User = Depends(get_current_user),
):
    job_run = job_service.create_and_run_job(core, batch, db, payload, user)
    return JobRunOut(job_id=job_run.job_id, pod=job_run.k8s_pod_name)


@router.get("/", response_model=list[JobOut])
def list_jobs(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return job_service.list_jobs(db)


@router.get("/{job_id}", response_model=JobDetailOut)
def get_job_detail(
    job_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return job_service.get_job(db, job_id)


@router.get("/{job_id}/runs/{run_id}/presign")
def presign_output_object(
    job_id: int,
    run_id: int,
    path: str = Query(..., description="Ruta relativa dentro de /opt/output"),
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

    s3_prefix = f"users/{job.created_by_id}/jobs/{job_id}/{run_id}/outputs"

    key = f"{s3_prefix.rstrip('/')}/{path.lstrip('/')}"
    url = presign_put_url(s3_client, key)
    return {"url": url}
