from fastapi import APIRouter, Depends
from kubernetes import client
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.config import get_settings
from app.core.database import get_db
from app.core.k8s import get_batch, get_core
from app.models.users import User
from app.schemas.jobs import JobCreate, JobRunOut, PodList
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


@router.get("/pods", response_model=list[PodList])
def list_pods(core=Depends(get_core), settings=Depends(get_settings)):
    ret = core.list_namespaced_pod(namespace=settings.namespace, watch=False)
    res = []
    for i in ret.items:
        res.append(
            PodList(
                name=i.metadata.name,
                namespace=i.metadata.namespace,
                status=i.status.phase,
            )
        )
    return res
