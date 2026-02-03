from logging import getLogger

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from kubernetes import client
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_admin
from app.core.aws import get_ddb_cluster_cache_table, put_k8s_cluster_creds_to_secret
from app.core.config import get_settings
from app.core.database import get_db
from app.core.k8s import get_core, swap_kubernetes_clients
from app.models.users import User
from app.schemas.cluster import (
    ClusterConfigUpdateIn,
    ClusterInsightsIn,
    GPUResources,
    Pod,
)
from app.services import cluster_service, job_service

router = APIRouter(prefix="/cluster", tags=["cluster"])
settings = get_settings()

logger = getLogger(__name__)

# async def dump_body(request: Request):
#     body = await request.body()
#     logger.error(f"err {body}")
#     return body


@router.post("/insights", status_code=status.HTTP_204_NO_CONTENT)
def submit_insights(
    payload: ClusterInsightsIn,
    #    _raw=Depends(dump_body),
    ddb_table=Depends(get_ddb_cluster_cache_table),
    db: Session = Depends(get_db),
    _=Depends(require_admin),
) -> None:
    cluster_service.save_cluster_insights(
        ddb_table=ddb_table,
        payload=payload,
        db=db,
    )


@router.get("/resources", response_model=list[GPUResources])
def get_resources(
    ddb_table=Depends(get_ddb_cluster_cache_table),
    _: User = Depends(get_current_user),
) -> list[GPUResources]:
    snapshot = cluster_service.get_insights(ddb_table)
    return snapshot.gpus


@router.get("/pods", response_model=list[Pod])
def get_pods(
    ddb_table=Depends(get_ddb_cluster_cache_table),
    _: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[Pod]:
    snapshot = cluster_service.get_insights(ddb_table)
    enriched: list[Pod] = []
    for pod in snapshot.pods:
        priority = None
        try:
            job_run = job_service.get_job_run_by_pod_name(db, pod.name)
        except HTTPException as exc:
            if exc.status_code != 404:
                raise
        else:
            if job_run.job is not None:
                priority = job_run.job.priority
        enriched.append(pod.model_copy(update={"priority": priority}))
    return enriched


@router.get("/pods/{pod_name}/logs")
def stream_pod_logs(
    pod_name: str,
    container: str | None = Query(default=None, description="Container within the pod"),
    follow: bool = Query(default=True, description="Stream logs as new entries arrive"),
    tail_lines: int | None = Query(
        default=200,
        ge=1,
        description="Number of lines to include from the end of the logs",
    ),
    timestamps: bool = Query(
        default=True, description="Include timestamps in the log output"
    ),
    core: client.CoreV1Api = Depends(get_core),
    _: User = Depends(get_current_user),
):
    log_iter = cluster_service.stream_pod_logs(
        core,
        pod_name=pod_name,
        namespace=settings.namespace,
        container=container,
        follow=follow,
        tail_lines=tail_lines,
        timestamps=timestamps,
    )
    return StreamingResponse(log_iter, media_type="text/plain")


@router.put("/cluster-config")
async def update_cluster_config(
    payload: ClusterConfigUpdateIn,
    request: Request,
    _: str = Depends(require_admin),
):
    sm_client = request.app.state.secrets_manager_client

    put_k8s_cluster_creds_to_secret(
        sm_client,
        cluster_url=payload.cluster_url,
        cluster_token=payload.cluster_token,
    )

    await swap_kubernetes_clients(
        request.app,
        cluster_url=payload.cluster_url,
        cluster_token=payload.cluster_token,
    )

    return {"ok": True}
