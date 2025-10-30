from fastapi import APIRouter, Depends, Query, status
from fastapi.responses import StreamingResponse
from kubernetes import client
from redis import Redis
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_admin
from app.core.config import get_settings
from app.core.database import get_db
from app.core.k8s import get_core
from app.core.redis import get_redis
from app.models.users import User
from app.schemas.cluster import ClusterInsightsIn, GPUResources, Pod
from app.services import cluster_service

router = APIRouter(prefix="/cluster", tags=["cluster"])
settings = get_settings()


@router.post("/insights", status_code=status.HTTP_204_NO_CONTENT)
def submit_insights(
    payload: ClusterInsightsIn,
    redis_client: Redis = Depends(get_redis),
    db: Session = Depends(get_db),
    _=Depends(require_admin),
) -> None:
    cluster_service.save_cluster_insights(
        redis_client=redis_client,
        payload=payload,
        db=db,
    )


@router.get("/resources", response_model=list[GPUResources])
def get_resources(
    redis_client: Redis = Depends(get_redis),
    _: User = Depends(get_current_user),
) -> list[GPUResources]:
    snapshot = cluster_service.get_insights(redis_client)
    return snapshot.gpus


@router.get("/pods", response_model=list[Pod])
def get_pods(
    redis_client: Redis = Depends(get_redis),
    _: User = Depends(get_current_user),
) -> list[Pod]:
    snapshot = cluster_service.get_insights(redis_client)
    return snapshot.pods


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
