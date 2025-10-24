from fastapi import APIRouter, Depends, status
from redis import Redis

from app.api.deps import require_admin
from app.core.redis import get_redis
from app.schemas.cluster import ClusterInsightsIn, Pod
from app.services import cluster_service

router = APIRouter(prefix="/cluster", tags=["cluster"])


@router.post("/insights", status_code=status.HTTP_204_NO_CONTENT)
def submit_insights(
    payload: ClusterInsightsIn,
    redis_client: Redis = Depends(get_redis),
    _=Depends(require_admin),
) -> None:
    cluster_service.save_cluster_insights(redis_client, payload)


@router.get("/resources")
def get_resources():
    pass


@router.get("/pods", response_model=list[Pod])
def get_pods():
    pass
