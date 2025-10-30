from fastapi import APIRouter, Depends, status
from redis import Redis
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_admin
from app.core.database import get_db
from app.core.redis import get_redis
from app.models.users import User
from app.schemas.cluster import ClusterInsightsIn, GPUResources, Pod
from app.services import cluster_service

router = APIRouter(prefix="/cluster", tags=["cluster"])


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
