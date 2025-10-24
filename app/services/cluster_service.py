from typing import Final

from fastapi import HTTPException
from redis import Redis

from app.schemas.cluster import ClusterInsightsIn

INSIGHTS_KEY: Final = "cluster:insights"


def save_cluster_insights(redis_client: Redis, payload: ClusterInsightsIn) -> None:
    """
    Persist the latest cluster snapshot so other endpoints can read it quickly.
    """
    redis_client.set(INSIGHTS_KEY, payload.model_dump_json())


def get_insights(redis_client: Redis) -> ClusterInsightsIn:
    snapshot = load_cluster_insights(redis_client)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Cluster insights not available")
    return snapshot


def load_cluster_insights(redis_client: Redis) -> ClusterInsightsIn | None:
    raw_snapshot = redis_client.get(INSIGHTS_KEY)
    if not raw_snapshot:
        return None
    return ClusterInsightsIn.model_validate_json(raw_snapshot)
