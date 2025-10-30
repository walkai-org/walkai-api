from collections.abc import Sequence
from typing import Final

from fastapi import HTTPException
from redis import Redis
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.jobs import JobRun
from app.schemas.cluster import ClusterInsightsIn, Pod, PodStatus
from app.schemas.jobs import RunStatus

INSIGHTS_KEY: Final = "cluster:insights"

# Map Pod status reported by Kubernetes into our internal JobRun status.
_POD_STATUS_TO_RUN: Final = {
    PodStatus.pending: RunStatus.pending,
    PodStatus.container_creating: RunStatus.scheduled,
    PodStatus.running: RunStatus.active,
    PodStatus.completed: RunStatus.succeeded,
    PodStatus.succeeded: RunStatus.succeeded,
    PodStatus.error: RunStatus.failed,
    PodStatus.crash: RunStatus.failed,
}


def save_cluster_insights(
    redis_client: Redis, payload: ClusterInsightsIn, db: Session
) -> None:
    """
    Persist the latest cluster snapshot so other endpoints can read it quickly.
    """
    _sync_job_runs(db, payload.pods)
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


def _sync_job_runs(db: Session, pods: Sequence[Pod]) -> None:
    """
    Update JobRun records based on the current snapshot of Pods.
    """
    pod_lookup = {pod.name: pod for pod in pods}
    if not pod_lookup:
        return

    stmt = select(JobRun).where(JobRun.k8s_pod_name.in_(tuple(pod_lookup.keys())))
    job_runs = db.scalars(stmt).all()

    updated = False
    for job_run in job_runs:
        if job_run.k8s_pod_name is None:
            continue

        pod = pod_lookup.get(job_run.k8s_pod_name)
        if pod is None:
            continue

        pod_status = _POD_STATUS_TO_RUN.get(pod.status)
        if pod_status and job_run.status != pod_status:
            job_run.status = pod_status
            updated = True

        if job_run.started_at != pod.start_time:
            job_run.started_at = pod.start_time
            updated = True

        if job_run.finished_at != pod.finish_time:
            job_run.finished_at = pod.finish_time
            updated = True

    if updated:
        db.commit()
