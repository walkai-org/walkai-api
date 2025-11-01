import time
from collections.abc import Iterable, Sequence
from typing import Final

from fastapi import HTTPException
from kubernetes import client
from kubernetes.client import ApiException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.jobs import JobRun
from app.schemas.cluster import ClusterInsightsIn, Pod, PodStatus
from app.schemas.jobs import RunStatus

INSIGHTS_PK: Final = "cache#cluster:insights"

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


def save_cluster_insights(ddb_table, payload: ClusterInsightsIn, db: Session) -> None:
    """
    Persist the latest cluster snapshot so other endpoints can read it quickly.
    """
    _sync_job_runs(db, payload.pods)

    ddb_table.put_item(
        Item={
            "pk": INSIGHTS_PK,
            "data": payload.model_dump_json(),
            "updated_at": int(time.time()),
        }
    )


def get_insights(ddb_table) -> ClusterInsightsIn:
    snapshot = load_cluster_insights(ddb_table)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Cluster insights not available")
    return snapshot


def load_cluster_insights(ddb_table) -> ClusterInsightsIn | None:
    resp = ddb_table.get_item(
        Key={"pk": INSIGHTS_PK},
        ConsistentRead=True,
    )
    item = resp.get("Item")
    if not item:
        return None
    return ClusterInsightsIn.model_validate_json(item["data"])


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


def stream_pod_logs(
    core: client.CoreV1Api,
    *,
    pod_name: str,
    namespace: str,
    container: str | None,
    follow: bool,
    tail_lines: int | None,
    timestamps: bool,
    chunk_size: int = 1024,
) -> Iterable[str]:
    """Stream logs from a Kubernetes pod, decoding into UTF-8 text chunks."""

    try:
        response = core.read_namespaced_pod_log(
            name=pod_name,
            namespace=namespace,
            container=container,
            follow=follow,
            tail_lines=tail_lines,
            timestamps=timestamps,
            _preload_content=False,
        )
    except ApiException as exc:  # pragma: no cover - exercised via HTTP layer
        if exc.status == 404:
            raise HTTPException(status_code=404, detail=f"Pod {pod_name} not found")
        raise HTTPException(
            status_code=502,
            detail="Kubernetes API error while fetching pod logs",
        ) from exc
    except Exception as exc:  # pragma: no cover - guard against unexpected errors
        raise HTTPException(
            status_code=502,
            detail="Unexpected error while streaming pod logs",
        ) from exc

    def _iterator() -> Iterable[str]:
        try:
            for chunk in response.stream(amt=chunk_size):
                if not chunk:
                    continue
                yield chunk.decode("utf-8", errors="replace")
        finally:
            response.close()

    return _iterator()
