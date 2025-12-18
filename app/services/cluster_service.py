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
from app.schemas.jobs import JobPriority, RunStatus
from app.services.quota_service import compute_billable_minutes, ensure_reset

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

        if job_run.started_at and job_run.finished_at:
            computed_minutes = compute_billable_minutes(
                job_run.started_at, job_run.finished_at
            )
            if job_run.billable_minutes != computed_minutes:
                delta = computed_minutes - job_run.billable_minutes
                job_run.billable_minutes = computed_minutes
                if (
                    delta > 0
                    and not job_run.is_scheduled
                    and job_run.user is not None
                    and job_run.job is not None
                    and job_run.job.priority
                    in {JobPriority.high, JobPriority.extra_high}
                ):
                    ensure_reset(job_run.user)
                    job_run.user.high_priority_minutes_used += delta
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

    resolved_container = container

    if resolved_container is None:
        try:
            pod = core.read_namespaced_pod(name=pod_name, namespace=namespace)
        except ApiException:  # pragma: no cover - best effort helper
            pod = None
        except Exception:  # pragma: no cover - guard against unexpected errors
            pod = None

        if pod is not None:
            spec = getattr(pod, "spec", None)
            containers = getattr(spec, "containers", None) if spec else None

            if containers:
                container_names = [
                    getattr(candidate, "name", None)
                    for candidate in containers
                    if getattr(candidate, "name", None)
                ]

                if container_names:
                    for name in container_names:
                        if not name.endswith("-uploader"):
                            resolved_container = name
                            break
                    else:
                        resolved_container = container_names[0]

    try:
        response = core.read_namespaced_pod_log(
            name=pod_name,
            namespace=namespace,
            container=resolved_container,
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
