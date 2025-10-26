from collections.abc import Sequence
from uuid import uuid4

from fastapi import HTTPException
from kubernetes import client, watch
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.core.config import get_settings
from app.models.jobs import Job, JobRun, RunStatus, Volume, VolumeState
from app.models.users import User
from app.schemas.jobs import JobCreate

settings = get_settings()


def _render_persistent_volume_claim(
    *, name: str, storage: int, read_only: bool = False
) -> dict[str, object]:
    access_mode = "ReadWriteOnce"
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": name},
        "spec": {
            "accessModes": [access_mode],
            "resources": {"requests": {"storage": f"{storage}Gi"}},
        },
    }


def _render_job_manifest(
    *,
    image: str,
    gpu: str,
    job_name: str,
    output_claim: str,
    run_id: int,
    job_id: int,
    run_token: str,
    api_base_url: str,
) -> dict[str, object]:
    volume_mounts: list[dict[str, object]] = [
        {"name": "output", "mountPath": "/opt/output"}
    ]

    main: dict[str, object] = {
        "name": job_name,
        "image": image,
        "volumeMounts": volume_mounts,
    }

    if gpu:
        resource_key = f"nvidia.com/mig-{gpu}"
        main["resources"] = {"limits": {resource_key: 1}}

    presign_endpoint = f"{api_base_url.rstrip('/')}/jobs/{job_id}/runs/{run_id}/presign"
    uploader_script = r"""
        set -euo pipefail

        apk add --no-cache curl jq ca-certificates >/dev/null

        API="https://${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT_HTTPS}"
        SA_TOKEN="$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)"
        CA_CERT="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        POD_URL="${API}/api/v1/namespaces/${POD_NAMESPACE}/pods/${POD_NAME}"

        MAIN="${MAIN_CONTAINER_NAME}"

        echo "Esperando a que el contenedor ${MAIN} termine..."
        exit_code=""
        deadline=$(( $(date +%s) + 3600 ))

        while [ -z "$exit_code" ]; do
            status="$(curl -sS -w '\n%{http_code}\n' --cacert "${CA_CERT}" \
            -H "Authorization: Bearer ${SA_TOKEN}" "${POD_URL}")" || true

            body="$(printf '%s' "$status" | head -n -1)"
            code="$(printf '%s' "$status" | tail -n 1)"

            if [ "$code" -ge 200 ] && [ "$code" -lt 300 ]; then
            exit_code="$(printf '%s' "$body" \
                | jq -r --arg MAIN "$MAIN" '
                    .status.containerStatuses[]
                    | select(.name==$MAIN)
                    | .state.terminated.exitCode // empty
                ' 2>/dev/null || true)"
            else
            echo "WARN: kube API devolvió HTTP $code; reintento..."
            fi

            if [ -z "$exit_code" ]; then
            [ "$(date +%s)" -gt "$deadline" ] && { echo "Timeout esperando exitCode"; exit 1; }
            sleep 2
            fi
        done

        echo "Contenedor ${MAIN} terminó con exitCode=${exit_code}"
        if [ "$exit_code" != "0" ]; then
            echo "Main container falló; no se suben outputs. Abortando."
            exit 1
        fi

        find /opt/output -type f | while read -r F; do
            REL="${F#/opt/output/}"
            PRES="$(curl -fsSL -H "X-Run-Token: ${RUN_TOKEN}" \
                    --get --data-urlencode "path=${REL}" \
                    "${PRESIGN_ENDPOINT}")"

            URL="$(printf '%s' "$PRES" | jq -r '.url // empty')"
            [ -n "$URL" ] || { echo "ERROR: presign no devolvió URL para ${REL}"; exit 2; }

            curl -f -X PUT --upload-file "$F" "$URL"
        done

        echo "Upload completado."
    """

    uploader = {
        "name": f"{job_name}-uploader",
        "image": "alpine:3.20",
        "command": ["/bin/sh", "-lc"],
        "args": [uploader_script],
        "env": [
            {"name": "RUN_TOKEN", "value": run_token},
            {"name": "PRESIGN_ENDPOINT", "value": presign_endpoint},
            {"name": "MAIN_CONTAINER_NAME", "value": job_name},
            {
                "name": "POD_NAME",
                "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}},
            },
            {
                "name": "POD_NAMESPACE",
                "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}},
            },
        ],
        "volumeMounts": volume_mounts,
    }

    volumes: list[dict[str, object]] = [
        {"name": "output", "persistentVolumeClaim": {"claimName": output_claim}}
    ]

    template: dict[str, object] = {
        "spec": {
            "serviceAccountName": "api-client",
            "restartPolicy": "Never",
            "securityContext": {"fsGroup": 1000},
            "containers": [main, uploader],
            "volumes": volumes,
        }
    }

    manifest: dict[str, object] = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name},
        "spec": {
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": 0,
            "template": template,
        },
    }

    return manifest


def apply_job(batch: client.BatchV1Api, manifest: dict):
    return batch.create_namespaced_job(
        body=manifest,
        namespace=settings.namespace,
    )


def apply_pvc(core: client.CoreV1Api, manifest: dict):
    return core.create_namespaced_persistent_volume_claim(
        body=manifest,
        namespace=settings.namespace,
    )


def create_volume(db: Session, storage: int, is_input: bool) -> Volume:
    vol_name = str(uuid4())
    vol = Volume(
        pvc_name=vol_name, size=storage, state=VolumeState.pvc, is_input=is_input
    )

    db.add(vol)
    db.flush()
    db.refresh(vol)
    return vol


def create_job(db: Session, payload: JobCreate, user_id: int) -> Job:
    job_name = str(uuid4())
    job = Job(
        image=payload.image,
        gpu_profile=payload.gpu,
        created_by_id=user_id,
        k8s_job_name=job_name,
    )
    db.add(job)
    db.flush()
    db.refresh(job)

    return job


def create_job_run(db: Session, job: Job, out_volume: Volume):
    run_token = uuid4().hex
    job_run = JobRun(
        job_id=job.id,
        status=RunStatus.pending,
        k8s_pod_name=None,
        started_at=None,
        finished_at=None,
        output_volume_id=out_volume.id,
        run_token=run_token,
    )
    db.add(job_run)
    db.flush()
    db.refresh(job_run)
    return job_run


def wait_for_first_pod_of_job(
    core: client.CoreV1Api,
    job_name: str,
    timeout_seconds: int = 60,
):
    w = watch.Watch()
    for event in w.stream(
        core.list_namespaced_pod,
        namespace=settings.namespace,
        label_selector=f"job-name={job_name}",
        timeout_seconds=timeout_seconds,
    ):
        pod: client.V1Pod = event["object"]  # type: ignore
        if event["type"] in {"ADDED", "MODIFIED"}:  # type: ignore
            w.stop()
            return pod
    return None


def create_and_run_job(
    core: client.CoreV1Api,
    batch: client.BatchV1Api,
    db: Session,
    payload: JobCreate,
    user: User,
):
    output_pvc = create_volume(db, is_input=False, storage=payload.storage)
    output_pvc_manifest = _render_persistent_volume_claim(
        name=output_pvc.pvc_name, storage=payload.storage, read_only=False
    )

    job = create_job(db, payload=payload, user_id=user.id)
    job_run = create_job_run(db, job, output_pvc)

    job_manifest = _render_job_manifest(
        image=job.image,
        gpu=job.gpu_profile,
        job_name=job.k8s_job_name,
        output_claim=output_pvc.pvc_name,
        run_id=job_run.id,
        job_id=job.id,
        run_token=job_run.run_token,
        api_base_url=settings.api_base_url,
    )
    apply_pvc(core, output_pvc_manifest)
    apply_job(batch, job_manifest)

    pod = wait_for_first_pod_of_job(core, job.k8s_job_name)
    if not pod:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Could not create pod for job {job.id}"
        )
    job_run.k8s_pod_name = pod.metadata.name
    db.commit()
    return job_run


def list_jobs(db: Session) -> Sequence[Job]:
    stmt = select(Job).options(selectinload(Job.runs)).order_by(Job.submitted_at.desc())
    result = db.execute(stmt)
    return result.scalars().unique().all()


def get_job(db: Session, job_id: int) -> Job:
    stmt = (
        select(Job)
        .options(
            selectinload(Job.runs).selectinload(JobRun.output_volume),
            selectinload(Job.runs).selectinload(JobRun.input_volume),
        )
        .where(Job.id == job_id)
    )
    result = db.execute(stmt).scalar_one_or_none()
    if result is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return result
