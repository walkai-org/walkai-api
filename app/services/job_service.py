import base64
import json
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from uuid import uuid4

from botocore.client import BaseClient
from botocore.exceptions import ClientError
from fastapi import HTTPException, status
from kubernetes import client, watch
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.core.config import get_settings
from app.models.jobs import Job, JobRun, RunStatus, Volume
from app.models.users import User
from app.schemas.jobs import JobCreate, JobImage

settings = get_settings()


def _render_persistent_volume_claim(*, name: str, storage: int) -> dict[str, object]:
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
    input_volume: Volume | None,
    run_id: int,
    job_id: int,
    run_token: str,
    api_base_url: str,
    image_pull_secret: str | None = None,
    secret_names: Sequence[str] | None = None,
) -> dict[str, object]:
    volume_mounts_main: list[dict[str, object]] = []
    volume_mounts_output: list[dict[str, object]] = []
    volume_mounts_input: list[dict[str, object]] = []
    volumes: list[dict[str, object]] = []

    if input_volume:
        volume_mounts_main.append({"name": "input", "mountPath": "/opt/input"})
        volume_mounts_input.append({"name": "input", "mountPath": "/opt/input"})
        volumes.append(
            {
                "name": "input",
                "persistentVolumeClaim": {"claimName": input_volume.pvc_name},
            }
        )

    if output_claim:
        volume_mounts_main.append({"name": "output", "mountPath": "/opt/output"})
        volume_mounts_output.append({"name": "output", "mountPath": "/opt/output"})
        volumes.append(
            {"name": "output", "persistentVolumeClaim": {"claimName": output_claim}}
        )

    main: dict[str, object] = {
        "name": job_name,
        "image": image,
        "imagePullPolicy": "Always",
        "volumeMounts": volume_mounts_main,
    }

    if secret_names:
        main["envFrom"] = [{"secretRef": {"name": name}} for name in secret_names]

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
        LOG_FILE="$(mktemp)"
        LOG_ENDPOINT="${POD_URL}/log?container=${MAIN}&timestamps=true"

        echo "Descargando logs del contenedor ${MAIN}..."
        if curl -fsS --cacert "${CA_CERT}" \
            -H "Authorization: Bearer ${SA_TOKEN}" \
            "${LOG_ENDPOINT}" > "${LOG_FILE}"
        then
            LOG_PATH="logs/${MAIN}.log"
            PRES="$(curl -fsSL -H "X-Run-Token: ${RUN_TOKEN}" \
                    --get --data-urlencode "path=${LOG_PATH}" \
                    "${PRESIGN_ENDPOINT}")"

            URL="$(printf '%s' "$PRES" | jq -r '.url // empty')"
            [ -n "$URL" ] || { echo "ERROR: presign no devolvió URL para logs"; rm -f "${LOG_FILE}"; exit 2; }

            curl -f -X PUT --upload-file "${LOG_FILE}" "$URL"
            echo "Logs subidos a ${LOG_PATH}"
        else
            echo "WARN: No se pudieron obtener logs del contenedor ${MAIN}"
        fi

        rm -f "${LOG_FILE}"
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
        "volumeMounts": volume_mounts_output,
    }

    input_list_endpoint = (
        f"{api_base_url.rstrip('/')}/jobs/{job_id}/runs/{run_id}/inputs"
    )

    downloader_script = r"""
        set -euo pipefail

        apk add --no-cache curl jq ca-certificates >/dev/null

        echo "Descargando inputs..."

        RESP="$(
        curl -fsSL \
            -H "X-Run-Token: ${RUN_TOKEN}" \
            "${INPUT_LIST_ENDPOINT}"
        )"

        echo "Respuesta de inputs: ${RESP}"

        echo "$RESP" | jq -r '.files[]' | while read -r KEY; do
            echo "Procesando ${KEY}..."

            PRES="$(
                curl -fsSL \
                    -H "X-Run-Token: ${RUN_TOKEN}" \
                    --get --data-urlencode "path=${KEY}" \
                    --get --data-urlencode "method=GET" \
                    --get --data-urlencode "direction=input" \
                    "${PRESIGN_ENDPOINT}"
            )"

            URL="$(printf '%s' "$PRES" | jq -r '.url // empty')"
            [ -n "$URL" ] || { echo "ERROR: presign no devolvió URL para ${KEY}"; exit 2; }

            DEST="/opt/input/${KEY}"

            mkdir -p "$(dirname "$DEST")"
            echo "Descargando ${KEY} a ${DEST}..."
            curl -fsSL "$URL" -o "$DEST"
        done

        echo "Inputs descargados correctamente."
    """
    init_containers: list[dict[str, object]] = []

    if input_volume:
        downloader = {
            "name": f"{job_name}-downloader",
            "image": "alpine:3.20",
            "command": ["/bin/sh", "-lc"],
            "args": [downloader_script],
            "env": [
                {"name": "RUN_TOKEN", "value": run_token},
                {"name": "INPUT_LIST_ENDPOINT", "value": input_list_endpoint},
                {"name": "PRESIGN_ENDPOINT", "value": presign_endpoint},
            ],
            "volumeMounts": volume_mounts_input,
        }
        init_containers.append(downloader)

    pod_spec: dict[str, object] = {
        "serviceAccountName": "api-client",
        "restartPolicy": "Never",
        "securityContext": {"fsGroup": 1000},
        "containers": [main, uploader],
        "volumes": volumes,
    }

    if init_containers:
        pod_spec["initContainers"] = init_containers

    if image_pull_secret:
        pod_spec["imagePullSecrets"] = [{"name": image_pull_secret}]

    template: dict[str, object] = {"spec": pod_spec}

    manifest: dict[str, object] = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name},
        "spec": {
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": 12,
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


def apply_registry_secret(core: client.CoreV1Api, manifest: dict):
    return core.create_namespaced_secret(
        body=manifest,
        namespace=settings.namespace,
    )


def _decode_registry_token(token: str) -> tuple[str, str]:
    try:
        decoded = base64.b64decode(token).decode("utf-8")
    except Exception as exc:  # pragma: no cover - defensive guard
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ECR authorization token is invalid",
        ) from exc

    username, separator, password = decoded.partition(":")
    if not separator or not username or not password:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ECR authorization token is malformed",
        )
    return username, password


def set_registry_secret_owner(
    core: client.CoreV1Api,
    *,
    secret_name: str,
    job_manifest: dict[str, object],
    job_resource,
):
    owner_reference = _build_job_owner_reference(job_manifest, job_resource)

    body = {"metadata": {"ownerReferences": [owner_reference]}}
    return core.patch_namespaced_secret(
        name=secret_name,
        namespace=settings.namespace,
        body=body,
    )


def _build_job_owner_reference(
    job_manifest: dict[str, object], job_resource
) -> dict[str, object]:
    metadata = getattr(job_resource, "metadata", None)
    job_uid = getattr(metadata, "uid", None) if metadata else None
    job_name = getattr(metadata, "name", None) if metadata else None
    manifest_metadata = job_manifest.get("metadata", {})
    if not isinstance(manifest_metadata, dict):
        manifest_metadata = {}
    if job_name is None:
        job_name = manifest_metadata.get("name")

    if not job_uid or not job_name:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Job metadata missing for owner reference",
        )

    owner_reference = {
        "apiVersion": job_manifest.get("apiVersion", "batch/v1"),
        "kind": job_manifest.get("kind", "Job"),
        "name": job_name,
        "uid": job_uid,
        "controller": True,
        "blockOwnerDeletion": False,
    }
    return owner_reference


def set_pvc_owner(
    core: client.CoreV1Api,
    *,
    pvc_name: str,
    job_manifest: dict[str, object],
    job_resource,
):
    owner_reference = _build_job_owner_reference(job_manifest, job_resource)
    body = {"metadata": {"ownerReferences": [owner_reference]}}
    return core.patch_namespaced_persistent_volume_claim(
        name=pvc_name,
        namespace=settings.namespace,
        body=body,
    )


def _render_registry_secret(
    *, name: str, registry: str, token: str
) -> dict[str, object]:
    registry_host = registry.rstrip("/")
    if not registry_host:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ECR registry URL is not configured",
        )

    username, password = _decode_registry_token(token)
    docker_config = {
        "auths": {
            registry_host: {
                "username": username,
                "password": password,
                "auth": token,
            }
        }
    }
    encoded_config = base64.b64encode(json.dumps(docker_config).encode("utf-8")).decode(
        "utf-8"
    )

    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name},
        "type": "kubernetes.io/dockerconfigjson",
        "data": {".dockerconfigjson": encoded_config},
    }


def _fetch_registry_token(ecr_client: BaseClient) -> str:
    response = ecr_client.get_authorization_token()
    auth_data = response.get("authorizationData", [])
    if not auth_data:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ECR authorization data is unavailable",
        )

    token = auth_data[0].get("authorizationToken")
    if not token:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ECR authorization token is missing",
        )
    return token


def _extract_repository_name(registry_url: str) -> str:
    trimmed = registry_url.strip().rstrip("/")
    if not trimmed:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ECR registry URL is not configured",
        )

    normalized = trimmed.split("://", 1)[-1]
    if "/" not in normalized:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ECR repository name is missing in registry URL",
        )

    repository = normalized.split("/", 1)[1].strip("/")
    if not repository:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ECR repository name is missing in registry URL",
        )
    return repository


def _pushed_at_sort_value(value: datetime | None) -> float:
    if value is None:
        return 0.0
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        return value.replace(tzinfo=UTC).timestamp()
    return value.timestamp()


def list_available_images(ecr_client: BaseClient) -> list[JobImage]:
    repository = _extract_repository_name(settings.ecr_url)
    registry = settings.ecr_url.rstrip("/")
    try:
        paginator = ecr_client.get_paginator("describe_images")
        pages = paginator.paginate(
            repositoryName=repository,
            filter={"tagStatus": "TAGGED"},
        )
    except ClientError as exc:
        print(exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to list images from ECR",
        ) from exc

    images: list[JobImage] = []
    try:
        for page in pages:
            for detail in page.get("imageDetails", []):
                tags = detail.get("imageTags") or []
                if not tags:
                    continue
                digest = detail.get("imageDigest")
                pushed_at = detail.get("imagePushedAt")
                for tag in tags:
                    images.append(
                        JobImage(
                            image=f"{registry}:{tag}",
                            tag=tag,
                            digest=digest,
                            pushed_at=pushed_at,
                        )
                    )
    except ClientError as exc:
        print(exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to list images from ECR",
        ) from exc

    images.sort(
        key=lambda item: (_pushed_at_sort_value(item.pushed_at), item.tag),
        reverse=True,
    )
    return images


def _generate_volume_name(prefix: str = "vol", *, suffix_length: int = 8) -> str:
    suffix = uuid4().hex[:suffix_length]
    return f"{prefix}-{suffix}"


def create_volume(
    db: Session,
    storage: int,
    is_input: bool,
    key_prefix: str | None = None,
    pvc_name: str | None = None,
) -> Volume:
    vol_name = pvc_name or str(uuid4())
    vol = Volume(
        pvc_name=vol_name,
        size=storage,
        is_input=is_input,
        key_prefix=key_prefix if is_input else None,
    )

    db.add(vol)
    db.flush()
    db.refresh(vol)
    return vol


def create_input_volume_with_upload(
    db: Session,
    *,
    user: User,
    storage: int,
) -> Volume:
    volume_name = _generate_volume_name(prefix="input")
    key_prefix = f"users/{user.id}/inputs/{volume_name}"
    volume = create_volume(
        db,
        storage=storage,
        is_input=True,
        key_prefix=key_prefix,
        pvc_name=volume_name,
    )
    db.commit()
    db.refresh(volume)
    return volume


def create_job(db: Session, payload: JobCreate, user_id: int) -> Job:
    job = Job(
        image=payload.image,
        gpu_profile=payload.gpu,
        created_by_id=user_id,
    )
    db.add(job)
    db.flush()
    db.refresh(job)

    return job


def create_job_run(
    db: Session,
    job: Job,
    out_volume: Volume,
    input_pvc: Volume | None = None,
):
    run_token = uuid4().hex
    job_name = str(uuid4())

    job_run = JobRun(
        job_id=job.id,
        status=RunStatus.pending,
        k8s_pod_name=None,
        started_at=None,
        finished_at=None,
        output_volume_id=out_volume.id,
        input_volume_id=input_pvc.id if input_pvc else None,
        run_token=run_token,
        k8s_job_name=job_name,
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
    ecr_client: BaseClient,
    db: Session,
    payload: JobCreate,
    user: User,
):
    output_pvc = create_volume(db, is_input=False, storage=payload.storage)
    output_pvc_manifest = _render_persistent_volume_claim(
        name=output_pvc.pvc_name, storage=payload.storage
    )

    input_vol = None
    input_pvc_manifest = None
    if payload.input_id:
        input_vol = get_volume(db, payload.input_id)
        input_pvc_manifest = _render_persistent_volume_claim(
            name=input_vol.pvc_name, storage=input_vol.size
        )

    job = create_job(db, payload=payload, user_id=user.id)
    job_run = create_job_run(db, job, output_pvc, input_pvc=input_vol)

    registry_secret_name = f"{job_run.k8s_job_name}-registry"
    authorization_token = _fetch_registry_token(ecr_client)
    registry_secret_manifest = _render_registry_secret(
        name=registry_secret_name,
        registry=settings.ecr_url,
        token=authorization_token,
    )

    job_manifest = _render_job_manifest(
        image=job.image,
        gpu=job.gpu_profile,
        job_name=job_run.k8s_job_name,
        output_claim=output_pvc.pvc_name,
        input_volume=input_vol,
        run_id=job_run.id,
        job_id=job.id,
        run_token=job_run.run_token,
        api_base_url=settings.api_base_url,
        image_pull_secret=registry_secret_name,
        secret_names=payload.secret_names,
    )
    apply_registry_secret(core, registry_secret_manifest)
    apply_pvc(core, output_pvc_manifest)
    if input_pvc_manifest:
        apply_pvc(core, input_pvc_manifest)

    job_resource = apply_job(batch, job_manifest)
    set_registry_secret_owner(
        core,
        secret_name=registry_secret_name,
        job_manifest=job_manifest,
        job_resource=job_resource,
    )
    set_pvc_owner(
        core,
        pvc_name=output_pvc.pvc_name,
        job_manifest=job_manifest,
        job_resource=job_resource,
    )
    if input_vol:
        set_pvc_owner(
            core,
            pvc_name=input_vol.pvc_name,
            job_manifest=job_manifest,
            job_resource=job_resource,
        )

    pod = wait_for_first_pod_of_job(core, job_run.k8s_job_name)
    if not pod:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Could not create pod for job {job.id}"
        )
    job_run.k8s_pod_name = pod.metadata.name  # type: ignore
    db.commit()
    return job_run


def list_jobs(db: Session) -> Sequence[Job]:
    stmt = select(Job).options(selectinload(Job.runs)).order_by(Job.submitted_at.desc())
    result = db.execute(stmt)
    return result.scalars().unique().all()


def list_volumes(db: Session, *, is_input: bool | None = None) -> Sequence[Volume]:
    stmt = select(Volume)
    if is_input is not None:
        stmt = stmt.where(Volume.is_input.is_(is_input))
    stmt = stmt.order_by(Volume.id.desc())
    result = db.execute(stmt)
    return result.scalars().all()


def get_job(db: Session, job_id: int) -> Job:
    stmt = (
        select(Job)
        .options(
            selectinload(Job.runs),
        )
        .where(Job.id == job_id)
    )
    result = db.execute(stmt).scalar_one_or_none()
    if result is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return result


def _resolve_volume_prefix(volume: Volume) -> str:
    prefix = volume.key_prefix
    if prefix:
        return prefix.rstrip("/")

    raise HTTPException(
        status_code=404, detail="Volume is not stored in object storage"
    )


def _resolve_output_prefix(job_run: JobRun) -> str:
    volume = job_run.output_volume
    if volume is None:
        raise HTTPException(status_code=404, detail="Run has no output volume")

    try:
        return _resolve_volume_prefix(volume)
    except HTTPException:
        pass

    job = job_run.job
    if job is None:
        raise HTTPException(status_code=502, detail="Job data missing for run")

    return f"users/{job.created_by_id}/jobs/{job_run.job_id}/{job_run.id}/outputs"


def _stream_s3_object(
    s3_client: BaseClient,
    *,
    key: str,
    not_found_detail: str,
    chunk_size: int,
):
    try:
        response = s3_client.get_object(
            Bucket=settings.aws_s3_bucket,
            Key=key,
        )
    except ClientError as exc:
        error = exc.response.get("Error", {}) if hasattr(exc, "response") else {}
        code = error.get("Code")
        if code in {"NoSuchKey", "404", "NotFound"}:
            raise HTTPException(status_code=404, detail=not_found_detail) from exc
        raise HTTPException(
            status_code=502,
            detail="Failed to retrieve object from storage",
        ) from exc

    body = response.get("Body")
    if body is None:
        return iter(()), response

    def _iterator() -> Iterable[bytes]:
        try:
            while True:
                chunk = body.read(chunk_size)
                if not chunk:
                    break
                yield chunk
        finally:
            body.close()

    return _iterator(), response


def _normalize_relative_path(path: str) -> str:
    candidate = path.strip().strip("/")
    if not candidate:
        raise HTTPException(status_code=400, detail="File path is required")

    parts = candidate.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise HTTPException(status_code=400, detail="Invalid file path")

    return "/".join(parts)


def get_job_run(db: Session, job_id: int, run_id: int) -> JobRun:
    stmt = (
        select(JobRun)
        .options(
            selectinload(JobRun.job),
            selectinload(JobRun.output_volume),
            selectinload(JobRun.input_volume),
        )
        .where(JobRun.id == run_id, JobRun.job_id == job_id)
    )
    result = db.execute(stmt).scalar_one_or_none()
    if result is None:
        raise HTTPException(status_code=404, detail="Job run not found")
    return result


def get_job_run_by_pod_name(db: Session, pod_name: str) -> JobRun:
    stmt = (
        select(JobRun)
        .options(
            selectinload(JobRun.job),
            selectinload(JobRun.output_volume),
            selectinload(JobRun.input_volume),
        )
        .where(JobRun.k8s_pod_name == pod_name)
    )
    result = db.execute(stmt).scalar_one_or_none()
    if result is None:
        raise HTTPException(status_code=404, detail="Job run not found")
    return result


def get_volume(db: Session, volume_id: int) -> Volume:
    volume = db.get(Volume, volume_id)
    if volume is None:
        raise HTTPException(status_code=404, detail="Volume not found")
    return volume


def list_volume_objects(
    s3_client: BaseClient,
    volume: Volume,
    *,
    continuation_token: str | None = None,
    max_keys: int | None = None,
) -> dict[str, object]:
    prefix = _resolve_volume_prefix(volume)
    s3_prefix = prefix.rstrip("/") + "/"

    kwargs: dict[str, object] = {
        "Bucket": settings.aws_s3_bucket,
        "Prefix": s3_prefix,
    }
    if continuation_token:
        kwargs["ContinuationToken"] = continuation_token
    if max_keys is not None:
        kwargs["MaxKeys"] = max_keys

    try:
        response = s3_client.list_objects_v2(**kwargs)
    except ClientError as exc:
        print(f"error {exc}")
        raise HTTPException(
            status_code=502,
            detail="Failed to list objects from storage",
        ) from exc

    contents: list[dict[str, object]] = []
    directories_set: set[str] = set()
    for item in response.get("Contents", []):
        key = item.get("Key")
        if not key or not key.startswith(s3_prefix):
            continue
        relative = key[len(s3_prefix) :]
        if not relative:
            continue
        contents.append(
            {
                "key": relative,
                "size": int(item.get("Size", 0)),
                "last_modified": item.get("LastModified"),
                "etag": item.get("ETag"),
            }
        )

        parts = relative.split("/")
        if len(parts) > 1:
            for index in range(len(parts) - 1):
                directories_set.add("/".join(parts[: index + 1]) + "/")

    return {
        "prefix": prefix,
        "objects": contents,
        "truncated": bool(response.get("IsTruncated")),
        "next_continuation_token": response.get("NextContinuationToken"),
    }


def stream_job_run_logs(
    s3_client: BaseClient,
    job_run: JobRun,
    *,
    chunk_size: int = 4096,
):
    if not job_run.k8s_job_name:
        raise HTTPException(status_code=404, detail="Log file not available")

    prefix = _resolve_output_prefix(job_run)
    key = f"{prefix}/logs/{job_run.k8s_job_name}.log"

    iterator, _ = _stream_s3_object(
        s3_client,
        key=key,
        not_found_detail="Log file not found",
        chunk_size=chunk_size,
    )
    return iterator


def stream_volume_file(
    s3_client: BaseClient,
    volume: Volume,
    path: str,
    *,
    chunk_size: int = 4096,
):
    normalized_path = _normalize_relative_path(path)
    prefix = _resolve_volume_prefix(volume)
    key = f"{prefix}/{normalized_path}"

    iterator, response = _stream_s3_object(
        s3_client,
        key=key,
        not_found_detail="File not found",
        chunk_size=chunk_size,
    )

    metadata = {
        "path": normalized_path,
        "content_type": response.get("ContentType"),
        "content_length": response.get("ContentLength"),
        "etag": response.get("ETag"),
    }
    return iterator, metadata
