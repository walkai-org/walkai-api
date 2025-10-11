from uuid import uuid4

from fastapi import HTTPException
from kubernetes import client, watch
from sqlalchemy.orm import Session

from app.models.jobs import Job, JobRun, RunStatus, Volume, VolumeState
from app.models.users import User
from app.schemas.jobs import JobCreate


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
    *, image: str, gpu: str, job_name: str, output_claim: str
) -> dict[str, object]:
    volume_mounts: list[dict[str, object]] = [
        {"name": "output", "mountPath": "/opt/output"}
    ]

    container: dict[str, object] = {
        "name": job_name,
        "image": image,
        "volumeMounts": volume_mounts,
    }

    if gpu:
        resource_key = f"nvidia.com/mig-{gpu}"
        container["resources"] = {"limits": {resource_key: 1}}

    volumes: list[dict[str, object]] = [
        {"name": "output", "persistentVolumeClaim": {"claimName": output_claim}}
    ]

    template: dict[str, object] = {
        "spec": {
            "restartPolicy": "Never",
            "containers": [container],
            "volumes": volumes,
        }
    }

    manifest: dict[str, object] = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name},
        "spec": {"backoffLimit": 0, "template": template},
    }

    return manifest


def apply_job(batch: client.BatchV1Api, manifest: dict):
    return batch.create_namespaced_job(
        body=manifest,
        namespace="walkai",
    )


def apply_pvc(core: client.CoreV1Api, manifest: dict):
    return core.create_namespaced_persistent_volume_claim(
        body=manifest,
        namespace="walkai",
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


def create_job_run(db: Session, job: Job, out_volume: Volume, pod_name: str):
    job_run = JobRun(
        job_id=job.id,
        status=RunStatus.pending,
        k8s_pod_name=pod_name,
        started_at=None,
        finished_at=None,
        output_volume_id=out_volume.id,
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
        namespace="walkai",
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
    job_manifest = _render_job_manifest(
        image=job.image,
        gpu=job.gpu_profile,
        job_name=job.k8s_job_name,
        output_claim=output_pvc.pvc_name,
    )
    apply_pvc(core, output_pvc_manifest)
    apply_job(batch, job_manifest)

    pod = wait_for_first_pod_of_job(core, job.k8s_job_name)
    if not pod:
        raise HTTPException(
            status_code=400, detail=f"Could not create pod for job {job.id}"
        )
    job_run = create_job_run(db, job, output_pvc, pod.metadata.name)  # type: ignore
    db.commit()
    return job_run
