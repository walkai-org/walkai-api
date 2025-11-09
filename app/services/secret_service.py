import base64
from typing import Final

from fastapi import HTTPException, status
from kubernetes import client
from kubernetes.client import ApiException

from app.core.config import get_settings
from app.schemas.secrets import (
    SecretCreate,
    SecretDetail,
    SecretRef,
    normalize_secret_name,
)

settings = get_settings()

MANAGED_SECRET_LABEL_KEY: Final = "app.walkai.dev/managed-secret"
MANAGED_SECRET_LABEL_VALUE: Final = "true"


def _render_secret_manifest(payload: SecretCreate) -> dict[str, object]:
    encoded_data = {
        key: base64.b64encode(value.encode("utf-8")).decode("utf-8")
        for key, value in payload.data.items()
    }
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": payload.name,
            "labels": {MANAGED_SECRET_LABEL_KEY: MANAGED_SECRET_LABEL_VALUE},
        },
        "type": "Opaque",
        "data": encoded_data,
    }


def create_secret(core: client.CoreV1Api, payload: SecretCreate) -> SecretDetail:
    manifest = _render_secret_manifest(payload)
    try:
        core.create_namespaced_secret(
            namespace=settings.namespace,
            body=manifest,
        )
    except ApiException as exc:
        if exc.status == status.HTTP_409_CONFLICT:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Secret {payload.name} already exists",
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to create secret in Kubernetes",
        ) from exc

    keys = sorted(payload.data.keys())
    return SecretDetail(name=payload.name, keys=keys)


def _is_managed_secret(resource) -> bool:
    metadata = getattr(resource, "metadata", None)
    labels = getattr(metadata, "labels", None) if metadata else None
    if isinstance(labels, dict):
        return labels.get(MANAGED_SECRET_LABEL_KEY) == MANAGED_SECRET_LABEL_VALUE
    return False


def list_managed_secrets(core: client.CoreV1Api) -> list[SecretRef]:
    try:
        secret_list = core.list_namespaced_secret(namespace=settings.namespace)
    except ApiException as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to list secrets from Kubernetes",
        ) from exc

    items = getattr(secret_list, "items", None) or []
    managed = []
    for secret in items:
        metadata = getattr(secret, "metadata", None)
        name = getattr(metadata, "name", None) if metadata else None
        if name and _is_managed_secret(secret):
            managed.append(SecretRef(name=name))

    managed.sort(key=lambda ref: ref.name)
    return managed


def _fetch_managed_secret(core: client.CoreV1Api, name: str):
    normalized = normalize_secret_name(name)
    try:
        secret = core.read_namespaced_secret(
            name=normalized,
            namespace=settings.namespace,
        )
    except ApiException as exc:
        if exc.status == status.HTTP_404_NOT_FOUND:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Secret {normalized} not found",
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to read secret from Kubernetes",
        ) from exc

    if not _is_managed_secret(secret):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Secret {normalized} is not managed by walk:ai",
        )

    return secret


def get_secret_detail(core: client.CoreV1Api, name: str) -> SecretDetail:
    secret = _fetch_managed_secret(core, name)
    data = getattr(secret, "data", None) or {}
    keys = sorted(data.keys())
    metadata = getattr(secret, "metadata", None)
    resolved_name = getattr(metadata, "name", None) if metadata else None
    return SecretDetail(name=resolved_name or normalize_secret_name(name), keys=keys)


def delete_secret(core: client.CoreV1Api, name: str) -> None:
    secret = _fetch_managed_secret(core, name)
    metadata = getattr(secret, "metadata", None)
    resolved_name = getattr(metadata, "name", None) if metadata else None
    normalized = resolved_name or normalize_secret_name(name)

    try:
        core.delete_namespaced_secret(
            name=normalized,
            namespace=settings.namespace,
        )
    except ApiException as exc:
        if exc.status == status.HTTP_404_NOT_FOUND:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Secret {normalized} not found",
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to delete secret from Kubernetes",
        ) from exc
