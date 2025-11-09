from fastapi import APIRouter, Depends, status
from kubernetes import client

from app.api.deps import get_current_user
from app.core.k8s import get_core
from app.schemas.secrets import SecretCreate, SecretDetail, SecretRef
from app.services import secret_service

router = APIRouter(prefix="/secrets", tags=["secrets"])


@router.post("/", response_model=SecretDetail, status_code=status.HTTP_201_CREATED)
def create_secret(
    payload: SecretCreate,
    core: client.CoreV1Api = Depends(get_core),
    _: object = Depends(get_current_user),
) -> SecretDetail:
    return secret_service.create_secret(core, payload)


@router.get("/", response_model=list[SecretRef])
def list_secrets(
    core: client.CoreV1Api = Depends(get_core),
    _: object = Depends(get_current_user),
) -> list[SecretRef]:
    return secret_service.list_managed_secrets(core)


@router.get("/{secret_name}", response_model=SecretDetail)
def get_secret_detail(
    secret_name: str,
    core: client.CoreV1Api = Depends(get_core),
    _: object = Depends(get_current_user),
) -> SecretDetail:
    return secret_service.get_secret_detail(core, secret_name)
