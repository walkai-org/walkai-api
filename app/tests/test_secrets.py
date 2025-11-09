import base64
from types import SimpleNamespace

from kubernetes.client import ApiException

from app.core.config import get_settings
from app.core.k8s import get_core
from app.main import app
from app.services import secret_service


def _override_core(fake_core):
    app.dependency_overrides[get_core] = lambda: fake_core


def _clear_core_override():
    app.dependency_overrides.pop(get_core, None)


def test_create_secret_creates_k8s_secret(auth_client):
    client, _ = auth_client
    settings = get_settings()
    captured: dict[str, object] = {}

    class FakeCore:
        def create_namespaced_secret(self, namespace, body):
            captured["namespace"] = namespace
            captured["body"] = body

    _override_core(FakeCore())
    try:
        response = client.post(
            "/secrets/",
            json={
                "name": "api-token",
                "data": {"API_KEY": "super-secret"},
            },
        )
    finally:
        _clear_core_override()

    assert response.status_code == 201
    assert response.json() == {"name": "api-token", "keys": ["API_KEY"]}
    assert captured["namespace"] == settings.namespace

    body = captured["body"]
    labels = body["metadata"]["labels"]
    assert (
        labels[secret_service.MANAGED_SECRET_LABEL_KEY]
        == secret_service.MANAGED_SECRET_LABEL_VALUE
    )
    encoded_value = base64.b64encode(b"super-secret").decode("utf-8")
    assert body["data"]["API_KEY"] == encoded_value


def test_list_secrets_returns_only_managed(auth_client):
    client, _ = auth_client
    settings = get_settings()

    managed_secret = SimpleNamespace(
        metadata=SimpleNamespace(
            name="managed-secret",
            labels={
                secret_service.MANAGED_SECRET_LABEL_KEY: secret_service.MANAGED_SECRET_LABEL_VALUE
            },
        )
    )
    unmanaged_secret = SimpleNamespace(
        metadata=SimpleNamespace(
            name="default-token",
            labels={"kubernetes.io/service-account.name": "default"},
        )
    )

    class FakeCore:
        def list_namespaced_secret(self, namespace):
            assert namespace == settings.namespace
            return SimpleNamespace(items=[managed_secret, unmanaged_secret])

    _override_core(FakeCore())
    try:
        response = client.get("/secrets/")
    finally:
        _clear_core_override()

    assert response.status_code == 200
    assert response.json() == [{"name": "managed-secret"}]


def test_get_secret_detail_includes_keys(auth_client):
    client, _ = auth_client
    settings = get_settings()

    secret = SimpleNamespace(
        metadata=SimpleNamespace(
            name="managed-secret",
            labels={
                secret_service.MANAGED_SECRET_LABEL_KEY: secret_service.MANAGED_SECRET_LABEL_VALUE
            },
        ),
        data={"API_KEY": "dmFsdWU=", "SECOND": "c2Vjb25k"},
    )

    class FakeCore:
        def read_namespaced_secret(self, name, namespace):
            assert name == "managed-secret"
            assert namespace == settings.namespace
            return secret

    _override_core(FakeCore())
    try:
        response = client.get("/secrets/managed-secret")
    finally:
        _clear_core_override()

    assert response.status_code == 200
    assert response.json() == {
        "name": "managed-secret",
        "keys": ["API_KEY", "SECOND"],
    }


def test_get_secret_detail_returns_404_when_missing(auth_client):
    client, _ = auth_client
    settings = get_settings()

    class FakeCore:
        def read_namespaced_secret(self, name, namespace):
            assert namespace == settings.namespace
            raise ApiException(status=404, reason="Not Found")

    _override_core(FakeCore())
    try:
        response = client.get("/secrets/missing")
    finally:
        _clear_core_override()

    assert response.status_code == 404
    assert response.json()["detail"] == "Secret missing not found"


def test_get_secret_detail_returns_404_for_unmanaged(auth_client):
    client, _ = auth_client
    settings = get_settings()

    secret = SimpleNamespace(
        metadata=SimpleNamespace(name="kube-root-ca.crt", labels={}),
        data={"ca.crt": "LS0t"},
    )

    class FakeCore:
        def read_namespaced_secret(self, name, namespace):
            assert namespace == settings.namespace
            return secret

    _override_core(FakeCore())
    try:
        response = client.get("/secrets/kube-root-ca.crt")
    finally:
        _clear_core_override()

    assert response.status_code == 404
    assert "not managed" in response.json()["detail"]


def test_delete_secret_removes_secret(auth_client):
    client, _ = auth_client
    settings = get_settings()
    captured: dict[str, object] = {}

    secret = SimpleNamespace(
        metadata=SimpleNamespace(
            name="managed-secret",
            labels={
                secret_service.MANAGED_SECRET_LABEL_KEY: secret_service.MANAGED_SECRET_LABEL_VALUE
            },
        ),
        data={},
    )

    class FakeCore:
        def read_namespaced_secret(self, name, namespace):
            assert name == "managed-secret"
            assert namespace == settings.namespace
            return secret

        def delete_namespaced_secret(self, name, namespace):
            captured["delete"] = (name, namespace)

    _override_core(FakeCore())
    try:
        response = client.delete("/secrets/managed-secret")
    finally:
        _clear_core_override()

    assert response.status_code == 204
    assert captured["delete"] == ("managed-secret", settings.namespace)


def test_delete_secret_returns_404_when_missing(auth_client):
    client, _ = auth_client
    settings = get_settings()

    class FakeCore:
        def read_namespaced_secret(self, name, namespace):
            assert namespace == settings.namespace
            raise ApiException(status=404, reason="Not Found")

    _override_core(FakeCore())
    try:
        response = client.delete("/secrets/missing")
    finally:
        _clear_core_override()

    assert response.status_code == 404
    assert response.json()["detail"] == "Secret missing not found"


def test_delete_secret_returns_404_for_unmanaged(auth_client):
    client, _ = auth_client
    settings = get_settings()

    secret = SimpleNamespace(
        metadata=SimpleNamespace(name="kube-root-ca.crt", labels={}),
        data=None,
    )

    class FakeCore:
        def read_namespaced_secret(self, name, namespace):
            assert namespace == settings.namespace
            return secret

    _override_core(FakeCore())
    try:
        response = client.delete("/secrets/kube-root-ca.crt")
    finally:
        _clear_core_override()

    assert response.status_code == 404
    assert "not managed" in response.json()["detail"]
