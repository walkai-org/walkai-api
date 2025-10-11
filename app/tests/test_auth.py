from sqlalchemy import select

from app.core.security import hash_token
from app.models.users import PersonalAccessToken


def test_create_pat_returns_token(auth_client, db_session):
    client, _ = auth_client
    response = client.post(
        "/users/me/tokens/",
        json={"name": "CLI token"},
    )

    assert response.status_code == 201
    payload = response.json()
    raw_token = payload["token"]
    assert payload["token_prefix"] == raw_token[:8]
    assert payload["name"] == "CLI token"

    db_session.expire_all()
    stored_pat = db_session.execute(select(PersonalAccessToken)).scalar_one()
    assert stored_pat.name == "CLI token"
    assert stored_pat.token_prefix == raw_token[:8]
    assert stored_pat.token_hash == hash_token(raw_token)
    assert stored_pat.token_hash != raw_token


def test_personal_access_token_authenticates_user(auth_client, db_session):
    client, _ = auth_client
    create_resp = client.post(
        "/users/me/tokens/",
        json={"name": "CLI token"},
    )
    raw_token = create_resp.json()["token"]

    list_resp = client.get(
        "/users/me/tokens/",
        headers={"Authorization": f"Bearer {raw_token}"},
    )

    assert list_resp.status_code == 200
    tokens = list_resp.json()
    assert len(tokens) == 1
    assert "token" not in tokens[0]
    assert tokens[0]["token_prefix"] == raw_token[:8]


def test_revoked_pat_cannot_authenticate(auth_client, db_session):
    client, _ = auth_client
    create_resp = client.post(
        "/users/me/tokens/",
        json={"name": "Temp token"},
    )
    created = create_resp.json()
    raw_token = created["token"]

    delete_resp = client.delete(
        f"/users/me/tokens/{created['id']}",
    )
    assert delete_resp.status_code == 204

    db_session.expire_all()
    remaining = db_session.execute(select(PersonalAccessToken)).all()
    assert not remaining

    client.cookies.clear()

    unauthorized = client.get(
        "/users/me/tokens/",
        headers={"Authorization": f"Bearer {raw_token}"},
    )

    assert unauthorized.status_code == 401
    assert unauthorized.json()["detail"] == "Not authenticated"
