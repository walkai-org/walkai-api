from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse

from sqlalchemy import select

from app.api import password_reset as password_reset_api
from app.core.security import hash_token, verify_password
from app.models.users import PasswordResetToken, User
from app.services import password_reset_service


def test_forgot_password_returns_generic_message_for_unknown_email(client):
    response = client.post(
        "/password/forgot",
        json={"email": "missing@example.com"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "message": "If the email address is valid, instructions will be sent."
    }


def test_forgot_password_creates_token_and_sends_email(client, db_session, monkeypatch):
    user = User(email="resetme@example.com", password_hash=None, role="user")
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    captured = {}

    def fake_send(to_email: str, link: str) -> None:
        captured["to_email"] = to_email
        captured["link"] = link

    monkeypatch.setattr(
        password_reset_api,
        "send_password_reset_via_acs_smtp",
        fake_send,
    )

    response = client.post(
        "/password/forgot",
        json={"email": "resetme@example.com"},
    )

    assert response.status_code == 200
    assert "link" in captured
    assert captured["to_email"] == "resetme@example.com"

    parsed = urlparse(captured["link"])
    token = parse_qs(parsed.query)["token"][0]
    assert parsed.scheme == "https"
    assert parsed.netloc == "frontend.local"
    assert parsed.path == "/reset-password"

    db_session.expire_all()
    stored = db_session.execute(select(PasswordResetToken)).scalar_one()
    assert stored.user_id == user.id
    assert stored.token_hash == hash_token(token)


def test_reset_password_updates_hash_and_marks_token_used(client, db_session):
    user = User(email="resetuser@example.com", password_hash=None, role="user")
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    token, raw_token = password_reset_service.create_password_reset_token(
        db_session,
        user,
        ttl_minutes=60,
    )

    response = client.post(
        "/password/reset",
        json={"token": raw_token, "password": "new-password"},
    )

    assert response.status_code == 200
    assert response.json() == {"message": "Password updated"}

    db_session.refresh(user)
    assert user.password_hash is not None
    assert verify_password("new-password", user.password_hash)

    db_session.refresh(token)
    assert token.used_at is not None


def test_reset_password_rejects_expired_or_used_token(client, db_session):
    user = User(email="expired@example.com", password_hash=None, role="user")
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    token, raw_token = password_reset_service.create_password_reset_token(
        db_session,
        user,
        ttl_minutes=60,
    )
    token.expires_at = datetime.now(UTC) - timedelta(minutes=1)
    db_session.commit()

    expired_response = client.post(
        "/password/reset",
        json={"token": raw_token, "password": "new-password"},
    )
    assert expired_response.status_code == 400

    token_two, raw_token_two = password_reset_service.create_password_reset_token(
        db_session,
        user,
        ttl_minutes=60,
    )
    token_two.used_at = datetime.now(UTC)
    db_session.commit()

    used_response = client.post(
        "/password/reset",
        json={"token": raw_token_two, "password": "new-password"},
    )
    assert used_response.status_code == 400
