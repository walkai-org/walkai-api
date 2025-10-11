import os
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.security import create_access
from app.models.users import User

temp_dir = Path(tempfile.mkdtemp(prefix="walkai-test-"))
os.environ["APP_ENV"] = "test"
os.environ["SQLITE_DB_PATH"] = str(temp_dir / "test.db")
os.environ["JWT_SECRET"] = "test-jwt-secret"
os.environ["JWT_ALGO"] = "HS256"
os.environ["ACCESS_MIN"] = "15"
os.environ["REDIS_URL"] = "redis://localhost:6379/0"
os.environ["CLUSTER_TOKEN"] = "test-cluster-token"
os.environ["CLUSTER_URL"] = "https://example.com"

from app.core import config as app_config  # noqa: E402

app_config.get_settings.cache_clear()

from app.core.database import Base, SessionLocal, engine  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture(autouse=True)
def reset_database():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def db_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture
def test_user(db_session) -> User:
    u = User(email="test@example.com", password_hash=None, role="admin")
    db_session.add(u)
    db_session.commit()
    db_session.refresh(u)
    return u


@pytest.fixture
def auth_client(client, test_user) -> tuple[TestClient, User]:
    token = create_access(str(test_user.id), test_user.role)
    client.cookies.set("access_token", token, path="/")
    return client, test_user


def create_test_user(db) -> User:
    user = User(email="user@example.com", password_hash=None, role="admin")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user
