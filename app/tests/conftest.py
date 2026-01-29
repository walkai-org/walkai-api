import os
from collections.abc import Generator

os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("JWT_SECRET", "test-secret")
os.environ.setdefault("JWT_ALGO", "HS256")
os.environ.setdefault("ACCESS_MIN", "15")
os.environ.setdefault("CLUSTER_TOKEN", "test-cluster-token")
os.environ.setdefault("CLUSTER_URL", "https://cluster.local")
os.environ.setdefault("API_BASE_URL", "https://api.local")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test-access-key")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test-secret-key")
os.environ.setdefault("AWS_REGION", "us-test-1")
os.environ.setdefault("AWS_S3_BUCKET", "test-bucket")
os.environ.setdefault(
    "DATABASE_URL", "postgresql+psycopg://test:test@localhost:5432/testdb"
)
os.environ.setdefault("ECR_ARN", "arn:aws:ecr:us-test-1:123456789012:repository/test")
os.environ.setdefault("DYNAMODB_OAUTH_TABLE", "walkai-test-oauth")
os.environ.setdefault("DYNAMODB_CLUSTER_CACHE_TABLE", "walkai-test-cluster-cache")
os.environ.setdefault("INVITE_BASE_URL", "https://frontend.local/invitations/accept")

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import StaticPool, create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.database import Base, get_db
from app.core.security import create_access
from app.main import app
from app.models.users import User


@pytest.fixture(scope="session")
def engine():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(engine) -> Generator[Session]:
    connection = engine.connect()
    trans = connection.begin()

    TestingSessionLocal = sessionmaker(
        bind=connection, autoflush=False, expire_on_commit=False, future=True
    )
    session = TestingSessionLocal()

    try:
        yield session
    finally:
        trans.rollback()
        session.close()
        connection.close()


@pytest.fixture
def client(db_session) -> Generator[TestClient]:
    # Override FastAPI's get_db to use our testing session
    def _override_get_db():
        try:
            yield db_session
        finally:
            pass

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def test_user(db_session) -> "User":
    from app.models.users import User

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
