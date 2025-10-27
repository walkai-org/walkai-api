from collections.abc import Generator

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
        session.close()
        trans.rollback()
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
