from collections.abc import Generator
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Session, sessionmaker

from app.core.config import get_settings

settings = get_settings()

CONNECTION_URL = settings.database_url

connection_url = make_url(CONNECTION_URL)
engine_kwargs: dict[str, Any] = {"pool_pre_ping": True}
connect_args: dict[str, Any] = {}

if connection_url.drivername.startswith("sqlite"):
    # Relax SQLite's default thread check so the same connection can be reused across requests.
    connect_args["check_same_thread"] = False
else:
    engine_kwargs.update(
        {
            "pool_size": 5,
            "max_overflow": 10,
            "pool_recycle": 1800,
            "pool_timeout": 30,
        }
    )
    connect_args["connect_timeout"] = 5

if connect_args:
    engine_kwargs["connect_args"] = connect_args

engine = create_engine(CONNECTION_URL, **engine_kwargs)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def get_db() -> Generator[Session]:
    session = SessionLocal()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def ping_database() -> bool:
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True
    except SQLAlchemyError:
        return False


class Base(MappedAsDataclass, DeclarativeBase):
    pass
