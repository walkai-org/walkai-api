from collections.abc import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Session, sessionmaker

from app.core.config import get_settings

settings = get_settings()

CONNECTION_URL = settings.database_url

engine = create_engine(
    CONNECTION_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=1800,
    pool_timeout=30,
    connect_args={"connect_timeout": 5},
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def get_db() -> Generator[Session, None, None]:
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
