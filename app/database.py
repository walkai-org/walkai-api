import sqlite3
from contextlib import contextmanager
from typing import Iterator

from .config import get_settings

settings = get_settings()


def _connect() -> sqlite3.Connection:
    """Create a new sqlite connection using the configured database path."""
    return sqlite3.connect(settings.database_path)


def ping_database() -> bool:
    try:
        with _connect() as connection:
            connection.execute("SELECT 1")
        return True
    except sqlite3.Error:
        return False


@contextmanager
def get_connection() -> Iterator[sqlite3.Connection]:
    connection = _connect()
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
