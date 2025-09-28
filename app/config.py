from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    environment: str
    database_url: str

    @property
    def is_sqlite(self) -> bool:
        return self.database_url.startswith("sqlite")


def _build_default_sqlite_url(environment: str) -> str:
    default_name = "walkai_prod.db" if environment == "production" else "walkai_dev.db"
    relative_path = os.getenv("SQLITE_DB_PATH", f"data/{default_name}")
    db_path = Path(relative_path)

    if not db_path.is_absolute():
        project_root = Path(__file__).resolve().parent.parent
        db_path = project_root / db_path

    db_path.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{db_path}"


@lru_cache
def get_settings() -> Settings:
    environment = os.getenv("APP_ENV", "development")
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        database_url = _build_default_sqlite_url(environment)

    return Settings(environment=environment, database_url=database_url)
