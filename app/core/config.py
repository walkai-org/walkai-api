import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    environment: str
    sqlite_path: Path

    @property
    def database_path(self) -> str:
        """Return the resolved sqlite database path as a string."""
        return str(self.sqlite_path)


def _resolve_sqlite_path(environment: str) -> Path:
    default_name = "walkai_prod.db" if environment == "production" else "walkai_dev.db"
    relative_path = Path(os.getenv("SQLITE_DB_PATH", f"data/{default_name}"))

    if not relative_path.is_absolute():
        project_root = Path(__file__).resolve().parent.parent
        sqlite_path = project_root / relative_path
    else:
        sqlite_path = relative_path

    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite_path


@lru_cache
def get_settings() -> Settings:
    environment = os.getenv("APP_ENV", "development")
    sqlite_path = _resolve_sqlite_path(environment)
    return Settings(environment=environment, sqlite_path=sqlite_path)
