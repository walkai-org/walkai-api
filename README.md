# walk:ai-api

FastAPI service powering the walk:ai backend APIs.

## Getting Started

1. Sync dependencies with [uv](https://github.com/astral-sh/uv): `uv sync`
2. Launch the development server: `uv run uvicorn app.main:app --reload`

## Configuration

- `APP_ENV`: specifies the active environment; defaults to `development`. Use `production` for production-oriented defaults.
- `SQLITE_DB_PATH`: optional path to the SQLite database file. When omitted, it falls back to `data/walkai_dev.db` or `data/walkai_prod.db` depending on `APP_ENV`.

The database path is resolved automatically and its parent directory is created if missing. The `/health` endpoint reports the current environment and confirms connectivity to SQLite.
