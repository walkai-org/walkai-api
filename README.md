# walk:ai-api

FastAPI service powering the walk:ai backend APIs.

## Getting Started

1. Start a local Redis instance (required for state storage): `docker run -d --name redis -p 6379:6379 redis:latest`
2. Sync dependencies with [uv](https://github.com/astral-sh/uv): `uv sync`
3. Launch the development server: `uv run uvicorn app.main:app --reload`

## Configuration

- `APP_ENV`: specifies the active environment; defaults to `development`. Use `production` for production-oriented defaults.
- `SQLITE_DB_PATH`: optional path to the SQLite database file. When omitted, it falls back to `data/walkai_dev.db` or `data/walkai_prod.db` depending on `APP_ENV`.

The database path is resolved automatically and its parent directory is created if missing. The `/health` endpoint reports the current environment and confirms connectivity to SQLite.

## Environment Variables

Copy `.env.example` to `.env` and provide the values required by your deployment.

Store the real secrets only in `.env`; avoid committing them to version control.
