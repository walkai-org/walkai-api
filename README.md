# walk:ai-api

FastAPI service powering the walk:ai backend APIs.

## Getting Started

1. Sync dependencies with [uv](https://github.com/astral-sh/uv): `uv sync`
2. Start the development server through uv: `uv run uvicorn app.main:app --reload`

## Configuration

- `APP_ENV`: controls the active environment; defaults to `development`. Set to `production` for production defaults.
- `DATABASE_URL`: full SQLAlchemy connection string. Overrides all other defaults when provided.
- `SQLITE_DB_PATH`: optional path to the SQLite database file when `DATABASE_URL` is not set. Defaults to `data/walkai_dev.db` or `data/walkai_prod.db` based on `APP_ENV`.

The `/health` endpoint reports the current environment and whether the application can reach the configured database.
