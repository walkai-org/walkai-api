#!/usr/bin/env sh
set -e

# Apply DB migrations before starting the API
uv run alembic upgrade head

exec uv run uvicorn app.main:app --host 0.0.0.0 --port 8000
