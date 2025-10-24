# Repository Guidelines

## Project Structure & Module Organization
- `app/main.py` runs the FastAPI application; feature routers live under `app/api`.
- `app/core` centralizes settings and startup hooks; update config there before touching entrypoints.
- `app/models` contains SQLAlchemy models kept in sync with `app/schemas` for request/response shapes.
- `app/services` holds domain logic and integrations; reuse services instead of hitting Redis or Kubernetes directly in routes.
- Tests sit in `app/tests`; seed data and local state live under `data/`, with cluster manifests in `secret.yml` and `mig-values.yml`.

## Build, Test & Development Commands
- `uv sync` installs dependencies declared in `pyproject.toml`/`uv.lock`.
- `uv run uvicorn app.main:app --reload` starts the API with hot reload for local dev.
- `uv run pytest` executes the test suite; pair with `-k <pattern>` or `-s` when debugging. Dont use UV_CACHE_DIR.
- `uv run ruff check` (and `uv run ruff format` before committing) enforces linting and import order.

## Coding Style & Naming Conventions
- Target Python 3.13, 4-space indentation, and full type hints on public functions.
- Use `snake_case` for modules, functions, and variables; reserve `PascalCase` for classes and Pydantic models.
- Keep FastAPI routers focused on orchestration; push validation and side effects into `app.services` helpers.
- Update matching Pydantic schemas and ORM models together to avoid drift.

## Testing Guidelines
- Place tests in `app/tests` with files named `test_*.py` and functions `test_<behavior>`.
- Reuse fixtures from `app/tests/conftest.py`; prefer FastAPI's `TestClient` over direct service calls.
- Mock Redis and Kubernetes clients so tests stay deterministic; document new fixtures in module docstrings.

## Commit & Pull Request Guidelines
- Write imperative, ~50-character subjects (e.g., `add tests`), expanding on context in the body as needed.
- Link related issues and call out schema or infrastructure changes explicitly.
- Run `uv run pytest` and `uv run ruff check` before pushing; include results or screenshots when PRs alter APIs.
- Request reviews from API and platform maintainers, and add sample payloads for new or changed endpoints.

## Environment & Deployment Notes
- Copy `.env.example` to `.env` for local secrets; keep real credentials out of git.
- Start Redis locally via `docker run -d --rm --name redis -p 6379:6379 redis:latest` before hitting stateful flows.
- Use the Minikube instructions in `README.md` for cluster validation; apply `secret.yml` and `mig-values.yml` when simulating GPUs.
