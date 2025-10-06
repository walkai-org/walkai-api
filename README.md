# walk:ai-api

FastAPI service powering the walk:ai backend APIs.

## Getting Started

1. Start a local Redis instance (required for state storage): `docker run -d --rm --name redis -p 6379:6379 redis:latest`
2. Sync dependencies with [uv](https://github.com/astral-sh/uv): `uv sync`
3. Launch the development server: `uv run uvicorn app.main:app --reload`

## Configuration

- There's an 

## Environment Variables

Copy `.env.example` to `.env` and provide the values required by your deployment.

Store the real secrets only in `.env`; avoid committing them to version control.
