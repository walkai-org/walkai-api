# walk:ai-api

FastAPI service powering the walk:ai backend APIs.

## Getting Started

1. Create a virtual environment: `python -m venv .venv`
2. Activate it and install dependencies: `pip install -r requirements.txt`
3. Run the development server: `uvicorn app.main:app --reload`

The API exposes a `/health` endpoint for basic availability checks.