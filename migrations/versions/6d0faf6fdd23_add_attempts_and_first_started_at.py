"""add attempts count and first started timestamp to job runs

Revision ID: 6d0faf6fdd23
Revises: 0a6d28cde5d1
Create Date: 2025-02-15 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import column, table

# revision identifiers, used by Alembic.
revision = "6d0faf6fdd23"
down_revision = "0a6d28cde5d1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "job_runs",
        sa.Column("attempts", sa.Integer(), server_default="1", nullable=False),
    )
    op.add_column(
        "job_runs",
        sa.Column("first_started_at", sa.DateTime(), nullable=True),
    )

    job_runs = table(
        "job_runs",
        column("id", sa.Integer()),
        column("started_at", sa.DateTime()),
        column("first_started_at", sa.DateTime()),
        column("attempts", sa.Integer()),
    )

    op.execute(job_runs.update().values(first_started_at=job_runs.c.started_at))
    op.execute(
        job_runs.update().where(job_runs.c.attempts.is_(None)).values(attempts=1)
    )


def downgrade() -> None:
    op.drop_column("job_runs", "first_started_at")
    op.drop_column("job_runs", "attempts")
