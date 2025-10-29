from __future__ import annotations

from uuid import uuid4

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import text

revision = "20241209_move_k8s_job_name_to_job_runs"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "job_runs",
        sa.Column("k8s_job_name", sa.String(length=255), nullable=True),
    )

    bind = op.get_bind()
    bind.execute(
        text(
            """
            UPDATE job_runs
            SET k8s_job_name = (
                SELECT jobs.k8s_job_name
                FROM jobs
                WHERE jobs.id = job_runs.job_id
            )
            WHERE k8s_job_name IS NULL
            """
        )
    )

    op.alter_column(
        "job_runs",
        "k8s_job_name",
        existing_type=sa.String(length=255),
        nullable=False,
    )

    op.drop_column("jobs", "k8s_job_name")


def downgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column("k8s_job_name", sa.String(length=255), nullable=True),
    )

    bind = op.get_bind()
    bind.execute(
        text(
            """
            UPDATE jobs
            SET k8s_job_name = (
                SELECT job_runs.k8s_job_name
                FROM job_runs
                WHERE job_runs.job_id = jobs.id
                ORDER BY job_runs.id ASC
                LIMIT 1
            )
            WHERE k8s_job_name IS NULL
            """
        )
    )

    missing = bind.execute(
        text("SELECT id FROM jobs WHERE k8s_job_name IS NULL")
    ).fetchall()
    for row in missing:
        bind.execute(
            text("UPDATE jobs SET k8s_job_name = :name WHERE id = :job_id"),
            {"name": uuid4().hex, "job_id": row.id},
        )

    op.alter_column(
        "jobs",
        "k8s_job_name",
        existing_type=sa.String(length=255),
        nullable=False,
    )

    op.drop_column("job_runs", "k8s_job_name")
