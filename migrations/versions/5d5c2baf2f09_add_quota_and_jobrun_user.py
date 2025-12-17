"""add quota tracking and job run ownership

Revision ID: 5d5c2baf2f09
Revises: 222d527f382b
Create Date: 2025-02-10 00:00:00.000000
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import column, table

# revision identifiers, used by Alembic.
revision = "5d5c2baf2f09"
down_revision = "222d527f382b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    op.add_column(
        "users",
        sa.Column(
            "high_priority_quota_minutes",
            sa.Integer(),
            server_default="180",
            nullable=False,
        ),
    )
    op.add_column(
        "job_runs",
        sa.Column("user_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "job_runs",
        sa.Column(
            "billable_minutes",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "job_runs",
        sa.Column(
            "is_scheduled",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )
    if bind.dialect.name == "sqlite":
        with op.batch_alter_table("job_runs", recreate="always") as batch_op:
            batch_op.create_foreign_key(
                "fk_job_runs_user_id_users",
                referent_table="users",
                local_cols=["user_id"],
                remote_cols=["id"],
            )
    else:
        op.create_foreign_key(
            "fk_job_runs_user_id_users",
            "job_runs",
            "users",
            ["user_id"],
            ["id"],
        )

    # Backfill user_id from jobs.created_by_id
    job_runs = table(
        "job_runs",
        column("id", sa.Integer),
        column("job_id", sa.Integer),
        column("user_id", sa.Integer),
        column("started_at", sa.DateTime(timezone=True)),
        column("finished_at", sa.DateTime(timezone=True)),
    )

    jobs_table = sa.table(
        "jobs", sa.column("id", sa.Integer), sa.column("created_by_id", sa.Integer)
    )

    # Update user_id with a single SQL that works on both Postgres and SQLite
    update_user_stmt = (
        job_runs.update()
        .values(
            user_id=sa.select(jobs_table.c.created_by_id)
            .where(jobs_table.c.id == job_runs.c.job_id)
            .scalar_subquery()
        )
        .where(job_runs.c.user_id.is_(None))
    )
    bind.execute(update_user_stmt)

    # Backfill billable_minutes in Python for portability across dialects
    select_stmt = sa.select(
        job_runs.c.id,
        job_runs.c.started_at,
        job_runs.c.finished_at,
    ).where(
        job_runs.c.started_at.is_not(None),
        job_runs.c.finished_at.is_not(None),
    )
    results = bind.execute(select_stmt).all()
    for run_id, started_at, finished_at in results:
        seconds = (finished_at - started_at).total_seconds()
        minutes = max(0, int((seconds + 59) // 60))  # ceil without import
        update_stmt = (
            job_runs.update()
            .where(job_runs.c.id == run_id)
            .values(billable_minutes=minutes)
        )
        bind.execute(update_stmt)


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        with op.batch_alter_table("job_runs", recreate="always") as batch_op:
            batch_op.drop_constraint("fk_job_runs_user_id_users", type_="foreignkey")
            batch_op.drop_column("is_scheduled")
            batch_op.drop_column("billable_minutes")
            batch_op.drop_column("user_id")
        with op.batch_alter_table("users", recreate="always") as batch_op:
            batch_op.drop_column("high_priority_quota_minutes")
    else:
        op.drop_constraint("fk_job_runs_user_id_users", "job_runs", type_="foreignkey")
        op.drop_column("job_runs", "is_scheduled")
        op.drop_column("job_runs", "billable_minutes")
        op.drop_column("job_runs", "user_id")
        op.drop_column("users", "high_priority_quota_minutes")
