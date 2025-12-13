"""add job schedules

Revision ID: c48b5adf4c2a
Revises: 5aebf12d5546
Create Date: 2025-02-05 00:00:00.000000
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "c48b5adf4c2a"
down_revision = "5aebf12d5546"
branch_labels = None
depends_on = None

schedulekind_enum = postgresql.ENUM(
    "once",
    "cron",
    name="schedulekind",
)


def upgrade() -> None:
    bind = op.get_bind()
    schedulekind_enum.create(bind, checkfirst=True)

    op.create_table(
        "job_schedules",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("job_id", sa.Integer(), sa.ForeignKey("jobs.id"), nullable=False),
        sa.Column("kind", schedulekind_enum, nullable=False),
        sa.Column("run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cron", sa.String(length=255), nullable=True),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            server_onupdate=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(kind = 'once' AND run_at IS NOT NULL AND cron IS NULL) OR "
            "(kind = 'cron' AND cron IS NOT NULL)",
            name="ck_job_schedules_kind_fields",
        ),
    )
    op.create_index(
        "ix_job_schedules_next_run_at",
        "job_schedules",
        ["next_run_at"],
    )


def downgrade() -> None:
    bind = op.get_bind()
    op.drop_index(
        "ix_job_schedules_next_run_at",
        table_name="job_schedules",
    )
    op.drop_table("job_schedules")
    schedulekind_enum.drop(bind, checkfirst=True)
