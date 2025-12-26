"""add terminating run status

Revision ID: bc7d2f0d1c5e
Revises: 6d0faf6fdd23
Create Date: 2025-02-20 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "bc7d2f0d1c5e"
down_revision = "6d0faf6fdd23"
branch_labels = None
depends_on = None

_OLD_RUNSTATUS_VALUES = (
    "pending",
    "scheduled",
    "active",
    "succeeded",
    "failed",
    "cancelled",
)
_RUNSTATUS_VALUES = (*_OLD_RUNSTATUS_VALUES, "terminating")


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(
            sa.text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_enum
                        JOIN pg_type ON pg_enum.enumtypid = pg_type.oid
                        WHERE pg_type.typname = 'runstatus'
                          AND pg_enum.enumlabel = 'terminating'
                    ) THEN
                        ALTER TYPE runstatus ADD VALUE 'terminating';
                    END IF;
                END$$;
                """
            )
        )
        return

    with op.batch_alter_table("job_runs", schema=None) as batch_op:
        batch_op.alter_column(
            "status",
            existing_type=sa.Enum(*_OLD_RUNSTATUS_VALUES, name="runstatus"),
            type_=sa.Enum(*_RUNSTATUS_VALUES, name="runstatus"),
            existing_nullable=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        has_terminating = bind.execute(
            sa.text("SELECT 1 FROM job_runs WHERE status = 'terminating' LIMIT 1")
        ).first()
        if has_terminating:
            raise RuntimeError(
                "Cannot downgrade runstatus enum while 'terminating' values exist."
            )

        op.execute("ALTER TYPE runstatus RENAME TO runstatus_old")
        op.execute(
            "CREATE TYPE runstatus AS ENUM "
            "('pending', 'scheduled', 'active', 'succeeded', 'failed', 'cancelled')"
        )
        op.execute(
            "ALTER TABLE job_runs ALTER COLUMN status TYPE runstatus "
            "USING status::text::runstatus"
        )
        op.execute("DROP TYPE runstatus_old")
        return

    with op.batch_alter_table("job_runs", schema=None) as batch_op:
        batch_op.alter_column(
            "status",
            existing_type=sa.Enum(*_RUNSTATUS_VALUES, name="runstatus"),
            type_=sa.Enum(*_OLD_RUNSTATUS_VALUES, name="runstatus"),
            existing_nullable=False,
        )
