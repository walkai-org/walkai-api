"""add quota usage counter and reset window

Revision ID: 0a6d28cde5d1
Revises: 5d5c2baf2f09
Create Date: 2025-02-11 00:00:00.000000
"""

from __future__ import annotations

import datetime

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0a6d28cde5d1"
down_revision = "5d5c2baf2f09"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column(
            "high_priority_minutes_used",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "users",
        sa.Column(
            "quota_resets_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )

    bind = op.get_bind()
    users = sa.table(
        "users",
        sa.column("id", sa.Integer),
        sa.column("quota_resets_at", sa.DateTime(timezone=True)),
    )

    now = datetime.datetime.now(datetime.UTC)
    reset_at = now + datetime.timedelta(days=7)
    bind.execute(users.update().values(quota_resets_at=reset_at))


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        with op.batch_alter_table("users", recreate="always") as batch_op:
            batch_op.drop_column("quota_resets_at")
            batch_op.drop_column("high_priority_minutes_used")
    else:
        op.drop_column("users", "quota_resets_at")
        op.drop_column("users", "high_priority_minutes_used")
