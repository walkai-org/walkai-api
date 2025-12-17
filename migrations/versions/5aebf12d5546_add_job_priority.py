"""add job priority

Revision ID: 5aebf12d5546
Revises: b4422e1d059d
Create Date: 2025-12-08 18:23:50.087246
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "5aebf12d5546"
down_revision = "b4422e1d059d"
branch_labels = None
depends_on = None

# Define the enum type explicitly for Postgres
jobpriority_enum = postgresql.ENUM(
    "low",
    "medium",
    "high",
    "extra-high",
    name="jobpriority",
)


def upgrade() -> None:
    bind = op.get_bind()

    # 1) Create the enum type in the DB (no-op if already exists)
    jobpriority_enum.create(bind, checkfirst=True)

    # 2) Apply the table changes
    with op.batch_alter_table("jobs", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "priority_class",
                jobpriority_enum,
                server_default="medium",
                nullable=False,
            )
        )
        batch_op.alter_column(
            "gpu_profile",
            existing_type=sa.VARCHAR(length=5),
            type_=sa.Enum(
                "1g.10gb",
                "2g.20gb",
                "3g.40gb",
                "4g.40gb",
                "7g.79gb",
                name="gpuprofile",
            ),
            existing_nullable=False,
        )


def downgrade() -> None:
    bind = op.get_bind()

    with op.batch_alter_table("jobs", schema=None) as batch_op:
        batch_op.alter_column(
            "gpu_profile",
            existing_type=sa.Enum(
                "1g.10gb",
                "2g.20gb",
                "3g.40gb",
                "4g.40gb",
                "7g.79gb",
                name="gpuprofile",
            ),
            type_=sa.VARCHAR(length=5),
            existing_nullable=False,
        )
        batch_op.drop_column("priority_class")

    # Drop the enum type (only if nothing else uses it)
    jobpriority_enum.drop(bind, checkfirst=True)
