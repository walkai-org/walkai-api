from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import expression

revision = "0001_initial_schema"
down_revision = None
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None


def upgrade() -> None:
    volume_state_enum = sa.Enum(
        "pvc",
        "stored",
        "deleted",
        name="volumestate",
    )
    gpu_profile_enum = sa.Enum(
        "1g.10gb",
        "2g.20gb",
        "3g.40gb",
        "4g.40gb",
        "7g.79gb",
        name="gpuprofile",
    )
    run_status_enum = sa.Enum(
        "pending",
        "scheduled",
        "active",
        "succeeded",
        "failed",
        "cancelled",
        name="runstatus",
    )

    bind = op.get_bind()
    volume_state_enum.create(bind, checkfirst=True)
    gpu_profile_enum.create(bind, checkfirst=True)
    run_status_enum.create(bind, checkfirst=True)

    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("email", sa.String(), nullable=False, unique=True),
        sa.Column("password_hash", sa.String(), nullable=True),
        sa.Column("role", sa.String(), nullable=False, server_default="admin"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )

    op.create_table(
        "user_invitations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("email", sa.String(), nullable=False),
        sa.Column("token_hash", sa.String(), nullable=False, unique=True),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("invited_by", sa.String(), nullable=True),
        sa.Column("used_at", sa.DateTime(), nullable=True),
    )
    op.create_index(
        "ix_user_invitations_email",
        "user_invitations",
        ["email"],
        unique=False,
    )

    op.create_table(
        "volumes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("pvc_name", sa.String(), nullable=False),
        sa.Column("size", sa.Integer(), nullable=False),
        sa.Column("key_prefix", sa.String(), nullable=True),
        sa.Column(
            "is_input",
            sa.Boolean(),
            nullable=False,
            server_default=expression.false(),
        ),
        sa.Column(
            "state",
            volume_state_enum,
            nullable=False,
        ),
    )

    op.create_table(
        "jobs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("image", sa.String(), nullable=False),
        sa.Column("gpu_profile", gpu_profile_enum, nullable=False),
        sa.Column(
            "submitted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "created_by_id",
            sa.Integer(),
            sa.ForeignKey("users.id"),
            nullable=False,
        ),
        sa.Column("k8s_job_name", sa.String(), nullable=False),
    )

    op.create_table(
        "social_identities",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("users.id"),
            nullable=False,
        ),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("provider_user_id", sa.String(), nullable=False),
        sa.Column(
            "email_verified",
            sa.Boolean(),
            nullable=False,
            server_default=expression.false(),
        ),
        sa.UniqueConstraint(
            "provider",
            "provider_user_id",
            name="uq_provider_sub",
        ),
    )
    op.create_index(
        "ix_social_identities_user_id",
        "social_identities",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        "ix_social_identities_provider",
        "social_identities",
        ["provider"],
        unique=False,
    )
    op.create_index(
        "ix_social_identities_provider_user_id",
        "social_identities",
        ["provider_user_id"],
        unique=False,
    )

    op.create_table(
        "personal_access_tokens",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("users.id"),
            nullable=False,
        ),
        sa.Column("token_hash", sa.String(), nullable=False, unique=True),
        sa.Column("token_prefix", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("last_used_at", sa.DateTime(), nullable=True),
    )
    op.create_index(
        "ix_personal_access_tokens_user_id",
        "personal_access_tokens",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        "ix_personal_access_tokens_token_prefix",
        "personal_access_tokens",
        ["token_prefix"],
        unique=False,
    )

    op.create_table(
        "job_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "job_id",
            sa.Integer(),
            sa.ForeignKey("jobs.id"),
            nullable=False,
        ),
        sa.Column("status", run_status_enum, nullable=False),
        sa.Column("run_token", sa.String(), nullable=False),
        sa.Column(
            "output_volume_id",
            sa.Integer(),
            sa.ForeignKey("volumes.id"),
            nullable=False,
        ),
        sa.Column(
            "k8s_pod_name",
            sa.String(),
            nullable=True,
        ),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("exit_code", sa.Integer(), nullable=True),
        sa.Column(
            "input_volume_id",
            sa.Integer(),
            sa.ForeignKey("volumes.id"),
            nullable=True,
        ),
        sa.CheckConstraint(
            "input_volume_id IS NULL OR input_volume_id <> output_volume_id",
            name="ck_job_input_output_distinct",
        ),
    )


def downgrade() -> None:
    op.drop_table("job_runs")

    op.drop_index(
        "ix_personal_access_tokens_token_prefix",
        table_name="personal_access_tokens",
    )
    op.drop_index(
        "ix_personal_access_tokens_user_id",
        table_name="personal_access_tokens",
    )
    op.drop_table("personal_access_tokens")

    op.drop_index(
        "ix_social_identities_provider_user_id",
        table_name="social_identities",
    )
    op.drop_index(
        "ix_social_identities_provider",
        table_name="social_identities",
    )
    op.drop_index(
        "ix_social_identities_user_id",
        table_name="social_identities",
    )
    op.drop_table("social_identities")

    op.drop_table("jobs")

    op.drop_table("volumes")

    op.drop_index(
        "ix_user_invitations_email",
        table_name="user_invitations",
    )
    op.drop_table("user_invitations")

    op.drop_table("users")

    bind = op.get_bind()
    run_status_enum = sa.Enum(
        "pending",
        "scheduled",
        "active",
        "succeeded",
        "failed",
        "cancelled",
        name="runstatus",
    )
    gpu_profile_enum = sa.Enum(
        "1g.10gb",
        "2g.20gb",
        "3g.40gb",
        "4g.40gb",
        "7g.79gb",
        name="gpuprofile",
    )
    volume_state_enum = sa.Enum(
        "pvc",
        "stored",
        "deleted",
        name="volumestate",
    )
    run_status_enum.drop(bind, checkfirst=True)
    gpu_profile_enum.drop(bind, checkfirst=True)
    volume_state_enum.drop(bind, checkfirst=True)
