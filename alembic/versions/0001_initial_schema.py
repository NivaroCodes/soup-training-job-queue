"""Create jobs and execution_effects tables.

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-11-12
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0001_initial_schema"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create jobs and execution_effects tables with all constraints and indexes."""
    op.create_table(
        "jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.String(length=32),
            server_default=sa.text("'queued'"),
            nullable=False,
        ),
        sa.Column(
            "attempt",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "max_attempts",
            sa.Integer(),
            server_default=sa.text("3"),
            nullable=False,
        ),
        sa.Column(
            "fencing_token",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("worker_id", sa.String(length=255), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "cancel_requested",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_jobs"),
        sa.UniqueConstraint("idempotency_key", name="uq_jobs_idempotency_key"),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
            name="valid_status",
        ),
        sa.CheckConstraint("attempt >= 0", name="attempt_non_negative"),
        sa.CheckConstraint("max_attempts >= 1", name="max_attempts_positive"),
        sa.CheckConstraint(
            "attempt <= max_attempts",
            name="attempt_within_max",
        ),
        sa.CheckConstraint(
            "fencing_token >= 0",
            name="fencing_token_non_negative",
        ),
        sa.CheckConstraint(
            "length(btrim(idempotency_key)) > 0",
            name="idempotency_key_non_empty",
        ),
        sa.CheckConstraint(
            "length(btrim(request_hash)) > 0",
            name="request_hash_non_empty",
        ),
    )

    op.create_index(
        "ix_jobs_queued_poll",
        "jobs",
        ["created_at", "id"],
        unique=False,
        postgresql_where=sa.text("status = 'queued'"),
    )
    op.create_index(
        "ix_jobs_running_lease_recovery",
        "jobs",
        ["lease_expires_at", "id"],
        unique=False,
        postgresql_where=sa.text("status = 'running'"),
    )
    op.create_index(
        "ix_jobs_created_at_id",
        "jobs",
        ["created_at", "id"],
        unique=False,
    )
    op.create_index(
        "ix_jobs_status_created_at_id",
        "jobs",
        ["status", "created_at", "id"],
        unique=False,
    )

    op.create_table(
        "execution_effects",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("worker_id", sa.String(length=255), nullable=False),
        sa.Column("fencing_token", sa.BigInteger(), nullable=False),
        sa.Column(
            "effect_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
            name="fk_execution_effects_job_id_jobs",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_execution_effects"),
        sa.UniqueConstraint("job_id", name="uq_execution_effects_job_id"),
        sa.CheckConstraint(
            "fencing_token >= 1",
            name="fencing_token_positive",
        ),
        sa.CheckConstraint(
            "length(btrim(worker_id)) > 0",
            name="worker_id_non_empty",
        ),
    )


def downgrade() -> None:
    """Drop execution_effects and jobs tables and indexes."""
    op.drop_table("execution_effects")
    op.drop_index("ix_jobs_status_created_at_id", table_name="jobs")
    op.drop_index("ix_jobs_created_at_id", table_name="jobs")
    op.drop_index(
        "ix_jobs_running_lease_recovery",
        table_name="jobs",
        postgresql_where=sa.text("status = 'running'"),
    )
    op.drop_index(
        "ix_jobs_queued_poll",
        table_name="jobs",
        postgresql_where=sa.text("status = 'queued'"),
    )
    op.drop_table("jobs")
