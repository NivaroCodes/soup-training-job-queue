"""PostgreSQL-backed SQLAlchemy 2.x ORM models for jobs and execution effects."""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class JobStatus(StrEnum):
    """Permitted states for a training job."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Job(Base):
    """Authoritative training-job state, lease, fencing, and cancellation record."""

    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=JobStatus.QUEUED.value,
        server_default=text("'queued'"),
    )
    attempt: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=3,
        server_default=text("3"),
    )
    fencing_token: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    worker_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    cancel_requested: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    cancel_requested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("clock_timestamp()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("clock_timestamp()"),
        onupdate=func.clock_timestamp(),
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    cancelled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    execution_effect: Mapped[ExecutionEffect | None] = relationship(
        back_populates="job",
        uselist=False,
    )

    @hybrid_property
    def finished_at(self) -> datetime | None:
        """Return terminal timestamp from completed_at or cancelled_at."""
        return self.completed_at if self.completed_at is not None else self.cancelled_at

    @finished_at.inplace.expression
    @classmethod
    def _finished_at_expression(cls) -> Any:
        return func.coalesce(cls.completed_at, cls.cancelled_at)

    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_jobs_idempotency_key"),
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
            name="valid_status",
        ),
        CheckConstraint("attempt >= 0", name="attempt_non_negative"),
        CheckConstraint("max_attempts >= 1", name="max_attempts_positive"),
        CheckConstraint("attempt <= max_attempts", name="attempt_within_max"),
        CheckConstraint("fencing_token >= 0", name="fencing_token_non_negative"),
        CheckConstraint(
            "length(btrim(idempotency_key)) > 0",
            name="idempotency_key_non_empty",
        ),
        CheckConstraint(
            "length(btrim(request_hash)) > 0",
            name="request_hash_non_empty",
        ),
        Index(
            "ix_jobs_queued_poll",
            "created_at",
            "id",
            postgresql_where=text("status = 'queued'"),
        ),
        Index(
            "ix_jobs_running_lease_recovery",
            "lease_expires_at",
            "id",
            postgresql_where=text("status = 'running'"),
        ),
        Index("ix_jobs_created_at_id", "created_at", "id"),
        Index("ix_jobs_status_created_at_id", "status", "created_at", "id"),
    )


class ExecutionEffect(Base):
    """At-most-one committed measurable database side effect per job."""

    __tablename__ = "execution_effects"

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    worker_id: Mapped[str] = mapped_column(String(255), nullable=False)
    fencing_token: Mapped[int] = mapped_column(BigInteger, nullable=False)
    effect_payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("clock_timestamp()"),
    )

    job: Mapped[Job] = relationship(back_populates="execution_effect")

    __table_args__ = (
        UniqueConstraint("job_id", name="uq_execution_effects_job_id"),
        CheckConstraint("fencing_token >= 1", name="fencing_token_positive"),
        CheckConstraint(
            "length(btrim(worker_id)) > 0",
            name="worker_id_non_empty",
        ),
    )
