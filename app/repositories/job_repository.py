"""PostgreSQL repository operations for Job records."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session, selectinload

from app.db.models import Job, JobStatus


class JobRepository:
    """Encapsulates SQLAlchemy queries and conditional updates for jobs."""

    @staticmethod
    def get_by_id(session: Session, job_id: uuid.UUID) -> Job | None:
        """Fetch a job by UUID with its execution effect if present."""
        stmt = (
            select(Job)
            .options(selectinload(Job.execution_effect))
            .where(Job.id == job_id)
        )
        return session.scalars(stmt).first()

    @staticmethod
    def get_by_idempotency_key(session: Session, idempotency_key: str) -> Job | None:
        """Fetch a job by its unique idempotency key."""
        stmt = (
            select(Job)
            .options(selectinload(Job.execution_effect))
            .where(Job.idempotency_key == idempotency_key)
        )
        return session.scalars(stmt).first()

    @staticmethod
    def create(
        session: Session,
        *,
        idempotency_key: str,
        request_hash: str,
        payload: dict[str, Any],
        max_attempts: int,
    ) -> Job:
        """Stage a new queued Job row in the current session."""
        job = Job(
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            payload=payload,
            status=JobStatus.QUEUED.value,
            max_attempts=max_attempts,
        )
        session.add(job)
        return job

    @staticmethod
    def list_jobs(
        session: Session,
        *,
        status: JobStatus | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[Job], int]:
        """Return deterministically ordered jobs and total count for pagination."""
        base_filter = []
        if status is not None:
            base_filter.append(Job.status == status.value)

        count_stmt = select(func.count(Job.id)).where(*base_filter)
        total = int(session.scalar(count_stmt) or 0)

        items_stmt = (
            select(Job)
            .options(selectinload(Job.execution_effect))
            .where(*base_filter)
            .order_by(Job.created_at.asc(), Job.id.asc())
            .limit(limit)
            .offset(offset)
        )
        items = list(session.scalars(items_stmt).all())
        return items, total

    @staticmethod
    def cancel_queued_job(session: Session, job_id: uuid.UUID) -> Job | None:
        """Atomically transition a queued job to cancelled, returning it if updated."""
        stmt = text(
            """
            UPDATE jobs
            SET status = 'cancelled',
                cancelled_at = clock_timestamp(),
                updated_at = clock_timestamp()
            WHERE id = :job_id
              AND status = 'queued'
            RETURNING id
            """
        )
        updated_id = session.execute(stmt, {"job_id": job_id}).scalar_one_or_none()
        if updated_id is None:
            return None
        session.expire_all()
        return JobRepository.get_by_id(session, updated_id)

    @staticmethod
    def request_running_job_cancellation(
        session: Session, job_id: uuid.UUID
    ) -> Job | None:
        """Conditionally set cancel_requested on a running job, returning it if updated."""
        stmt = text(
            """
            UPDATE jobs
            SET cancel_requested = TRUE,
                cancel_requested_at = COALESCE(cancel_requested_at, clock_timestamp()),
                updated_at = clock_timestamp()
            WHERE id = :job_id
              AND status = 'running'
            RETURNING id
            """
        )
        updated_id = session.execute(stmt, {"job_id": job_id}).scalar_one_or_none()
        if updated_id is None:
            return None
        session.expire_all()
        return JobRepository.get_by_id(session, updated_id)
