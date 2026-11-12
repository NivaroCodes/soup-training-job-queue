"""Business logic for creating, inspecting, listing, and cancelling jobs."""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import set_committed_value

from app.db.models import Job, JobStatus
from app.repositories.job_repository import JobRepository

TERMINAL_STATUSES: frozenset[str] = frozenset(
    {
        JobStatus.SUCCEEDED.value,
        JobStatus.FAILED.value,
        JobStatus.CANCELLED.value,
    }
)
_CANCEL_RACE_RETRIES = 3


class JobNotFoundError(Exception):
    """Raised when a requested job does not exist."""


class IdempotencyConflictError(Exception):
    """Raised when an idempotency key is reused with a different request payload."""


def compute_request_hash(payload: dict[str, Any], max_attempts: int) -> str:
    """Compute a deterministic SHA-256 hex digest of the job creation parameters."""
    canonical = json.dumps(
        {"payload": payload, "max_attempts": max_attempts},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class JobService:
    """Coordinates job operations across transactions and repository calls."""

    @staticmethod
    def create_job(
        session: Session,
        *,
        idempotency_key: str,
        payload: dict[str, Any],
        max_attempts: int,
    ) -> tuple[Job, bool]:
        """Create a job idempotently, returning (job, created_new)."""
        request_hash = compute_request_hash(payload, max_attempts)

        existing = JobRepository.get_by_idempotency_key(session, idempotency_key)
        if existing is not None:
            if existing.request_hash == request_hash:
                return existing, False
            raise IdempotencyConflictError(
                f"Idempotency key '{idempotency_key}' was already used with a different payload."
            )

        job = JobRepository.create(
            session,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            payload=payload,
            max_attempts=max_attempts,
        )
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            concurrent = JobRepository.get_by_idempotency_key(session, idempotency_key)
            if concurrent is None:
                raise
            if concurrent.request_hash == request_hash:
                return concurrent, False
            raise IdempotencyConflictError(
                f"Idempotency key '{idempotency_key}' was already used with a different payload."
            ) from None

        # expire_on_commit=False keeps RETURNING-populated fields (id, created_at, updated_at)
        # intact; mark execution_effect as None to prevent a post-commit lazy-load SELECT.
        set_committed_value(job, "execution_effect", None)
        return job, True

    @staticmethod
    def get_job(session: Session, job_id: uuid.UUID) -> Job:
        """Fetch a job by ID or raise JobNotFoundError."""
        job = JobRepository.get_by_id(session, job_id)
        if job is None:
            raise JobNotFoundError(f"Job '{job_id}' was not found.")
        return job

    @staticmethod
    def list_jobs(
        session: Session,
        *,
        status: JobStatus | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[Job], int]:
        """Return deterministically ordered jobs and total count."""
        return JobRepository.list_jobs(
            session,
            status=status,
            limit=limit,
            offset=offset,
        )

    @staticmethod
    def cancel_job(session: Session, job_id: uuid.UUID) -> Job:
        """Cancel a queued job directly, request cancellation if running, or return terminal state."""
        for _ in range(_CANCEL_RACE_RETRIES):
            cancelled = JobRepository.cancel_queued_job(session, job_id)
            if cancelled is not None:
                session.commit()
                return cancelled

            requested = JobRepository.request_running_job_cancellation(session, job_id)
            if requested is not None:
                session.commit()
                return requested

            current = JobRepository.get_by_id(session, job_id)
            if current is None:
                raise JobNotFoundError(f"Job '{job_id}' was not found.")
            if current.status in TERMINAL_STATUSES:
                return current

            # State changed concurrently (e.g. claimed or requeued) between updates; retry.
            session.rollback()

        raise RuntimeError(
            f"Unable to resolve concurrent state transition while cancelling job '{job_id}'."
        )
