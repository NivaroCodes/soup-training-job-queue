"""PostgreSQL worker coordination repository (claiming, heartbeats, fencing, recovery)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models import ExecutionEffect


@dataclass(frozen=True, slots=True)
class ClaimedJob:
    """Snapshot of a job atomically claimed by a worker."""

    job_id: uuid.UUID
    payload: dict[str, Any]
    fencing_token: int
    attempt: int
    max_attempts: int
    cancel_requested: bool


@dataclass(frozen=True, slots=True)
class RecoveryStats:
    """Counts of expired running jobs recovered in a single sweep."""

    cancelled: int = 0
    requeued: int = 0
    failed: int = 0

    @property
    def total(self) -> int:
        return self.cancelled + self.requeued + self.failed


class WorkerRepository:
    """Encapsulates atomic PostgreSQL transactions for worker execution and recovery."""

    @staticmethod
    def claim_next_job(
        session: Session,
        *,
        worker_id: str,
        lease_ttl_seconds: float,
    ) -> ClaimedJob | None:
        """Atomically claim one queued job using FOR UPDATE SKIP LOCKED."""
        select_stmt = text(
            """
            SELECT id
            FROM jobs
            WHERE status = 'queued'
            ORDER BY created_at ASC, id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """
        )
        job_id = session.execute(select_stmt).scalar_one_or_none()
        if job_id is None:
            session.rollback()
            return None

        update_stmt = text(
            """
            UPDATE jobs
            SET status = 'running',
                worker_id = :worker_id,
                fencing_token = fencing_token + 1,
                attempt = attempt + 1,
                lease_expires_at = clock_timestamp() + make_interval(secs => :lease_ttl),
                last_heartbeat_at = clock_timestamp(),
                started_at = COALESCE(started_at, clock_timestamp()),
                updated_at = clock_timestamp()
            WHERE id = :job_id
              AND status = 'queued'
            RETURNING id, payload, fencing_token, attempt, max_attempts, cancel_requested
            """
        )
        row = (
            session.execute(
                update_stmt,
                {
                    "job_id": job_id,
                    "worker_id": worker_id,
                    "lease_ttl": lease_ttl_seconds,
                },
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            session.rollback()
            return None

        session.commit()
        return ClaimedJob(
            job_id=row["id"],
            payload=dict(row["payload"]),
            fencing_token=int(row["fencing_token"]),
            attempt=int(row["attempt"]),
            max_attempts=int(row["max_attempts"]),
            cancel_requested=bool(row["cancel_requested"]),
        )

    @staticmethod
    def renew_heartbeat(
        session: Session,
        *,
        job_id: uuid.UUID,
        worker_id: str,
        fencing_token: int,
        lease_ttl_seconds: float,
    ) -> tuple[bool, bool]:
        """Extend lease if worker still holds valid ownership and fencing token.

        Returns (renewed, cancel_requested).
        """
        stmt = text(
            """
            UPDATE jobs
            SET lease_expires_at = clock_timestamp() + make_interval(secs => :lease_ttl),
                last_heartbeat_at = clock_timestamp(),
                updated_at = clock_timestamp()
            WHERE id = :job_id
              AND status = 'running'
              AND worker_id = :worker_id
              AND fencing_token = :fencing_token
              AND lease_expires_at > clock_timestamp()
            RETURNING cancel_requested
            """
        )
        row = (
            session.execute(
                stmt,
                {
                    "job_id": job_id,
                    "worker_id": worker_id,
                    "fencing_token": fencing_token,
                    "lease_ttl": lease_ttl_seconds,
                },
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            session.rollback()
            return False, False

        session.commit()
        return True, bool(row["cancel_requested"])

    @staticmethod
    def complete_job_atomically(
        session: Session,
        *,
        job_id: uuid.UUID,
        worker_id: str,
        fencing_token: int,
        effect_payload: dict[str, Any],
    ) -> bool:
        """Execute the 9-step atomic completion transaction (effect + succeeded)."""
        lock_stmt = text(
            """
            SELECT id,
                   status,
                   worker_id,
                   fencing_token,
                   (lease_expires_at > clock_timestamp()) AS lease_valid,
                   cancel_requested
            FROM jobs
            WHERE id = :job_id
            FOR UPDATE
            """
        )
        locked = session.execute(lock_stmt, {"job_id": job_id}).mappings().one_or_none()
        if (
            locked is None
            or locked["status"] != "running"
            or locked["worker_id"] != worker_id
            or int(locked["fencing_token"]) != fencing_token
            or not bool(locked["lease_valid"])
            or bool(locked["cancel_requested"])
        ):
            session.rollback()
            return False

        effect = ExecutionEffect(
            job_id=job_id,
            worker_id=worker_id,
            fencing_token=fencing_token,
            effect_payload=effect_payload,
        )
        session.add(effect)

        try:
            session.flush()
            update_stmt = text(
                """
                UPDATE jobs
                SET status = 'succeeded',
                    completed_at = clock_timestamp(),
                    updated_at = clock_timestamp(),
                    worker_id = NULL,
                    lease_expires_at = NULL
                WHERE id = :job_id
                  AND status = 'running'
                  AND worker_id = :worker_id
                  AND fencing_token = :fencing_token
                  AND lease_expires_at > clock_timestamp()
                  AND cancel_requested = FALSE
                """
            )
            result = session.execute(
                update_stmt,
                {
                    "job_id": job_id,
                    "worker_id": worker_id,
                    "fencing_token": fencing_token,
                },
            )
            if result.rowcount != 1:
                session.rollback()
                return False
            session.commit()
            return True
        except IntegrityError:
            session.rollback()
            return False

    @staticmethod
    def complete_cooperative_cancellation(
        session: Session,
        *,
        job_id: uuid.UUID,
        worker_id: str,
        fencing_token: int,
    ) -> bool:
        """Transition running job to cancelled if lease, token, and cancel_requested hold."""
        lock_stmt = text(
            """
            SELECT id
            FROM jobs
            WHERE id = :job_id
            FOR UPDATE
            """
        )
        if session.execute(lock_stmt, {"job_id": job_id}).scalar_one_or_none() is None:
            session.rollback()
            return False

        update_stmt = text(
            """
            UPDATE jobs
            SET status = 'cancelled',
                cancelled_at = clock_timestamp(),
                updated_at = clock_timestamp(),
                worker_id = NULL,
                lease_expires_at = NULL
            WHERE id = :job_id
              AND status = 'running'
              AND worker_id = :worker_id
              AND fencing_token = :fencing_token
              AND lease_expires_at > clock_timestamp()
              AND cancel_requested = TRUE
            """
        )
        result = session.execute(
            update_stmt,
            {
                "job_id": job_id,
                "worker_id": worker_id,
                "fencing_token": fencing_token,
            },
        )
        if result.rowcount != 1:
            session.rollback()
            return False
        session.commit()
        return True

    @staticmethod
    def handle_retryable_failure(
        session: Session,
        *,
        job_id: uuid.UUID,
        worker_id: str,
        fencing_token: int,
        error_message: str,
    ) -> str:
        """Handle retryable failure with strict precedence: cancelled > queued > failed.

        Returns one of: 'cancelled', 'queued', 'failed', 'aborted_stale'.
        """
        lock_stmt = text(
            """
            SELECT id,
                   status,
                   worker_id,
                   fencing_token,
                   (lease_expires_at > clock_timestamp()) AS lease_valid,
                   cancel_requested,
                   attempt,
                   max_attempts
            FROM jobs
            WHERE id = :job_id
            FOR UPDATE
            """
        )
        locked = session.execute(lock_stmt, {"job_id": job_id}).mappings().one_or_none()
        if (
            locked is None
            or locked["status"] != "running"
            or locked["worker_id"] != worker_id
            or int(locked["fencing_token"]) != fencing_token
            or not bool(locked["lease_valid"])
        ):
            session.rollback()
            return "aborted_stale"

        if bool(locked["cancel_requested"]):
            session.rollback()
            cancelled = WorkerRepository.complete_cooperative_cancellation(
                session,
                job_id=job_id,
                worker_id=worker_id,
                fencing_token=fencing_token,
            )
            return "cancelled" if cancelled else "aborted_stale"

        attempt = int(locked["attempt"])
        max_attempts = int(locked["max_attempts"])

        if attempt < max_attempts:
            update_stmt = text(
                """
                UPDATE jobs
                SET status = 'queued',
                    last_error = :error_message,
                    updated_at = clock_timestamp(),
                    worker_id = NULL,
                    lease_expires_at = NULL
                WHERE id = :job_id
                  AND status = 'running'
                  AND worker_id = :worker_id
                  AND fencing_token = :fencing_token
                  AND lease_expires_at > clock_timestamp()
                  AND cancel_requested = FALSE
                  AND attempt < max_attempts
                """
            )
            target_outcome = "queued"
        else:
            update_stmt = text(
                """
                UPDATE jobs
                SET status = 'failed',
                    last_error = :error_message,
                    completed_at = clock_timestamp(),
                    updated_at = clock_timestamp(),
                    worker_id = NULL,
                    lease_expires_at = NULL
                WHERE id = :job_id
                  AND status = 'running'
                  AND worker_id = :worker_id
                  AND fencing_token = :fencing_token
                  AND lease_expires_at > clock_timestamp()
                  AND cancel_requested = FALSE
                  AND attempt >= max_attempts
                """
            )
            target_outcome = "failed"

        result = session.execute(
            update_stmt,
            {
                "job_id": job_id,
                "worker_id": worker_id,
                "fencing_token": fencing_token,
                "error_message": error_message,
            },
        )
        if result.rowcount == 1:
            session.commit()
            return target_outcome

        # Zero-row fallback: roll back, re-read state, and follow cancellation if valid.
        session.rollback()
        reread = session.execute(
            text(
                """
                SELECT status,
                       worker_id,
                       fencing_token,
                       (lease_expires_at > clock_timestamp()) AS lease_valid,
                       cancel_requested
                FROM jobs
                WHERE id = :job_id
                """
            ),
            {"job_id": job_id},
        ).mappings().one_or_none()
        if (
            reread is not None
            and reread["status"] == "running"
            and bool(reread["cancel_requested"])
            and reread["worker_id"] == worker_id
            and int(reread["fencing_token"]) == fencing_token
            and bool(reread["lease_valid"])
        ):
            cancelled = WorkerRepository.complete_cooperative_cancellation(
                session,
                job_id=job_id,
                worker_id=worker_id,
                fencing_token=fencing_token,
            )
            return "cancelled" if cancelled else "aborted_stale"

        return "aborted_stale"

    @staticmethod
    def fail_job_non_retryable(
        session: Session,
        *,
        job_id: uuid.UUID,
        worker_id: str,
        fencing_token: int,
        error_message: str,
    ) -> bool:
        """Transition running job directly to failed (takes precedence over cancel_requested)."""
        lock_stmt = text(
            """
            SELECT id
            FROM jobs
            WHERE id = :job_id
            FOR UPDATE
            """
        )
        if session.execute(lock_stmt, {"job_id": job_id}).scalar_one_or_none() is None:
            session.rollback()
            return False

        update_stmt = text(
            """
            UPDATE jobs
            SET status = 'failed',
                last_error = :error_message,
                completed_at = clock_timestamp(),
                updated_at = clock_timestamp(),
                worker_id = NULL,
                lease_expires_at = NULL
            WHERE id = :job_id
              AND status = 'running'
              AND worker_id = :worker_id
              AND fencing_token = :fencing_token
              AND lease_expires_at > clock_timestamp()
            """
        )
        result = session.execute(
            update_stmt,
            {
                "job_id": job_id,
                "worker_id": worker_id,
                "fencing_token": fencing_token,
                "error_message": error_message,
            },
        )
        if result.rowcount != 1:
            session.rollback()
            return False
        session.commit()
        return True

    @staticmethod
    def recover_expired_jobs(
        session: Session,
        *,
        batch_size: int = 50,
    ) -> RecoveryStats:
        """Recover running jobs whose leases expired, checking cancel_requested first."""
        select_stmt = text(
            """
            SELECT id, attempt, max_attempts, cancel_requested, fencing_token
            FROM jobs
            WHERE status = 'running'
              AND lease_expires_at <= clock_timestamp()
            ORDER BY lease_expires_at ASC, id ASC
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
            """
        )
        rows = (
            session.execute(select_stmt, {"batch_size": batch_size})
            .mappings()
            .all()
        )
        if not rows:
            session.rollback()
            return RecoveryStats()

        cancelled_count = 0
        requeued_count = 0
        failed_count = 0

        for row in rows:
            job_id = row["id"]
            token = int(row["fencing_token"])
            attempt = int(row["attempt"])
            max_attempts = int(row["max_attempts"])
            cancel_requested = bool(row["cancel_requested"])

            if cancel_requested:
                res = session.execute(
                    text(
                        """
                        UPDATE jobs
                        SET status = 'cancelled',
                            cancelled_at = clock_timestamp(),
                            updated_at = clock_timestamp(),
                            worker_id = NULL,
                            lease_expires_at = NULL,
                            fencing_token = fencing_token + 1
                        WHERE id = :job_id
                          AND status = 'running'
                          AND fencing_token = :fencing_token
                          AND lease_expires_at <= clock_timestamp()
                          AND cancel_requested = TRUE
                        """
                    ),
                    {"job_id": job_id, "fencing_token": token},
                )
                if res.rowcount == 1:
                    cancelled_count += 1
            elif attempt < max_attempts:
                res = session.execute(
                    text(
                        """
                        UPDATE jobs
                        SET status = 'queued',
                            last_error = COALESCE(last_error, 'worker lease expired'),
                            updated_at = clock_timestamp(),
                            worker_id = NULL,
                            lease_expires_at = NULL,
                            fencing_token = fencing_token + 1
                        WHERE id = :job_id
                          AND status = 'running'
                          AND fencing_token = :fencing_token
                          AND lease_expires_at <= clock_timestamp()
                          AND cancel_requested = FALSE
                          AND attempt < max_attempts
                        """
                    ),
                    {"job_id": job_id, "fencing_token": token},
                )
                if res.rowcount == 1:
                    requeued_count += 1
            else:
                res = session.execute(
                    text(
                        """
                        UPDATE jobs
                        SET status = 'failed',
                            last_error = 'worker lease expired after max_attempts',
                            completed_at = clock_timestamp(),
                            updated_at = clock_timestamp(),
                            worker_id = NULL,
                            lease_expires_at = NULL,
                            fencing_token = fencing_token + 1
                        WHERE id = :job_id
                          AND status = 'running'
                          AND fencing_token = :fencing_token
                          AND lease_expires_at <= clock_timestamp()
                          AND cancel_requested = FALSE
                          AND attempt >= max_attempts
                        """
                    ),
                    {"job_id": job_id, "fencing_token": token},
                )
                if res.rowcount == 1:
                    failed_count += 1

        session.commit()
        return RecoveryStats(
            cancelled=cancelled_count,
            requeued=requeued_count,
            failed=failed_count,
        )
