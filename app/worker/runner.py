"""Independent worker runner coordinating claim, heartbeat, execution, and recovery."""

from __future__ import annotations

import os
import socket
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from app.repositories.worker_repository import (
    ClaimedJob,
    RecoveryStats,
    WorkerRepository,
)
from app.worker.executor import (
    JobCancelledError,
    LeaseLostError,
    NonRetryableJobError,
    RetryableJobError,
    SimulatedJobExecutor,
)
from app.worker.heartbeat import LeaseHeartbeat

ExecuteFn = Callable[
    [dict[str, Any], threading.Event, threading.Event],
    dict[str, Any],
]


def generate_worker_id(prefix: str = "worker") -> str:
    """Generate a unique worker identifier for an independent OS process."""
    return f"{prefix}-{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


class WorkerRunner:
    """Polls PostgreSQL for jobs, executes them under a leased fence, and recovers expired jobs."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        worker_id: str | None = None,
        poll_interval_seconds: float = 0.5,
        lease_ttl_seconds: float = 30.0,
        heartbeat_interval_seconds: float = 10.0,
        recovery_interval_seconds: float = 5.0,
        recovery_batch_size: int = 50,
        executor: ExecuteFn | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.worker_id = worker_id or generate_worker_id()
        self.poll_interval_seconds = poll_interval_seconds
        self.lease_ttl_seconds = lease_ttl_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.recovery_interval_seconds = recovery_interval_seconds
        self.recovery_batch_size = recovery_batch_size
        self._executor = executor

    def _run_executor(
        self,
        payload: dict[str, Any],
        cancel_event: threading.Event,
        lease_lost_event: threading.Event,
    ) -> dict[str, Any]:
        if self._executor is not None:
            return self._executor(payload, cancel_event, lease_lost_event)
        return SimulatedJobExecutor.execute(
            payload,
            cancel_event=cancel_event,
            lease_lost_event=lease_lost_event,
        )

    def run_recovery_once(self) -> RecoveryStats:
        """Execute one dead-worker lease recovery sweep."""
        with self.session_factory() as session:
            return WorkerRepository.recover_expired_jobs(
                session,
                batch_size=self.recovery_batch_size,
            )

    def run_once(self) -> bool:
        """Attempt to claim and process a single queued job. Returns True if a job was claimed."""
        with self.session_factory() as session:
            claimed: ClaimedJob | None = WorkerRepository.claim_next_job(
                session,
                worker_id=self.worker_id,
                lease_ttl_seconds=self.lease_ttl_seconds,
            )

        if claimed is None:
            return False

        self._process_claimed_job(claimed)
        return True

    def _process_claimed_job(self, claimed: ClaimedJob) -> None:
        with LeaseHeartbeat(
            session_factory=self.session_factory,
            job_id=claimed.job_id,
            worker_id=self.worker_id,
            fencing_token=claimed.fencing_token,
            lease_ttl_seconds=self.lease_ttl_seconds,
            heartbeat_interval_seconds=self.heartbeat_interval_seconds,
            initial_cancel_requested=claimed.cancel_requested,
        ) as heartbeat:
            try:
                effect_payload = self._run_executor(
                    claimed.payload,
                    heartbeat.cancel_requested,
                    heartbeat.lease_lost,
                )
            except LeaseLostError:
                return
            except JobCancelledError:
                if heartbeat.lease_lost.is_set():
                    return
                with self.session_factory() as session:
                    WorkerRepository.complete_cooperative_cancellation(
                        session,
                        job_id=claimed.job_id,
                        worker_id=self.worker_id,
                        fencing_token=claimed.fencing_token,
                    )
                return
            except NonRetryableJobError as exc:
                if heartbeat.lease_lost.is_set():
                    return
                with self.session_factory() as session:
                    WorkerRepository.fail_job_non_retryable(
                        session,
                        job_id=claimed.job_id,
                        worker_id=self.worker_id,
                        fencing_token=claimed.fencing_token,
                        error_message=str(exc),
                    )
                return
            except RetryableJobError as exc:
                if heartbeat.lease_lost.is_set():
                    return
                with self.session_factory() as session:
                    WorkerRepository.handle_retryable_failure(
                        session,
                        job_id=claimed.job_id,
                        worker_id=self.worker_id,
                        fencing_token=claimed.fencing_token,
                        error_message=str(exc),
                    )
                return
            except Exception as exc:
                if heartbeat.lease_lost.is_set():
                    return
                with self.session_factory() as session:
                    WorkerRepository.handle_retryable_failure(
                        session,
                        job_id=claimed.job_id,
                        worker_id=self.worker_id,
                        fencing_token=claimed.fencing_token,
                        error_message=f"Unexpected worker error: {exc}",
                    )
                return

        # Heartbeat stopped cleanly; commit atomic side effect + succeeded transition.
        if heartbeat.lease_lost.is_set():
            return
        if heartbeat.cancel_requested.is_set():
            with self.session_factory() as session:
                WorkerRepository.complete_cooperative_cancellation(
                    session,
                    job_id=claimed.job_id,
                    worker_id=self.worker_id,
                    fencing_token=claimed.fencing_token,
                )
            return

        with self.session_factory() as session:
            WorkerRepository.complete_job_atomically(
                session,
                job_id=claimed.job_id,
                worker_id=self.worker_id,
                fencing_token=claimed.fencing_token,
                effect_payload=effect_payload,
            )

    def run_forever(self, stop_event: threading.Event | None = None) -> None:
        """Continuously poll for jobs and periodically recover expired leases."""
        stopper = stop_event or threading.Event()
        last_recovery = 0.0

        while not stopper.is_set():
            now = time.monotonic()
            if now - last_recovery >= self.recovery_interval_seconds:
                self.run_recovery_once()
                last_recovery = now

            claimed = self.run_once()
            if not claimed:
                stopper.wait(timeout=self.poll_interval_seconds)
