"""Independent background heartbeat thread with its own PostgreSQL session."""

from __future__ import annotations

import threading
import uuid
from types import TracebackType

from sqlalchemy.orm import Session, sessionmaker

from app.repositories.worker_repository import WorkerRepository


class LeaseHeartbeat:
    """Renews a worker's job lease on a dedicated background thread and DB session."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        job_id: uuid.UUID,
        worker_id: str,
        fencing_token: int,
        lease_ttl_seconds: float,
        heartbeat_interval_seconds: float,
        initial_cancel_requested: bool = False,
    ) -> None:
        self._session_factory = session_factory
        self._job_id = job_id
        self._worker_id = worker_id
        self._fencing_token = fencing_token
        self._lease_ttl_seconds = lease_ttl_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds

        self.lease_lost = threading.Event()
        self.cancel_requested = threading.Event()
        if initial_cancel_requested:
            self.cancel_requested.set()

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self.beats_succeeded = 0
        self.last_error: BaseException | None = None

    def start(self) -> None:
        """Start the background heartbeat thread."""
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"heartbeat-{self._worker_id}-{self._job_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the heartbeat thread to stop and wait for clean exit."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def beat_once(self) -> tuple[bool, bool]:
        """Execute a single heartbeat renewal using a dedicated PostgreSQL session."""
        with self._session_factory() as session:
            try:
                renewed, cancel_flag = WorkerRepository.renew_heartbeat(
                    session,
                    job_id=self._job_id,
                    worker_id=self._worker_id,
                    fencing_token=self._fencing_token,
                    lease_ttl_seconds=self._lease_ttl_seconds,
                )
            except Exception as exc:
                session.rollback()
                self.last_error = exc
                raise

        if not renewed:
            self.lease_lost.set()
            return False, False

        self.beats_succeeded += 1
        if cancel_flag:
            self.cancel_requested.set()
        return True, cancel_flag

    def _run_loop(self) -> None:
        while not self._stop_event.wait(timeout=self._heartbeat_interval_seconds):
            try:
                renewed, _ = self.beat_once()
                if not renewed:
                    return
            except Exception:
                # Keep trying until lease_ttl expires or stop() is signalled;
                # worker's final fenced DB check remains authoritative.
                continue

    def __enter__(self) -> LeaseHeartbeat:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.stop()
