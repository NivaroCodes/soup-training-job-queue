"""CLI entry point for launching an independent worker OS process."""

from __future__ import annotations

import argparse
import signal
import threading

from app.core.config import get_settings
from app.db.session import get_session_factory
from app.worker.runner import WorkerRunner


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Run an independent Soup Training Job Queue worker process."
    )
    parser.add_argument("--worker-id", default=None, help="Explicit unique worker ID.")
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=settings.worker_poll_interval_seconds,
    )
    parser.add_argument(
        "--lease-ttl",
        type=float,
        default=settings.worker_lease_ttl_seconds,
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=settings.worker_heartbeat_interval_seconds,
    )
    parser.add_argument(
        "--recovery-interval",
        type=float,
        default=settings.worker_recovery_interval_seconds,
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single recovery sweep + claim/execute cycle and exit.",
    )
    args = parser.parse_args(argv)

    runner = WorkerRunner(
        session_factory=get_session_factory(),
        worker_id=args.worker_id,
        poll_interval_seconds=args.poll_interval,
        lease_ttl_seconds=args.lease_ttl,
        heartbeat_interval_seconds=args.heartbeat_interval,
        recovery_interval_seconds=args.recovery_interval,
    )

    if args.once:
        runner.run_recovery_once()
        runner.run_once()
        return 0

    stop_event = threading.Event()

    def _handle_signal(_signum: int, _frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    runner.run_forever(stop_event=stop_event)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
