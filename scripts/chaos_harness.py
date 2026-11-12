"""Reproducible Docker Compose chaos harness and PostgreSQL invariant verifier."""

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import create_engine, text


class TeeLogger:
    """Writes log lines to both stdout and a run-specific log file."""

    def __init__(self, log_path: Path) -> None:
        self._file = log_path.open("w", encoding="utf-8")
        self._lock = threading.Lock()

    def log(self, message: str) -> None:
        ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        line = f"[{ts}] {message}"
        with self._lock:
            print(line, flush=True)
            self._file.write(line + "\n")
            self._file.flush()

    def close(self) -> None:
        with self._lock:
            self._file.close()


def _run_cmd(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        capture_output=True,
        text=True,
        check=False,
    )


def _get_worker_containers() -> list[str]:
    res = _run_cmd(["docker", "compose", "ps", "-q", "worker"])
    if res.returncode != 0:
        raise RuntimeError(f"Failed to list worker containers: {res.stderr.strip()}")
    return [line.strip() for line in res.stdout.splitlines() if line.strip()]


def _get_container_running_state(container_id: str) -> bool:
    res = _run_cmd(
        ["docker", "inspect", "-f", "{{.State.Running}}", container_id]
    )
    return res.returncode == 0 and res.stdout.strip().lower() == "true"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run chaos workload against Docker Compose deployment and verify invariants."
    )
    parser.add_argument("--jobs", type=int, default=10000, help="Unique logical jobs to submit.")
    parser.add_argument("--workers", type=int, default=8, help="Expected worker container count.")
    parser.add_argument("--kill-interval", type=float, default=2.0, help="Seconds between SIGKILLs.")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic RNG seed.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="FastAPI base URL.")
    parser.add_argument(
        "--database-url",
        default=os.environ.get(
            "CHAOS_DATABASE_URL",
            "postgresql+psycopg://soup:soup@127.0.0.1:5433/soup_training",
        ),
        help="PostgreSQL URL for direct invariant verification.",
    )
    parser.add_argument("--drain-timeout", type=float, default=300.0, help="Max seconds to drain.")
    parser.add_argument("--duplicate-rate", type=float, default=0.15, help="Fraction of jobs duplicated.")
    parser.add_argument("--cancel-rate", type=float, default=0.10, help="Fraction of jobs cancelled.")
    parser.add_argument("--conflict-probes", type=int, default=20, help="Number of 409 conflict probes.")
    parser.add_argument("--concurrency", type=int, default=32, help="HTTP submission worker threads.")
    parser.add_argument("--job-steps", type=int, default=2, help="Simulated steps per job.")
    parser.add_argument("--step-duration-ms", type=float, default=15.0, help="Duration per step (ms).")
    parser.add_argument("--max-attempts", type=int, default=5, help="Max attempts per job.")
    parser.add_argument("--no-chaos", action="store_true", help="Disable SIGKILL chaos (smoke mode).")
    parser.add_argument("--clean-db", action="store_true", help="Truncate jobs/effects before run.")
    parser.add_argument("--artifacts-dir", default="artifacts/chaos", help="Evidence output directory.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    artifacts_dir = Path(args.artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    run_id = f"run_{run_ts}_seed{args.seed}_{uuid.uuid4().hex[:6]}"
    log_path = artifacts_dir / f"{run_id}.log"
    json_path = artifacts_dir / f"{run_id}.json"

    logger = TeeLogger(log_path)
    rng = random.Random(args.seed)
    started_at = datetime.now(UTC)

    report: dict[str, Any] = {
        "run_id": run_id,
        "seed": args.seed,
        "started_at": started_at.isoformat(),
        "ended_at": None,
        "duration_seconds": None,
        "config": {
            "jobs": args.jobs,
            "workers": args.workers,
            "kill_interval": args.kill_interval,
            "no_chaos": args.no_chaos,
            "duplicate_rate": args.duplicate_rate,
            "cancel_rate": args.cancel_rate,
            "conflict_probes": args.conflict_probes,
            "concurrency": args.concurrency,
            "job_steps": args.job_steps,
            "step_duration_ms": args.step_duration_ms,
            "max_attempts": args.max_attempts,
            "drain_timeout": args.drain_timeout,
        },
        "worker_containers_observed": [],
        "submitted_jobs": 0,
        "duplicate_requests_sent": 0,
        "duplicate_id_mismatches": 0,
        "conflict_409_probes": 0,
        "conflict_409_failures": 0,
        "cancel_requests_sent": 0,
        "http_errors": [],
        "kill_events": [],
        "restart_events": [],
        "final_state_counts": {},
        "execution_effects_count": 0,
        "invariants": [],
        "overall_passed": False,
    }

    engine = create_engine(args.database_url, pool_pre_ping=True)

    try:
        logger.log(f"Starting chaos run {run_id} (jobs={args.jobs}, workers={args.workers}, seed={args.seed}, no_chaos={args.no_chaos})")

        # 1. Verify API health & worker containers.
        with httpx.Client(base_url=args.base_url, timeout=10.0) as health_client:
            h_resp = health_client.get("/health")
            if h_resp.status_code != 200:
                raise RuntimeError(f"API health check failed: {h_resp.status_code} {h_resp.text}")

        containers = _get_worker_containers()
        report["worker_containers_observed"] = containers
        logger.log(f"Observed {len(containers)} worker containers: {containers}")
        if len(containers) < args.workers:
            raise RuntimeError(
                f"Expected at least {args.workers} worker containers, found {len(containers)}."
            )

        if args.clean_db:
            logger.log("Cleaning jobs and execution_effects tables before run (--clean-db).")
            with engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE execution_effects, jobs;"))

        # 2. Prepare deterministic workload indices.
        duplicate_indices = set(
            rng.sample(range(args.jobs), k=min(args.jobs, int(args.jobs * args.duplicate_rate)))
        )
        cancel_indices = set(
            rng.sample(range(args.jobs), k=min(args.jobs, int(args.jobs * args.cancel_rate)))
        )
        conflict_indices = rng.sample(
            range(args.jobs), k=min(args.jobs, args.conflict_probes)
        )

        # 3. Start background SIGKILL chaos thread if enabled.
        chaos_stop = threading.Event()
        chaos_lock = threading.Lock()

        def _chaos_loop() -> None:
            while not chaos_stop.wait(timeout=args.kill_interval):
                try:
                    current_containers = _get_worker_containers()
                    running = [c for c in current_containers if _get_container_running_state(c)]
                    if not running:
                        continue
                    target = rng.choice(running)
                    kill_ts = datetime.now(UTC).isoformat()
                    k_res = _run_cmd(["docker", "kill", "-s", "SIGKILL", target])
                    with chaos_lock:
                        report["kill_events"].append(
                            {
                                "timestamp": kill_ts,
                                "container_id": target[:12],
                                "returncode": k_res.returncode,
                            }
                        )
                    logger.log(f"CHAOS SIGKILL -> {target[:12]} (rc={k_res.returncode})")

                    # Ensure killed worker restarts cleanly.
                    restarted = False
                    for _ in range(20):
                        if _get_container_running_state(target):
                            restarted = True
                            break
                        time.sleep(0.2)
                    if not restarted:
                        _run_cmd(["docker", "start", target])
                        for _ in range(20):
                            if _get_container_running_state(target):
                                restarted = True
                                break
                            time.sleep(0.2)
                    with chaos_lock:
                        report["restart_events"].append(
                            {
                                "timestamp": datetime.now(UTC).isoformat(),
                                "container_id": target[:12],
                                "restarted": restarted,
                            }
                        )
                    logger.log(f"CHAOS RESTART -> {target[:12]} (restarted={restarted})")
                except Exception as exc:
                    logger.log(f"CHAOS ERROR: {exc}")

        chaos_thread: threading.Thread | None = None
        if not args.no_chaos:
            chaos_thread = threading.Thread(target=_chaos_loop, daemon=True)
            chaos_thread.start()

        # 4. Submit jobs, duplicate idempotency requests, and concurrent cancellations.
        job_ids_by_idx: dict[int, str] = {}
        state_lock = threading.Lock()

        limits = httpx.Limits(
            max_connections=max(32, args.concurrency * 2),
            max_keepalive_connections=max(16, args.concurrency),
        )
        with httpx.Client(base_url=args.base_url, timeout=30.0, limits=limits) as client:

            def _submit_job(idx: int) -> None:
                key = f"{run_id}:job:{idx}"
                payload = {
                    "model": "chaos-net",
                    "index": idx,
                    "steps": args.job_steps,
                    "step_duration_ms": args.step_duration_ms,
                }
                body = {"payload": payload, "max_attempts": args.max_attempts}

                resp = client.post("/jobs", headers={"Idempotency-Key": key}, json=body)
                if resp.status_code not in (200, 201):
                    with state_lock:
                        report["http_errors"].append(
                            f"POST /jobs idx={idx} status={resp.status_code} body={resp.text[:200]}"
                        )
                    return

                job_id = resp.json()["id"]
                with state_lock:
                    job_ids_by_idx[idx] = job_id
                    report["submitted_jobs"] += 1

                if idx in duplicate_indices:
                    dup_resp = client.post(
                        "/jobs", headers={"Idempotency-Key": key}, json=body
                    )
                    with state_lock:
                        report["duplicate_requests_sent"] += 1
                    if dup_resp.status_code not in (200, 201):
                        with state_lock:
                            report["http_errors"].append(
                                f"DUP POST /jobs idx={idx} status={dup_resp.status_code}"
                            )
                    elif dup_resp.json()["id"] != job_id:
                        with state_lock:
                            report["duplicate_id_mismatches"] += 1

                if idx in cancel_indices:
                    c_resp = client.post(f"/jobs/{job_id}/cancel")
                    with state_lock:
                        report["cancel_requests_sent"] += 1
                    if c_resp.status_code != 200:
                        with state_lock:
                            report["http_errors"].append(
                                f"POST /jobs/{job_id}/cancel status={c_resp.status_code}"
                            )

            logger.log("Submitting workload...")
            with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                futures = [pool.submit(_submit_job, i) for i in range(args.jobs)]
                for idx_done, fut in enumerate(as_completed(futures), start=1):
                    fut.result()
                    if idx_done % 1000 == 0 or idx_done == args.jobs:
                        logger.log(f"Submitted {idx_done}/{args.jobs} logical jobs...")

            # Run explicit 409 Conflict probes on already-submitted keys.
            for idx in conflict_indices:
                key = f"{run_id}:job:{idx}"
                conflict_resp = client.post(
                    "/jobs",
                    headers={"Idempotency-Key": key},
                    json={
                        "payload": {"model": "conflict-check", "index": idx, "tampered": True},
                        "max_attempts": args.max_attempts,
                    },
                )
                report["conflict_409_probes"] += 1
                if conflict_resp.status_code != 409:
                    report["conflict_409_failures"] += 1
                    report["http_errors"].append(
                        f"Expected 409 for idx={idx}, got {conflict_resp.status_code}"
                    )

        # 5. Wait for queue drain while chaos continues until active jobs drain, then stop chaos.
        logger.log("Waiting for all submitted jobs to reach terminal states...")
        drain_deadline = time.monotonic() + args.drain_timeout
        prefix_like = f"{run_id}:job:%"

        while time.monotonic() < drain_deadline:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT status, COUNT(*) AS cnt
                        FROM jobs
                        WHERE idempotency_key LIKE :prefix
                        GROUP BY status
                        """
                    ),
                    {"prefix": prefix_like},
                ).mappings().all()
            counts = {r["status"]: int(r["cnt"]) for r in rows}
            active = counts.get("queued", 0) + counts.get("running", 0)
            if active == 0:
                break
            time.sleep(1.0)

        chaos_stop.set()
        if chaos_thread is not None:
            chaos_thread.join(timeout=10.0)

        # Give a short final recovery window if a worker was killed right as chaos stopped.
        if time.monotonic() < drain_deadline:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT status, COUNT(*) AS cnt
                        FROM jobs
                        WHERE idempotency_key LIKE :prefix
                        GROUP BY status
                        """
                    ),
                    {"prefix": prefix_like},
                ).mappings().all()
            counts = {r["status"]: int(r["cnt"]) for r in rows}
            while (counts.get("queued", 0) + counts.get("running", 0)) > 0 and time.monotonic() < drain_deadline:
                time.sleep(1.0)
                with engine.connect() as conn:
                    rows = conn.execute(
                        text(
                            """
                            SELECT status, COUNT(*) AS cnt
                            FROM jobs
                            WHERE idempotency_key LIKE :prefix
                            GROUP BY status
                            """
                        ),
                        {"prefix": prefix_like},
                    ).mappings().all()
                counts = {r["status"]: int(r["cnt"]) for r in rows}

        # 6. Direct PostgreSQL invariant verification.
        with engine.connect() as conn:
            state_rows = conn.execute(
                text(
                    """
                    SELECT status, COUNT(*) AS cnt
                    FROM jobs
                    WHERE idempotency_key LIKE :prefix
                    GROUP BY status
                    """
                ),
                {"prefix": prefix_like},
            ).mappings().all()
            final_counts = Counter({r["status"]: int(r["cnt"]) for r in state_rows})
            report["final_state_counts"] = dict(final_counts)
            total_db_jobs = sum(final_counts.values())

            effects_count = int(
                conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM execution_effects e
                        JOIN jobs j ON j.id = e.job_id
                        WHERE j.idempotency_key LIKE :prefix
                        """
                    ),
                    {"prefix": prefix_like},
                ).scalar_one()
            )
            report["execution_effects_count"] = effects_count

            dup_effects = int(
                conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM (
                            SELECT e.job_id
                            FROM execution_effects e
                            JOIN jobs j ON j.id = e.job_id
                            WHERE j.idempotency_key LIKE :prefix
                            GROUP BY e.job_id
                            HAVING COUNT(*) > 1
                        ) sub
                        """
                    ),
                    {"prefix": prefix_like},
                ).scalar_one()
            )

            succeeded_missing_effect = int(
                conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM jobs j
                        LEFT JOIN execution_effects e ON e.job_id = j.id
                        WHERE j.idempotency_key LIKE :prefix
                          AND j.status = 'succeeded'
                          AND e.id IS NULL
                        """
                    ),
                    {"prefix": prefix_like},
                ).scalar_one()
            )

            non_succeeded_with_effect = int(
                conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM jobs j
                        JOIN execution_effects e ON e.job_id = j.id
                        WHERE j.idempotency_key LIKE :prefix
                          AND j.status IN ('queued', 'running', 'cancelled', 'failed')
                        """
                    ),
                    {"prefix": prefix_like},
                ).scalar_one()
            )

            succeeded_with_cancel = int(
                conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM jobs
                        WHERE idempotency_key LIKE :prefix
                          AND status = 'succeeded'
                          AND cancel_requested = TRUE
                        """
                    ),
                    {"prefix": prefix_like},
                ).scalar_one()
            )

            fencing_mismatches = int(
                conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM execution_effects e
                        JOIN jobs j ON j.id = e.job_id
                        WHERE j.idempotency_key LIKE :prefix
                          AND (e.fencing_token != j.fencing_token OR e.fencing_token < 1)
                        """
                    ),
                    {"prefix": prefix_like},
                ).scalar_one()
            )

        invariants = [
            {
                "name": "no_lost_jobs",
                "passed": total_db_jobs == args.jobs and len(report["http_errors"]) == 0,
                "details": f"db_jobs={total_db_jobs}, expected={args.jobs}, http_errors={len(report['http_errors'])}",
            },
            {
                "name": "no_duplicate_side_effects",
                "passed": dup_effects == 0,
                "details": f"duplicate_effect_jobs={dup_effects}",
            },
            {
                "name": "no_stuck_running_or_queued_jobs",
                "passed": final_counts.get("running", 0) == 0 and final_counts.get("queued", 0) == 0,
                "details": f"running={final_counts.get('running', 0)}, queued={final_counts.get('queued', 0)}",
            },
            {
                "name": "consistent_final_states",
                "passed": (
                    succeeded_missing_effect == 0
                    and non_succeeded_with_effect == 0
                    and succeeded_with_cancel == 0
                    and effects_count == final_counts.get("succeeded", 0)
                ),
                "details": (
                    f"succeeded={final_counts.get('succeeded', 0)}, effects={effects_count}, "
                    f"succeeded_missing_effect={succeeded_missing_effect}, "
                    f"non_succeeded_with_effect={non_succeeded_with_effect}, "
                    f"succeeded_with_cancel={succeeded_with_cancel}"
                ),
            },
            {
                "name": "fencing_integrity",
                "passed": fencing_mismatches == 0,
                "details": f"fencing_mismatches={fencing_mismatches}",
            },
            {
                "name": "idempotency_and_409_conflict",
                "passed": report["duplicate_id_mismatches"] == 0 and report["conflict_409_failures"] == 0,
                "details": (
                    f"duplicates={report['duplicate_requests_sent']}, "
                    f"mismatches={report['duplicate_id_mismatches']}, "
                    f"409_probes={report['conflict_409_probes']}, "
                    f"409_failures={report['conflict_409_failures']}"
                ),
            },
        ]
        report["invariants"] = invariants
        report["overall_passed"] = all(inv["passed"] for inv in invariants)

        logger.log(f"Final state counts: {report['final_state_counts']}, effects={effects_count}")
        logger.log(f"Kill events: {len(report['kill_events'])}, Restart events: {len(report['restart_events'])}")
        for inv in invariants:
            status_str = "PASS" if inv["passed"] else "FAIL"
            logger.log(f"INVARIANT [{status_str}] {inv['name']}: {inv['details']}")

    except Exception as exc:
        logger.log(f"HARNESS FATAL ERROR: {exc}")
        report["http_errors"].append(f"FATAL: {exc}")
        report["overall_passed"] = False
    finally:
        ended_at = datetime.now(UTC)
        report["ended_at"] = ended_at.isoformat()
        report["duration_seconds"] = round((ended_at - started_at).total_seconds(), 3)
        json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        logger.log(f"Saved JSON report: {json_path}")
        logger.log(f"Saved log output: {log_path}")
        logger.close()
        engine.dispose()

    return 0 if report["overall_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
