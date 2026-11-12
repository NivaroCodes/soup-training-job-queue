"""Reproducible HTTP load testing and PostgreSQL/Docker telemetry harness."""

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import threading
import time
import uuid
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import create_engine, text


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = (len(sorted_vals) - 1) * (pct / 100.0)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return round(sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac, 3)


def _git_revision() -> str:
    res = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False
    )
    return res.stdout.strip() if res.returncode == 0 else "unknown"


def _sample_docker_stats() -> list[dict[str, str]]:
    res = subprocess.run(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if res.returncode != 0:
        return []
    rows = []
    for line in res.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) == 3:
            rows.append({"container": parts[0], "cpu": parts[1], "mem": parts[2]})
    return rows


def _get_container_restarts() -> dict[str, Any]:
    ps_res = subprocess.run(
        ["docker", "compose", "ps", "-q"], capture_output=True, text=True, check=False
    )
    if ps_res.returncode != 0:
        return {}
    ids = [line.strip() for line in ps_res.stdout.splitlines() if line.strip()]
    if not ids:
        return {}
    insp = subprocess.run(
        [
            "docker",
            "inspect",
            "-f",
            "{{.Name}}\t{{.State.Running}}\t{{.RestartCount}}",
            *ids,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if insp.returncode != 0:
        return {}
    out: dict[str, Any] = {}
    for line in insp.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) == 3:
            out[parts[0].lstrip("/")] = {
                "running": parts[1].lower() == "true",
                "restart_count": int(parts[2]),
            }
    return out


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load test soup-training-job-queue API.")
    parser.add_argument(
        "--scenario",
        choices=[
            "post_unique",
            "post_duplicate",
            "get_job",
            "get_list",
            "post_cancel",
            "read_heavy",
            "mixed",
        ],
        required=True,
    )
    parser.add_argument("--rps", type=float, required=True, help="Target requests per second.")
    parser.add_argument("--duration", type=float, default=10.0, help="Measured duration (s).")
    parser.add_argument("--warmup", type=float, default=2.0, help="Warm-up duration (s).")
    parser.add_argument("--concurrency", type=int, default=64, help="Max in-flight worker threads.")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic RNG seed.")
    parser.add_argument("--window-seconds", type=float, default=30.0, help="Telemetry window size (s).")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument(
        "--database-url",
        default=os.environ.get(
            "PERF_DATABASE_URL",
            "postgresql+psycopg://soup:soup@127.0.0.1:5433/soup_training",
        ),
    )
    parser.add_argument("--drain-timeout", type=float, default=120.0)
    parser.add_argument("--artifacts-dir", default="artifacts/perf")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    artifacts_dir = Path(args.artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    run_id = f"perf_{args.scenario}_{int(args.rps)}rps_{run_ts}_{uuid.uuid4().hex[:6]}"
    log_path = artifacts_dir / f"{run_id}.log"
    json_path = artifacts_dir / f"{run_id}.json"

    rng = random.Random(args.seed)
    engine = create_engine(args.database_url, pool_pre_ping=True)

    log_lines: list[str] = []
    log_lock = threading.Lock()

    def _log(msg: str) -> None:
        line = f"[{datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')}] {msg}"
        with log_lock:
            print(line, flush=True)
            log_lines.append(line)

    started_at = datetime.now(UTC)
    _log(
        f"Starting {run_id}: scenario={args.scenario}, target_rps={args.rps}, "
        f"duration={args.duration}s, warmup={args.warmup}s, concurrency={args.concurrency}, seed={args.seed}"
    )

    restarts_before = _get_container_restarts()

    limits = httpx.Limits(
        max_connections=max(64, args.concurrency * 2),
        max_keepalive_connections=max(32, args.concurrency),
    )

    seed_job_ids: list[str] = []
    seed_dup_keys: list[str] = []
    cancel_pool_ids: list[str] = []

    with httpx.Client(base_url=args.base_url, timeout=15.0, limits=limits) as client:
        if args.scenario in ("post_duplicate", "get_job", "read_heavy", "mixed"):
            for i in range(50):
                key = f"{run_id}:seed:{i}"
                r = client.post(
                    "/jobs",
                    headers={"Idempotency-Key": key},
                    json={"payload": {"model": "perf-seed", "idx": i, "steps": 2, "step_duration_ms": 15.0}},
                )
                if r.status_code in (200, 201):
                    seed_dup_keys.append(key)
                    seed_job_ids.append(r.json()["id"])

        if args.scenario == "post_cancel":
            total_cancel_needed = int(args.rps * (args.warmup + args.duration)) + 20
            for i in range(total_cancel_needed):
                key = f"{run_id}:cancel_seed:{i}"
                r = client.post(
                    "/jobs",
                    headers={"Idempotency-Key": key},
                    json={"payload": {"model": "perf-cancel", "idx": i, "steps": 2, "step_duration_ms": 15.0}},
                )
                if r.status_code in (200, 201):
                    cancel_pool_ids.append(r.json()["id"])

    # Shared recording state.
    req_lock = threading.Lock()
    latencies_ms: list[float] = []
    per_endpoint_lat: dict[str, list[float]] = defaultdict(list)
    per_endpoint_status: dict[str, Counter[int]] = defaultdict(Counter)
    per_endpoint_errors: Counter[str] = Counter()
    status_counts: Counter[int] = Counter()
    conn_errors = 0
    timeouts = 0
    error_samples: list[str] = []
    unique_counter = 0
    unique_recorded_count = 0
    cancel_cursor = 0

    # Windowed telemetry state.
    window_latencies: list[float] = []
    window_errors = 0
    pg_samples: list[dict[str, Any]] = []
    timeseries: list[dict[str, Any]] = []
    sample_stop = threading.Event()
    prefix_like = f"{run_id}:%"
    unique_prefix_like = f"{run_id}:u:%"

    def _pg_sampler() -> None:
        while not sample_stop.is_set():
            try:
                with engine.connect() as conn:
                    row = conn.execute(
                        text(
                            """
                            SELECT
                                COUNT(*) FILTER (WHERE state = 'active') AS active_conns,
                                COUNT(*) AS total_conns,
                                COUNT(*) FILTER (WHERE wait_event_type = 'Lock') AS lock_waits,
                                COALESCE(MAX(EXTRACT(EPOCH FROM (clock_timestamp() - query_start)) * 1000.0)
                                    FILTER (WHERE state = 'active'), 0.0) AS max_active_query_ms
                            FROM pg_stat_activity
                            WHERE datname = current_database()
                              AND pid <> pg_backend_pid()
                            """
                        )
                    ).mappings().one()
                    pg_samples.append(
                        {
                            "active_conns": int(row["active_conns"]),
                            "total_conns": int(row["total_conns"]),
                            "lock_waits": int(row["lock_waits"]),
                            "max_active_query_ms": round(float(row["max_active_query_ms"]), 2),
                        }
                    )
            except Exception:
                pass
            sample_stop.wait(timeout=1.0)

    def _window_reporter() -> None:
        nonlocal window_latencies, window_errors
        win_idx = 0
        while not sample_stop.wait(timeout=args.window_seconds):
            win_idx += 1
            with req_lock:
                w_lats = window_latencies
                w_errs = window_errors
                window_latencies = []
                window_errors = 0
            w_sorted = sorted(w_lats)
            w_rps = round((len(w_lats) + w_errs) / args.window_seconds, 2)

            q_counts: dict[str, int] = {}
            pg_snap: dict[str, Any] = {}
            try:
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
                    q_counts = {r["status"]: int(r["cnt"]) for r in rows}
                    pg_row = conn.execute(
                        text(
                            """
                            SELECT
                                COUNT(*) AS total_conns,
                                COUNT(*) FILTER (WHERE state = 'active') AS active_conns,
                                COUNT(*) FILTER (WHERE wait_event_type = 'Lock') AS lock_waits
                            FROM pg_stat_activity
                            WHERE datname = current_database()
                              AND pid <> pg_backend_pid()
                            """
                        )
                    ).mappings().one()
                    pg_snap = {k: int(v) for k, v in pg_row.items()}
            except Exception:
                pass

            d_stats = _sample_docker_stats()
            entry = {
                "window": win_idx,
                "timestamp": datetime.now(UTC).isoformat(),
                "achieved_rps": w_rps,
                "requests": len(w_lats),
                "errors": w_errs,
                "p50_ms": _percentile(w_sorted, 50),
                "p95_ms": _percentile(w_sorted, 95),
                "p99_ms": _percentile(w_sorted, 99),
                "max_ms": round(w_sorted[-1], 3) if w_sorted else 0.0,
                "pg": pg_snap,
                "queue_counts": q_counts,
                "docker_stats": d_stats,
            }
            timeseries.append(entry)
            _log(
                f"[W{win_idx}] rps={w_rps}, p50={entry['p50_ms']}ms, p95={entry['p95_ms']}ms, "
                f"p99={entry['p99_ms']}ms, err={w_errs}, pg={pg_snap}, jobs={q_counts}"
            )

    sampler_thread = threading.Thread(target=_pg_sampler, daemon=True)
    sampler_thread.start()
    window_thread = threading.Thread(target=_window_reporter, daemon=True)
    window_thread.start()

    def _next_request_spec(record: bool) -> tuple[str, str, str, dict[str, Any] | None, dict[str, str] | None]:
        nonlocal unique_counter, cancel_cursor
        with req_lock:
            idx = unique_counter
            unique_counter += 1

        scen = args.scenario
        if scen == "mixed":
            roll = rng.random()
            if roll < 0.40:
                scen = "post_unique"
            elif roll < 0.55:
                scen = "post_duplicate"
            elif roll < 0.75:
                scen = "get_job"
            elif roll < 0.90:
                scen = "get_list"
            else:
                scen = "post_cancel"
        elif scen == "read_heavy":
            scen = "get_job" if rng.random() < 0.50 else "get_list"

        if scen == "post_unique":
            prefix = "u" if record else "wu"
            return (
                "POST /jobs (unique)",
                "POST",
                "/jobs",
                {"payload": {"model": "perf", "idx": idx, "steps": 2, "step_duration_ms": 15.0}},
                {"Idempotency-Key": f"{run_id}:{prefix}:{idx}"},
            )
        if scen == "post_duplicate":
            d_idx = idx % len(seed_dup_keys)
            return (
                "POST /jobs (duplicate)",
                "POST",
                "/jobs",
                {"payload": {"model": "perf-seed", "idx": d_idx, "steps": 2, "step_duration_ms": 15.0}},
                {"Idempotency-Key": seed_dup_keys[d_idx]},
            )
        if scen == "get_job":
            jid = seed_job_ids[idx % len(seed_job_ids)]
            return ("GET /jobs/{id}", "GET", f"/jobs/{jid}", None, None)
        if scen == "get_list":
            offset = (idx * 20) % 200
            return ("GET /jobs", "GET", f"/jobs?limit=20&offset={offset}", None, None)
        if cancel_pool_ids:
            with req_lock:
                c_idx = cancel_cursor
                cancel_cursor += 1
            jid = cancel_pool_ids[c_idx % len(cancel_pool_ids)]
        else:
            jid = seed_job_ids[idx % len(seed_job_ids)]
        return ("POST /jobs/{id}/cancel", "POST", f"/jobs/{jid}/cancel", None, None)

    measure_start = 0.0
    measure_end = 0.0

    with httpx.Client(base_url=args.base_url, timeout=15.0, limits=limits) as client:

        def _execute_one(record: bool) -> None:
            nonlocal conn_errors, timeouts, window_errors, unique_recorded_count
            ep_label, method, path, body, headers = _next_request_spec(record)
            t0 = time.perf_counter()
            try:
                if method == "POST":
                    resp = client.post(path, json=body, headers=headers)
                else:
                    resp = client.get(path)
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                if record:
                    with req_lock:
                        latencies_ms.append(elapsed_ms)
                        window_latencies.append(elapsed_ms)
                        status_counts[resp.status_code] += 1
                        per_endpoint_lat[ep_label].append(elapsed_ms)
                        per_endpoint_status[ep_label][resp.status_code] += 1
                        if ep_label == "POST /jobs (unique)" and 200 <= resp.status_code < 300:
                            unique_recorded_count += 1
            except httpx.TimeoutException as exc:
                if record:
                    with req_lock:
                        timeouts += 1
                        window_errors += 1
                        per_endpoint_errors[ep_label] += 1
                        if len(error_samples) < 10:
                            error_samples.append(f"TIMEOUT ({ep_label}): {exc}")
            except httpx.HTTPError as exc:
                if record:
                    with req_lock:
                        conn_errors += 1
                        window_errors += 1
                        per_endpoint_errors[ep_label] += 1
                        if len(error_samples) < 10:
                            error_samples.append(f"HTTP_ERR ({ep_label}): {exc}")

        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            if args.warmup > 0:
                warm_interval = 1.0 / args.rps
                warm_count = int(args.rps * args.warmup)
                t_w0 = time.perf_counter()
                warm_futs = []
                for i in range(warm_count):
                    target = t_w0 + i * warm_interval
                    delay = target - time.perf_counter()
                    if delay > 0:
                        time.sleep(delay)
                    warm_futs.append(pool.submit(_execute_one, False))
                for f in warm_futs:
                    f.result()

            interval = 1.0 / args.rps
            total_target = int(args.rps * args.duration)
            measure_start = time.perf_counter()
            futs = []
            for i in range(total_target):
                target = measure_start + i * interval
                delay = target - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
                futs.append(pool.submit(_execute_one, True))
            for f in futs:
                f.result()
            measure_end = time.perf_counter()

    docker_stats_snapshot = _sample_docker_stats()
    restarts_after = _get_container_restarts()
    sample_stop.set()
    sampler_thread.join(timeout=3.0)
    window_thread.join(timeout=3.0)

    measured_seconds = max(0.001, measure_end - measure_start)
    total_recorded = len(latencies_ms) + conn_errors + timeouts
    achieved_rps = round(total_recorded / measured_seconds, 2)

    sorted_lat = sorted(latencies_ms)
    status_2xx = sum(v for k, v in status_counts.items() if 200 <= k < 300)
    status_4xx = sum(v for k, v in status_counts.items() if 400 <= k < 500)
    status_5xx = sum(v for k, v in status_counts.items() if 500 <= k < 600)

    per_endpoint_summary: dict[str, Any] = {}
    for ep_label, ep_lats in sorted(per_endpoint_lat.items()):
        ep_sorted = sorted(ep_lats)
        sc = per_endpoint_status[ep_label]
        per_endpoint_summary[ep_label] = {
            "count": len(ep_sorted),
            "p50_ms": _percentile(ep_sorted, 50),
            "p95_ms": _percentile(ep_sorted, 95),
            "p99_ms": _percentile(ep_sorted, 99),
            "max_ms": round(ep_sorted[-1], 3) if ep_sorted else 0.0,
            "status_2xx": sum(v for k, v in sc.items() if 200 <= k < 300),
            "status_4xx": sum(v for k, v in sc.items() if 400 <= k < 500),
            "status_5xx": sum(v for k, v in sc.items() if 500 <= k < 600),
            "errors": per_endpoint_errors[ep_label],
        }

    # Drain and verify correctness.
    drain_deadline = time.monotonic() + args.drain_timeout
    while time.monotonic() < drain_deadline:
        with engine.connect() as conn:
            active = int(
                conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM jobs
                        WHERE idempotency_key LIKE :prefix
                          AND status IN ('queued', 'running')
                        """
                    ),
                    {"prefix": prefix_like},
                ).scalar_one()
            )
        if active == 0:
            break
        time.sleep(0.5)

    with engine.connect() as conn:
        job_rows = conn.execute(
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
        job_counts = {r["status"]: int(r["cnt"]) for r in job_rows}

        persisted_unique_measured = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM jobs
                    WHERE idempotency_key LIKE :uprefix
                    """
                ),
                {"uprefix": unique_prefix_like},
            ).scalar_one()
        )

        wait_stats = conn.execute(
            text(
                """
                SELECT COALESCE(AVG(EXTRACT(EPOCH FROM (started_at - created_at)) * 1000.0), 0.0) AS avg_wait_ms
                FROM jobs
                WHERE idempotency_key LIKE :prefix
                  AND started_at IS NOT NULL
                """
            ),
            {"prefix": prefix_like},
        ).scalar_one()

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

        inconsistent = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM jobs j
                    LEFT JOIN execution_effects e ON e.job_id = j.id
                    WHERE j.idempotency_key LIKE :prefix
                      AND (
                        (j.status = 'succeeded' AND e.id IS NULL)
                        OR (j.status IN ('cancelled', 'failed') AND e.id IS NOT NULL)
                      )
                    """
                ),
                {"prefix": prefix_like},
            ).scalar_one()
        )

    restart_deltas = {
        name: {
            "running": info["running"],
            "restarts_during_run": info["restart_count"]
            - restarts_before.get(name, {}).get("restart_count", 0),
        }
        for name, info in restarts_after.items()
    }

    ended_at = datetime.now(UTC)
    result: dict[str, Any] = {
        "run_id": run_id,
        "git_revision": _git_revision(),
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "config": {
            "scenario": args.scenario,
            "target_rps": args.rps,
            "duration_seconds": args.duration,
            "warmup_seconds": args.warmup,
            "concurrency": args.concurrency,
            "seed": args.seed,
            "window_seconds": args.window_seconds,
        },
        "http": {
            "total_requests": total_recorded,
            "measured_seconds": round(measured_seconds, 3),
            "achieved_rps": achieved_rps,
            "p50_ms": _percentile(sorted_lat, 50),
            "p95_ms": _percentile(sorted_lat, 95),
            "p99_ms": _percentile(sorted_lat, 99),
            "max_ms": round(sorted_lat[-1], 3) if sorted_lat else 0.0,
            "status_2xx": status_2xx,
            "status_4xx": status_4xx,
            "status_5xx": status_5xx,
            "connection_errors": conn_errors,
            "timeouts": timeouts,
            "error_samples": error_samples,
            "per_endpoint": per_endpoint_summary,
        },
        "postgres": {
            "samples": len(pg_samples),
            "max_active_conns": max((s["active_conns"] for s in pg_samples), default=0),
            "max_total_conns": max((s["total_conns"] for s in pg_samples), default=0),
            "max_lock_waits": max((s["lock_waits"] for s in pg_samples), default=0),
            "max_active_query_ms": max((s["max_active_query_ms"] for s in pg_samples), default=0.0),
        },
        "workers_and_correctness": {
            "job_counts": job_counts,
            "submitted_unique_measured": unique_recorded_count,
            "persisted_unique_measured": persisted_unique_measured,
            "avg_queue_wait_ms": round(float(wait_stats or 0.0), 2),
            "duplicate_effects": dup_effects,
            "inconsistent_states": inconsistent,
        },
        "container_health": restart_deltas,
        "docker_stats_snapshot": docker_stats_snapshot,
        "timeseries": timeseries,
    }

    _log(
        f"Result {run_id}: achieved_rps={achieved_rps}/{args.rps}, "
        f"p50={result['http']['p50_ms']}ms, p95={result['http']['p95_ms']}ms, "
        f"p99={result['http']['p99_ms']}ms, max={result['http']['max_ms']}ms, "
        f"2xx={status_2xx}, 4xx={status_4xx}, 5xx={status_5xx}, "
        f"conn_err={conn_errors}, timeouts={timeouts}, "
        f"pg_max_conns={result['postgres']['max_total_conns']} "
        f"(active={result['postgres']['max_active_conns']}, locks={result['postgres']['max_lock_waits']}), "
        f"unique_sub={unique_recorded_count}/db={persisted_unique_measured}, "
        f"jobs={job_counts}, dup_fx={dup_effects}, inconsistent={inconsistent}"
    )

    json_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
