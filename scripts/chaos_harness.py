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
from collections.abc import Callable
import concurrent.futures
from concurrent.futures import Future, ThreadPoolExecutor, as_completed, wait
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


class ChaosScheduler:
    """Non-blocking chaos scheduler that decouples SIGKILL intervals from worker restart recovery."""

    def __init__(
        self,
        *,
        kill_interval: float,
        rng: random.Random,
        logger: TeeLogger | None = None,
        get_containers_fn: Callable[[], list[str]] = _get_worker_containers,
        get_running_state_fn: Callable[[str], bool] = _get_container_running_state,
        run_cmd_fn: Callable[[list[str]], subprocess.CompletedProcess[str]] = _run_cmd,
        max_kill_workers: int = 4,
        max_recovery_workers: int = 4,
        stop_timeout: float = 3.0,
        recovery_timeout: float = 10.0,
        poll_interval: float = 0.05,
    ) -> None:
        self.kill_interval = kill_interval
        self.rng = rng
        self.logger = logger
        self.get_containers_fn = get_containers_fn
        self.get_running_state_fn = get_running_state_fn
        self.run_cmd_fn = run_cmd_fn
        self.max_kill_workers = max_kill_workers
        self.max_recovery_workers = max_recovery_workers
        self.stop_timeout = stop_timeout
        self.recovery_timeout = recovery_timeout
        self.poll_interval = poll_interval

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._kill_pool = ThreadPoolExecutor(
            max_workers=max_kill_workers, thread_name_prefix="chaos-kill"
        )
        self._recovery_pool = ThreadPoolExecutor(
            max_workers=max_recovery_workers, thread_name_prefix="chaos-recovery"
        )
        self._recovering_containers: set[str] = set()
        self._in_flight_kill_futures: set[Future[Any]] = set()
        self._in_flight_recovery_futures: set[Future[Any]] = set()
        self._lock = threading.Lock()

        self.kill_events: list[dict[str, Any]] = []
        self.restart_events: list[dict[str, Any]] = []
        self.dispatch_events: list[dict[str, Any]] = []
        self.kill_intervals: list[float] = []
        self._dispatched_kills_count: int = 0
        self._last_actual_kill_mono: float | None = None

    @property
    def recovery_events(self) -> list[dict[str, Any]]:
        return self.restart_events

    def _log(self, message: str) -> None:
        if self.logger is not None:
            self.logger.log(message)

    def _on_kill_future_done(self, future: Future[Any]) -> None:
        with self._lock:
            self._in_flight_kill_futures.discard(future)

    def _on_recovery_future_done(self, future: Future[Any]) -> None:
        with self._lock:
            self._in_flight_recovery_futures.discard(future)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._scheduler_loop, daemon=True, name="chaos-scheduler"
        )
        self._thread.start()

    def stop(self, timeout: float = 15.0) -> None:
        start_mono = time.monotonic()
        self._stop_event.set()

        # 1. Join scheduler thread if running
        if self._thread is not None and self._thread.is_alive():
            elapsed = time.monotonic() - start_mono
            remaining = max(0.0, timeout - elapsed)
            self._thread.join(timeout=remaining)
            if self._thread.is_alive():
                raise TimeoutError(
                    f"ChaosScheduler shutdown timed out after {timeout}s: "
                    f"scheduler thread '{self._thread.name}' is still alive."
                )
            self._thread = None

        # 2. Wait for in-flight kill futures
        elapsed = time.monotonic() - start_mono
        remaining = max(0.0, timeout - elapsed)
        with self._lock:
            active_kills = [f for f in self._in_flight_kill_futures if not f.done()]

        if active_kills:
            done, not_done = wait(active_kills, timeout=remaining)
            if not_done:
                self._kill_pool.shutdown(wait=False, cancel_futures=True)
                self._recovery_pool.shutdown(wait=False, cancel_futures=True)
                raise TimeoutError(
                    f"ChaosScheduler shutdown timed out: {len(not_done)} kill task(s) still in-flight."
                )

        self._kill_pool.shutdown(wait=False, cancel_futures=False)

        # 3. Wait for in-flight recovery futures
        elapsed = time.monotonic() - start_mono
        remaining = max(0.0, timeout - elapsed)
        with self._lock:
            active_recoveries = [f for f in self._in_flight_recovery_futures if not f.done()]

        if active_recoveries:
            done, not_done = wait(active_recoveries, timeout=remaining)
            if not_done:
                self._recovery_pool.shutdown(wait=False, cancel_futures=True)
                raise TimeoutError(
                    f"ChaosScheduler shutdown timed out: {len(not_done)} recovery task(s) still in-flight."
                )

        self._recovery_pool.shutdown(wait=False, cancel_futures=False)

    def _scheduler_loop(self) -> None:
        next_tick = time.monotonic() + self.kill_interval
        while not self._stop_event.is_set():
            now = time.monotonic()
            wait_time = next_tick - now
            if wait_time > 0:
                if self._stop_event.wait(timeout=wait_time):
                    break

            if self._stop_event.is_set():
                break

            self._trigger_kill_cycle()

            if self.kill_interval <= 0:
                next_tick = time.monotonic() + 0.001
            else:
                next_tick += self.kill_interval
                now = time.monotonic()
                if next_tick <= now:
                    missed = int((now - next_tick) / self.kill_interval) + 1
                    next_tick += missed * self.kill_interval

    def _trigger_kill_cycle(self) -> None:
        if self._stop_event.is_set():
            return
        try:
            current_containers = self.get_containers_fn()
            with self._lock:
                recovering = set(self._recovering_containers)

            candidates: list[str] = []
            for c in current_containers:
                if c not in recovering and self.get_running_state_fn(c):
                    candidates.append(c)

            if not candidates:
                self._log("CHAOS: No running worker container available for kill (all stopped or recovering).")
                return

            target = self.rng.choice(candidates)
            dispatch_mono = time.monotonic()
            dispatch_ts = datetime.now(UTC).isoformat()

            with self._lock:
                self._recovering_containers.add(target)
                self._dispatched_kills_count += 1
                self.dispatch_events.append({
                    "timestamp": dispatch_ts,
                    "container_id": target[:12],
                    "status": "dispatched",
                })

            try:
                future = self._kill_pool.submit(
                    self._execute_kill, target, dispatch_ts, dispatch_mono
                )
                with self._lock:
                    self._in_flight_kill_futures.add(future)
                future.add_done_callback(self._on_kill_future_done)
            except Exception as submit_exc:
                with self._lock:
                    self._recovering_containers.discard(target)
                    self.dispatch_events.append({
                        "timestamp": datetime.now(UTC).isoformat(),
                        "container_id": target[:12],
                        "status": "dispatch_failed",
                        "error": str(submit_exc),
                    })
                self._log(f"CHAOS SCHEDULER DISPATCH ERROR ({target[:12]}): {submit_exc}")
        except Exception as exc:
            self._log(f"CHAOS SCHEDULER ERROR: {exc}")

    def _execute_kill(
        self, target: str, dispatch_ts: str, dispatch_mono: float | None = None
    ) -> None:
        kill_ts: str | None = None
        kill_success = False
        k_res: subprocess.CompletedProcess[str] | None = None
        kill_exc: Exception | None = None
        try:
            # 1. Record monotonic and UTC timestamps immediately before invoking docker kill
            with self._lock:
                kill_mono = time.monotonic()
                kill_ts = datetime.now(UTC).isoformat()
                interval: float | None = None
                if self._last_actual_kill_mono is not None:
                    interval = round(kill_mono - self._last_actual_kill_mono, 3)
                    self.kill_intervals.append(interval)
                self._last_actual_kill_mono = kill_mono

            # 2. Invoke docker kill
            try:
                k_res = self.run_cmd_fn(["docker", "kill", "-s", "SIGKILL", target])
            except Exception as exc:
                kill_exc = exc

            kill_success = (kill_exc is None) and (k_res is not None and k_res.returncode == 0)
            kill_error: str | None = None
            if kill_exc is not None:
                kill_error = str(kill_exc)
            elif k_res is not None and k_res.returncode != 0:
                kill_error = k_res.stderr.strip() or f"Non-zero exit code {k_res.returncode}"

            with self._lock:
                self.kill_events.append({
                    "timestamp": kill_ts,
                    "dispatch_timestamp": dispatch_ts,
                    "container_id": target[:12],
                    "returncode": k_res.returncode if k_res is not None else None,
                    "interval_seconds": interval,
                    "success": kill_success,
                    "error": kill_error,
                })

            if not kill_success:
                self._log(
                    f"CHAOS SIGKILL FAILED -> {target[:12]} "
                    f"(rc={k_res.returncode if k_res else 'exc'}, error={kill_error})"
                )
                with self._lock:
                    self._recovering_containers.discard(target)
                return

            self._log(f"CHAOS SIGKILL -> {target[:12]} (rc=0, interval={interval}s)")

            # 3. Submit recovery work to the separate _recovery_pool
            try:
                rec_future = self._recovery_pool.submit(
                    self._execute_recovery, target, dispatch_ts, kill_ts
                )
                with self._lock:
                    self._in_flight_recovery_futures.add(rec_future)
                rec_future.add_done_callback(self._on_recovery_future_done)
            except Exception as rec_submit_exc:
                with self._lock:
                    self._recovering_containers.discard(target)
                    self.restart_events.append({
                        "timestamp": datetime.now(UTC).isoformat(),
                        "container_id": target[:12],
                        "restarted": False,
                        "stopped_observed": False,
                        "start_attempted": False,
                        "start_returncode": None,
                        "confirmed_running": False,
                        "status": "recovery_submission_failed",
                        "error": str(rec_submit_exc),
                    })
                self._log(f"CHAOS RECOVERY SUBMIT ERROR ({target[:12]}): {rec_submit_exc}")

        except Exception as exc:
            self._log(f"CHAOS KILL UNCAUGHT ERROR ({target[:12]}): {exc}")
            with self._lock:
                self._recovering_containers.discard(target)

    def _execute_kill_and_recovery(
        self, target: str, dispatch_ts: str, dispatch_mono: float | None = None
    ) -> None:
        """Backward-compatible entry point delegating to decoupled kill and recovery execution."""
        self._execute_kill(target, dispatch_ts, dispatch_mono)

    def _execute_recovery(
        self, target: str, dispatch_ts: str, kill_ts: str | None = None
    ) -> None:
        stopped_observed = False
        start_attempted = False
        start_returncode: int | None = None
        start_exc: Exception | None = None
        confirmed_running = False
        status = "unknown"
        error: str | None = None

        try:
            # 1. Verify stopped state before attempting docker start
            stop_deadline = time.monotonic() + self.stop_timeout
            while time.monotonic() < stop_deadline and not self._stop_event.is_set():
                if not self.get_running_state_fn(target):
                    stopped_observed = True
                    break
                time.sleep(self.poll_interval)

            if not stopped_observed:
                if self._stop_event.is_set():
                    status = "cancelled_by_shutdown"
                    error = "Harness shutdown while waiting for container to stop"
                else:
                    status = "recovery_timeout"
                    error = "Timeout waiting for container to stop after SIGKILL"
                return

            # 2. Check if Docker already auto-restarted the container
            if self.get_running_state_fn(target):
                confirmed_running = True
                status = "success"
                return

            # 3. If shutdown was signaled, abort without attempting docker start
            if self._stop_event.is_set():
                status = "cancelled_by_shutdown"
                error = "Harness shutdown before docker start attempt"
                return

            # 4. Attempt docker start
            start_attempted = True
            try:
                s_res = self.run_cmd_fn(["docker", "start", target])
                start_returncode = s_res.returncode
                if s_res.returncode != 0:
                    start_exc = RuntimeError(
                        s_res.stderr.strip() or f"docker start failed with exit code {s_res.returncode}"
                    )
            except Exception as exc:
                start_exc = exc

            if start_exc is not None or (start_returncode is not None and start_returncode != 0):
                status = "failed_start"
                error = str(start_exc)
                return

            # 5. Confirm container resumed running state within recovery_timeout
            recovery_deadline = time.monotonic() + self.recovery_timeout
            while time.monotonic() < recovery_deadline and not self._stop_event.is_set():
                if self.get_running_state_fn(target):
                    confirmed_running = True
                    break
                time.sleep(self.poll_interval)

            if confirmed_running:
                status = "success"
                error = None
            elif self._stop_event.is_set():
                status = "cancelled_by_shutdown"
                error = "Harness shutdown while waiting for container to resume running state"
            else:
                status = "recovery_timeout"
                error = "Timeout waiting for container to resume running state"

        except Exception as exc:
            status = "unexpected_error"
            error = str(exc)
            self._log(f"CHAOS RECOVERY UNCAUGHT ERROR ({target[:12]}): {exc}")
        finally:
            restarted = (status == "success")
            event = {
                "timestamp": datetime.now(UTC).isoformat(),
                "container_id": target[:12],
                "restarted": restarted,
                "stopped_observed": stopped_observed,
                "start_attempted": start_attempted,
                "start_returncode": start_returncode,
                "confirmed_running": confirmed_running,
                "status": status,
                "error": error,
            }
            with self._lock:
                self.restart_events.append(event)
                self._recovering_containers.discard(target)

            self._log(
                f"CHAOS RECOVERY -> {target[:12]} "
                f"(status={status}, start_attempted={start_attempted}, confirmed_running={confirmed_running})"
            )

    def get_timing_stats(self) -> dict[str, Any]:
        with self._lock:
            intervals = list(self.kill_intervals)
            kill_events = list(self.kill_events)
            restart_events = list(self.restart_events)
            dispatch_count = self._dispatched_kills_count

        successful_kills = sum(1 for e in kill_events if e.get("success") is True)
        failed_kills = sum(1 for e in kill_events if e.get("success") is not True)

        actual_start_attempts = sum(1 for e in restart_events if e.get("start_attempted") is True)
        successful_restarts = sum(1 for e in restart_events if e.get("status") == "success")
        auto_restarts = sum(
            1 for e in restart_events if e.get("status") == "success" and not e.get("start_attempted")
        )
        manual_restarts = sum(
            1 for e in restart_events if e.get("status") == "success" and e.get("start_attempted")
        )
        failed_starts = sum(1 for e in restart_events if e.get("status") == "failed_start")
        recovery_timeouts = sum(1 for e in restart_events if e.get("status") == "recovery_timeout")
        cancelled_by_shutdown = sum(1 for e in restart_events if e.get("status") == "cancelled_by_shutdown")
        unexpected_errors = sum(1 for e in restart_events if e.get("status") == "unexpected_error")
        failed_restarts = failed_starts + recovery_timeouts + unexpected_errors

        return {
            "configured_interval_seconds": self.kill_interval,
            "dispatched_kill_tasks": dispatch_count,
            "actual_kill_attempts": len(kill_events),
            "kill_count": len(kill_events),
            "successful_kills": successful_kills,
            "failed_kills": failed_kills,
            "recovery_events_count": len(restart_events),
            "recovery_count": len(restart_events),
            "actual_start_attempts": actual_start_attempts,
            "restart_attempts": actual_start_attempts,
            "restart_count": actual_start_attempts,
            "successful_restarts": successful_restarts,
            "successful_recoveries": successful_restarts,
            "auto_restarts": auto_restarts,
            "manual_restarts": manual_restarts,
            "failed_restarts": failed_restarts,
            "failed_starts": failed_starts,
            "recovery_timeouts": recovery_timeouts,
            "cancelled_by_shutdown": cancelled_by_shutdown,
            "unexpected_errors": unexpected_errors,
            "execution_intervals": intervals,
            "intervals": intervals,
            "avg_interval_seconds": round(sum(intervals) / len(intervals), 3) if intervals else None,
            "min_interval_seconds": round(min(intervals), 3) if intervals else None,
            "max_interval_seconds": round(max(intervals), 3) if intervals else None,
        }


class ActiveCancellationCohort:
    """Manages dedicated probe jobs to verify active cooperative cancellation of running jobs."""

    def __init__(
        self,
        *,
        base_url: str,
        engine: Any,
        run_id: str,
        logger: TeeLogger | None = None,
        probe_count: int = 8,
        probe_steps: int = 40,
        probe_step_duration_ms: float = 500.0,
        startup_timeout: float = 15.0,
        completion_timeout: float = 30.0,
        max_attempts: int = 5,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url
        self.engine = engine
        self.run_id = run_id
        self.logger = logger
        self.probe_count = probe_count
        self.probe_steps = probe_steps
        self.probe_step_duration_ms = probe_step_duration_ms
        self.startup_timeout = startup_timeout
        self.completion_timeout = completion_timeout
        self.max_attempts = max_attempts
        self._external_client = client

    def _log(self, message: str) -> None:
        if self.logger is not None:
            self.logger.log(message)

    def run(self) -> dict[str, Any]:
        self._log(f"Starting synchronized active cancellation cohort: {self.probe_count} probe jobs...")
        probes: list[dict[str, Any]] = []
        for idx in range(self.probe_count):
            probes.append({
                "probe_index": idx,
                "idempotency_key": f"{self.run_id}:active_cancel_probe:{idx}",
                "job_id": None,
                "submitted": False,
                "confirmed_running": False,
                "cancellation_accepted": False,
                "cancel_response_status": None,
                "final_status": None,
                "effects_count": None,
                "startup_timeout": False,
                "completion_timeout": False,
                "error": None,
            })

        client = self._external_client or httpx.Client(base_url=self.base_url, timeout=10.0)
        barrier_reached = False
        try:
            # 1. Submit all probe jobs concurrently
            def _submit_probe(probe: dict[str, Any]) -> None:
                idx = probe["probe_index"]
                key = probe["idempotency_key"]
                body = {
                    "payload": {
                        "model": "active-cancel-probe",
                        "index": idx,
                        "steps": self.probe_steps,
                        "step_duration_ms": self.probe_step_duration_ms,
                    },
                    "max_attempts": self.max_attempts,
                }
                try:
                    resp = client.post("/jobs", headers={"Idempotency-Key": key}, json=body)
                    if resp.status_code in (200, 201):
                        probe["job_id"] = resp.json()["id"]
                        probe["submitted"] = True
                        self._log(f"Probe {idx} submitted: job_id={probe['job_id']}")
                    else:
                        probe["error"] = f"Submit failed: status={resp.status_code} body={resp.text[:100]}"
                except Exception as exc:
                    probe["error"] = f"Submit exception: {exc}"

            with ThreadPoolExecutor(max_workers=max(1, self.probe_count)) as pool:
                list(pool.map(_submit_probe, probes))

            # 2. Poll until every probe has reached 'running' (synchronization barrier)
            startup_deadline = time.monotonic() + self.startup_timeout
            while time.monotonic() < startup_deadline:
                all_running = True
                premature_abort = False
                for p in probes:
                    if not p["submitted"]:
                        all_running = False
                        premature_abort = True
                        break
                    if p["confirmed_running"]:
                        continue

                    job_id = p["job_id"]
                    try:
                        g_resp = client.get(f"/jobs/{job_id}")
                        if g_resp.status_code == 200:
                            st = g_resp.json().get("status")
                            if st == "running":
                                p["confirmed_running"] = True
                                self._log(f"Probe {p['probe_index']} confirmed running: job_id={job_id}")
                            elif st in ("succeeded", "failed", "cancelled"):
                                p["final_status"] = st
                                p["error"] = f"Premature terminal status before barrier: {st}"
                                all_running = False
                                premature_abort = True
                                break
                            else:
                                all_running = False
                        else:
                            all_running = False
                    except Exception as exc:
                        p["error"] = f"Poll status exception: {exc}"
                        all_running = False

                if premature_abort:
                    break
                if all_running and all(p["confirmed_running"] for p in probes):
                    barrier_reached = True
                    break
                time.sleep(0.05)

            # Record startup timeouts for any probe that didn't reach running
            if not barrier_reached:
                for p in probes:
                    if not p["confirmed_running"] and p.get("error") is None:
                        p["startup_timeout"] = True
                        p["error"] = f"Startup timeout after {self.startup_timeout}s waiting for barrier"
                self._log("Active cancellation barrier NOT reached; aborting cancellation phase.")
            else:
                self._log(f"Barrier reached: all {len(probes)} probes running. Issuing concurrent cancellations...")

                # 3. Issue cancellation requests concurrently
                def _cancel_probe(probe: dict[str, Any]) -> None:
                    job_id = probe["job_id"]
                    try:
                        c_resp = client.post(f"/jobs/{job_id}/cancel")
                        probe["cancel_response_status"] = c_resp.status_code
                        if c_resp.status_code == 200:
                            probe["cancellation_accepted"] = True
                            self._log(f"Probe {probe['probe_index']} cancel accepted (200 OK): job_id={job_id}")
                        else:
                            probe["error"] = f"Cancel failed: status={c_resp.status_code}"
                    except Exception as exc:
                        probe["error"] = f"Cancel exception: {exc}"

                with ThreadPoolExecutor(max_workers=max(1, self.probe_count)) as pool:
                    list(pool.map(_cancel_probe, probes))

                # 4. Verify every probe reaches terminal state
                completion_deadline = time.monotonic() + self.completion_timeout
                while time.monotonic() < completion_deadline:
                    all_terminal = True
                    for p in probes:
                        if p.get("final_status") in ("cancelled", "succeeded", "failed"):
                            continue
                        job_id = p["job_id"]
                        try:
                            g_resp = client.get(f"/jobs/{job_id}")
                            if g_resp.status_code == 200:
                                st = g_resp.json().get("status")
                                if st in ("cancelled", "succeeded", "failed"):
                                    p["final_status"] = st
                                    self._log(f"Probe {p['probe_index']} reached terminal status '{st}': job_id={job_id}")
                                else:
                                    all_terminal = False
                            else:
                                all_terminal = False
                        except Exception:
                            all_terminal = False
                    if all_terminal:
                        break
                    time.sleep(0.05)

                for p in probes:
                    if p.get("final_status") not in ("cancelled", "succeeded", "failed"):
                        p["completion_timeout"] = True
                        p["error"] = f"Completion timeout after {self.completion_timeout}s"

        finally:
            if self._external_client is None:
                client.close()

        # 5. Verify database invariants for all probes
        with self.engine.connect() as conn:
            for p in probes:
                job_id = p.get("job_id")
                if job_id:
                    effects = conn.execute(
                        text("SELECT COUNT(*) FROM execution_effects WHERE job_id = :jid"),
                        {"jid": job_id},
                    ).scalar_one()
                    p["effects_count"] = int(effects)

                    db_status = conn.execute(
                        text("SELECT status FROM jobs WHERE id = :jid"),
                        {"jid": job_id},
                    ).scalar_one_or_none()
                    if db_status:
                        p["final_status"] = db_status
                else:
                    p["effects_count"] = 0

        probes_submitted = sum(1 for p in probes if p["submitted"])
        probes_confirmed_running = sum(1 for p in probes if p["confirmed_running"])
        cancellation_accepted = sum(1 for p in probes if p["cancellation_accepted"])
        jobs_finally_cancelled = sum(1 for p in probes if p["final_status"] == "cancelled")
        cancelled_jobs_with_effects = sum(
            1 for p in probes if p["final_status"] == "cancelled" and (p["effects_count"] or 0) > 0
        )
        startup_timeouts = sum(1 for p in probes if p["startup_timeout"])
        completion_timeouts = sum(1 for p in probes if p["completion_timeout"])

        all_passed = (
            barrier_reached
            and probes_submitted == self.probe_count
            and probes_confirmed_running == self.probe_count
            and cancellation_accepted == self.probe_count
            and jobs_finally_cancelled == self.probe_count
            and cancelled_jobs_with_effects == 0
            and startup_timeouts == 0
            and completion_timeouts == 0
        )

        result = {
            "passed": all_passed,
            "barrier_reached": barrier_reached,
            "probes_submitted": probes_submitted,
            "probes_confirmed_running": probes_confirmed_running,
            "cancellation_requests_accepted": cancellation_accepted,
            "jobs_finally_cancelled": jobs_finally_cancelled,
            "cancelled_jobs_with_effects": cancelled_jobs_with_effects,
            "startup_timeouts": startup_timeouts,
            "completion_timeouts": completion_timeouts,
            "probes": probes,
        }

        status_str = "PASS" if all_passed else "FAIL"
        self._log(
            f"Active cancellation cohort finished [{status_str}]: "
            f"barrier_reached={barrier_reached}, "
            f"submitted={probes_submitted}/{self.probe_count}, "
            f"confirmed_running={probes_confirmed_running}, "
            f"cancel_accepted={cancellation_accepted}, "
            f"finally_cancelled={jobs_finally_cancelled}, "
            f"effects={cancelled_jobs_with_effects}, "
            f"startup_timeouts={startup_timeouts}, "
            f"completion_timeouts={completion_timeouts}"
        )
        return result


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
    parser.add_argument("--active-cancel-probes", type=int, default=8, help="Dedicated active cancellation probe count.")
    parser.add_argument("--active-cancel-steps", type=int, default=40, help="Steps for active cancellation probes.")
    parser.add_argument("--active-cancel-step-ms", type=float, default=500.0, help="Step duration (ms) for probes.")
    parser.add_argument("--max-recovery-workers", type=int, default=4, help="Max workers for restart recovery.")
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
            "active_cancel_probes": args.active_cancel_probes,
            "active_cancel_steps": args.active_cancel_steps,
            "active_cancel_step_ms": args.active_cancel_step_ms,
            "max_recovery_workers": args.max_recovery_workers,
        },
        "worker_containers_observed": [],
        "submitted_jobs": 0,
        "duplicate_requests_sent": 0,
        "duplicate_id_mismatches": 0,
        "conflict_409_probes": 0,
        "conflict_409_failures": 0,
        "cancel_requests_sent": 0,
        "http_errors": [],
        "active_cancellation": {},
        "kill_events": [],
        "restart_events": [],
        "chaos_scheduler_stats": {},
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

        # 3. Start background non-blocking SIGKILL chaos scheduler if enabled.
        scheduler = ChaosScheduler(
            kill_interval=args.kill_interval,
            rng=rng,
            logger=logger,
            get_containers_fn=_get_worker_containers,
            get_running_state_fn=_get_container_running_state,
            run_cmd_fn=_run_cmd,
            max_recovery_workers=args.max_recovery_workers,
        )
        if not args.no_chaos:
            scheduler.start()

        ac_report: dict[str, Any] = {}
        try:
            # 4. Run synchronized active cancellation probe cohort concurrently with chaos.
            probe_count = min(args.workers, args.active_cancel_probes)
            active_cohort = ActiveCancellationCohort(
                base_url=args.base_url,
                engine=engine,
                run_id=run_id,
                logger=logger,
                probe_count=probe_count,
                probe_steps=args.active_cancel_steps,
                probe_step_duration_ms=args.active_cancel_step_ms,
                startup_timeout=15.0,
                completion_timeout=35.0,
                max_attempts=args.max_attempts,
            )
            ac_report = active_cohort.run()
            report["active_cancellation"] = ac_report

            # 5. Submit jobs, duplicate idempotency requests, and concurrent cancellations.
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

            # 6. Wait for queue drain while chaos continues until active jobs drain, then stop chaos.
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
        finally:
            if not args.no_chaos:
                scheduler.stop()
                report["kill_events"] = scheduler.kill_events
                report["restart_events"] = scheduler.restart_events
                report["chaos_scheduler_stats"] = scheduler.get_timing_stats()

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

        # 7. Direct PostgreSQL invariant verification.
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
            {
                "name": "active_cancellation_integrity",
                "passed": bool(ac_report.get("passed", False)),
                "details": (
                    f"barrier_reached={ac_report.get('barrier_reached', False)}, "
                    f"submitted={ac_report.get('probes_submitted', 0)}, "
                    f"confirmed_running={ac_report.get('probes_confirmed_running', 0)}, "
                    f"cancel_accepted={ac_report.get('cancellation_requests_accepted', 0)}, "
                    f"finally_cancelled={ac_report.get('jobs_finally_cancelled', 0)}, "
                    f"effects_count={ac_report.get('cancelled_jobs_with_effects', 0)}, "
                    f"startup_timeouts={ac_report.get('startup_timeouts', 0)}, "
                    f"completion_timeouts={ac_report.get('completion_timeouts', 0)}"
                ),
            },
        ]
        report["invariants"] = invariants
        report["overall_passed"] = all(inv["passed"] for inv in invariants)

        logger.log(f"Final state counts: {report['final_state_counts']}, effects={effects_count}")
        logger.log(f"Kill events: {len(report['kill_events'])}, Restart events: {len(report['restart_events'])}")
        if report.get("chaos_scheduler_stats"):
            stats = report["chaos_scheduler_stats"]
            logger.log(
                f"Chaos scheduler stats: configured={stats['configured_interval_seconds']}s, "
                f"avg_interval={stats['avg_interval_seconds']}s, "
                f"min_interval={stats['min_interval_seconds']}s, "
                f"max_interval={stats['max_interval_seconds']}s, "
                f"kills={stats['successful_kills']}/{stats['kill_count']}, "
                f"restarts={stats['successful_restarts']}/{stats['restart_count']}"
            )
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
