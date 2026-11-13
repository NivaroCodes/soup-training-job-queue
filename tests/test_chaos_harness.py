"""Unit tests for the non-blocking chaos scheduler and active cancellation cohort."""

from __future__ import annotations

import random
import subprocess
import threading
import time
import uuid
from typing import Any
from unittest.mock import MagicMock, call

import httpx
import pytest

from scripts.chaos_harness import (
    ActiveCancellationCohort,
    ChaosScheduler,
)


# -----------------------------------------------------------------------------
# 1. Non-blocking Chaos Scheduling & Lifecycle Tests
# -----------------------------------------------------------------------------


def test_non_blocking_chaos_scheduling() -> None:
    """Verify kill scheduling ticks on interval without waiting for container restart."""
    containers = [f"worker_{i}" for i in range(8)]
    rng = random.Random(42)

    restart_gate = threading.Event()
    killed_containers: set[str] = set()

    def mock_run_cmd(args: list[str]) -> subprocess.CompletedProcess[str]:
        if "kill" in args:
            cid = args[-1]
            killed_containers.add(cid)
            return subprocess.CompletedProcess(args, returncode=0, stdout="", stderr="")
        if "start" in args:
            return subprocess.CompletedProcess(args, returncode=0, stdout="", stderr="")
        return subprocess.CompletedProcess(args, returncode=0, stdout="", stderr="")

    def mock_get_running_state(cid: str) -> bool:
        # A killed container remains un-restarted until restart_gate is set
        if cid in killed_containers and not restart_gate.is_set():
            return False
        return True

    scheduler = ChaosScheduler(
        kill_interval=0.04,
        rng=rng,
        get_containers_fn=lambda: containers,
        get_running_state_fn=mock_get_running_state,
        run_cmd_fn=mock_run_cmd,
        max_recovery_workers=4,
    )

    try:
        scheduler.start()
        # Sleep enough for multiple scheduling ticks (0.04s interval -> ~4-5 ticks in 0.18s)
        time.sleep(0.18)
    finally:
        restart_gate.set()
        scheduler.stop(timeout=2.0)

    stats = scheduler.get_timing_stats()
    # Multiple kills must have fired even though restart was delayed
    assert stats["kill_count"] >= 3, f"Expected >= 3 kills, got {stats['kill_count']}"
    assert len(stats["intervals"]) >= 2
    # Verify intervals are close to 0.04s, definitely not blocked by restart
    for interval in stats["intervals"]:
        assert interval < 0.15, f"Interval {interval} indicates blocking behavior!"


def test_prevention_of_repeated_selection_of_recovering_container() -> None:
    """Verify that a container undergoing recovery is never reselected by the scheduler."""
    containers = ["container_A", "container_B"]
    rng = random.Random(42)

    running_states = {"container_A": True, "container_B": True}

    def mock_get_running_state(cid: str) -> bool:
        return running_states.get(cid, True)

    scheduler = ChaosScheduler(
        kill_interval=1.0,
        rng=rng,
        get_containers_fn=lambda: containers,
        get_running_state_fn=mock_get_running_state,
    )

    # Manually mark container_A as recovering
    with scheduler._lock:
        scheduler._recovering_containers.add("container_A")

    # Trigger cycle: container_A is recovering, only container_B can be picked
    scheduler._trigger_kill_cycle()

    with scheduler._lock:
        assert "container_B" in scheduler._recovering_containers
        assert "container_A" in scheduler._recovering_containers

    # Now both are recovering, triggering cycle again should find no candidates
    scheduler._trigger_kill_cycle()

    scheduler.stop(timeout=1.0)


def test_actual_kill_execution_intervals_not_dispatch_intervals() -> None:
    """Verify kill intervals measure actual docker kill execution start times, not dispatch times."""
    containers = ["c1", "c2"]
    rng = random.Random(42)

    actual_kill_times: list[float] = []
    task1_barrier = threading.Event()

    def mock_run_cmd(args: list[str]) -> subprocess.CompletedProcess[str]:
        if "kill" in args:
            actual_kill_times.append(time.monotonic())
            if args[-1] == "c1":
                task1_barrier.wait(timeout=1.0)
            return subprocess.CompletedProcess(args, returncode=0, stdout="", stderr="")
        return subprocess.CompletedProcess(args, returncode=0, stdout="", stderr="")

    scheduler = ChaosScheduler(
        kill_interval=1.0,
        rng=rng,
        get_containers_fn=lambda: containers,
        get_running_state_fn=lambda c: True,
        run_cmd_fn=mock_run_cmd,
        max_recovery_workers=2,
    )

    # Dispatch c1
    with scheduler._lock:
        scheduler._recovering_containers.add("c1")
        scheduler._dispatched_kills_count += 1
        scheduler.dispatch_events.append({"timestamp": "ts1", "container_id": "c1"})

    f1 = scheduler._kill_pool.submit(
        scheduler._execute_kill, "c1", "2026-11-13T00:00:00Z", time.monotonic()
    )
    with scheduler._lock:
        scheduler._in_flight_kill_futures.add(f1)

    time.sleep(0.08)

    # Dispatch c2
    with scheduler._lock:
        scheduler._recovering_containers.add("c2")
        scheduler._dispatched_kills_count += 1
        scheduler.dispatch_events.append({"timestamp": "ts2", "container_id": "c2"})

    f2 = scheduler._kill_pool.submit(
        scheduler._execute_kill, "c2", "2026-11-13T00:00:00Z", time.monotonic()
    )
    with scheduler._lock:
        scheduler._in_flight_kill_futures.add(f2)

    task1_barrier.set()
    import concurrent.futures
    concurrent.futures.wait([f1, f2], timeout=2.0)

    stats = scheduler.get_timing_stats()
    scheduler.stop(timeout=1.0)

    assert stats["dispatched_kill_tasks"] == 2
    assert stats["actual_kill_attempts"] == 2
    assert len(stats["execution_intervals"]) == 1
    # Both events must contain both dispatch and execution timestamps
    for e in scheduler.kill_events:
        assert "dispatch_timestamp" in e
        assert "timestamp" in e


def test_absolute_cadence_despite_slow_container_discovery() -> None:
    """Verify schedule does not drift due to slow container discovery."""
    containers = [f"c{i}" for i in range(8)]
    rng = random.Random(42)

    def slow_get_containers() -> list[str]:
        time.sleep(0.02)
        return containers

    scheduler = ChaosScheduler(
        kill_interval=0.06,
        rng=rng,
        get_containers_fn=slow_get_containers,
        get_running_state_fn=lambda c: True,
        run_cmd_fn=lambda args: subprocess.CompletedProcess(args, returncode=0),
        max_recovery_workers=4,
        stop_timeout=0.02,
        recovery_timeout=0.02,
        poll_interval=0.01,
    )

    t_start = time.monotonic()
    scheduler.start()
    time.sleep(0.22)
    scheduler.stop(timeout=2.0)

    stats = scheduler.get_timing_stats()
    assert stats["actual_kill_attempts"] >= 3
    for interval in stats["execution_intervals"]:
        assert interval < 0.12, f"Interval {interval} shows schedule drift!"


def test_skipping_missed_ticks_without_burst_execution() -> None:
    """Verify missed ticks are skipped rather than executed in a burst."""
    containers = [f"c{i}" for i in range(8)]
    rng = random.Random(42)

    first_call = True
    def blocking_get_containers() -> list[str]:
        nonlocal first_call
        if first_call:
            first_call = False
            # Block for 0.28s, missing ~2-3 ticks of 0.09s
            time.sleep(0.28)
        return containers

    scheduler = ChaosScheduler(
        kill_interval=0.09,
        rng=rng,
        get_containers_fn=blocking_get_containers,
        get_running_state_fn=lambda c: True,
        run_cmd_fn=lambda args: subprocess.CompletedProcess(args, returncode=0),
        stop_timeout=0.02,
        recovery_timeout=0.02,
        poll_interval=0.01,
    )

    scheduler.start()
    time.sleep(0.42)
    scheduler.stop(timeout=2.0)

    stats = scheduler.get_timing_stats()
    assert stats["actual_kill_attempts"] <= 3, f"Burst execution occurred: {stats['actual_kill_attempts']} kills"
    for interval in stats["execution_intervals"]:
        assert interval >= 0.04, f"Near-zero interval {interval} indicates burst execution!"


def test_failed_kill_returncode_no_restart_attempt() -> None:
    """Verify non-zero docker kill returncode does not attempt restart recovery."""
    rng = random.Random(42)
    start_called = False

    def mock_run_cmd(args: list[str]) -> subprocess.CompletedProcess[str]:
        nonlocal start_called
        if "kill" in args:
            return subprocess.CompletedProcess(args, returncode=137, stdout="", stderr="kill failed")
        if "start" in args:
            start_called = True
            return subprocess.CompletedProcess(args, returncode=0)
        return subprocess.CompletedProcess(args, returncode=0)

    scheduler = ChaosScheduler(
        kill_interval=1.0,
        rng=rng,
        get_containers_fn=lambda: ["c_fail"],
        get_running_state_fn=lambda c: True,
        run_cmd_fn=mock_run_cmd,
    )

    scheduler._trigger_kill_cycle()
    time.sleep(0.1)
    scheduler.stop(timeout=1.0)

    stats = scheduler.get_timing_stats()
    assert stats["actual_kill_attempts"] == 1
    assert stats["failed_kills"] == 1
    assert stats["successful_kills"] == 0
    assert stats["restart_attempts"] == 0
    assert stats["successful_restarts"] == 0
    assert not start_called, "docker start must not be attempted after failed kill!"
    assert "c_fail" not in scheduler._recovering_containers


def test_exception_during_docker_kill() -> None:
    """Verify exception during docker kill is recorded as failed kill without restart."""
    rng = random.Random(42)

    def mock_run_cmd(args: list[str]) -> subprocess.CompletedProcess[str]:
        if "kill" in args:
            raise RuntimeError("docker daemon socket error")
        return subprocess.CompletedProcess(args, returncode=0)

    scheduler = ChaosScheduler(
        kill_interval=1.0,
        rng=rng,
        get_containers_fn=lambda: ["c_exc"],
        get_running_state_fn=lambda c: True,
        run_cmd_fn=mock_run_cmd,
    )

    scheduler._trigger_kill_cycle()
    time.sleep(0.1)
    scheduler.stop(timeout=1.0)

    stats = scheduler.get_timing_stats()
    assert stats["failed_kills"] == 1
    assert stats["successful_kills"] == 0
    assert stats["restart_attempts"] == 0
    assert len(scheduler.kill_events) == 1
    assert "socket error" in (scheduler.kill_events[0]["error"] or "")
    assert "c_exc" not in scheduler._recovering_containers


def test_failed_docker_start() -> None:
    """Verify failed docker start records failed_start and does not claim restart success."""
    rng = random.Random(42)
    running = True

    def mock_get_running_state(cid: str) -> bool:
        return running

    def mock_run_cmd(args: list[str]) -> subprocess.CompletedProcess[str]:
        nonlocal running
        if "kill" in args:
            running = False
            return subprocess.CompletedProcess(args, returncode=0)
        if "start" in args:
            return subprocess.CompletedProcess(args, returncode=1, stderr="container start error")
        return subprocess.CompletedProcess(args, returncode=0)

    scheduler = ChaosScheduler(
        kill_interval=1.0,
        rng=rng,
        get_containers_fn=lambda: ["c_start_fail"],
        get_running_state_fn=mock_get_running_state,
        run_cmd_fn=mock_run_cmd,
        stop_timeout=0.1,
        recovery_timeout=0.2,
    )

    scheduler._trigger_kill_cycle()
    time.sleep(0.15)
    scheduler.stop(timeout=1.0)

    stats = scheduler.get_timing_stats()
    assert stats["successful_kills"] == 1
    assert stats["restart_attempts"] == 1
    assert stats["failed_restarts"] == 1
    assert stats["failed_starts"] == 1
    assert stats["successful_restarts"] == 0

    assert len(scheduler.restart_events) == 1
    event = scheduler.restart_events[0]
    assert event["stopped_observed"] is True
    assert event["start_attempted"] is True
    assert event["start_returncode"] == 1
    assert event["status"] == "failed_start"
    assert event["restarted"] is False
    assert "c_start_fail" not in scheduler._recovering_containers


def test_recovery_timeout() -> None:
    """Verify recovery timeout is recorded when container never resumes running state."""
    rng = random.Random(42)
    running = True

    def mock_get_running_state(cid: str) -> bool:
        return running

    def mock_run_cmd(args: list[str]) -> subprocess.CompletedProcess[str]:
        nonlocal running
        if "kill" in args:
            running = False
            return subprocess.CompletedProcess(args, returncode=0)
        if "start" in args:
            return subprocess.CompletedProcess(args, returncode=0)
        return subprocess.CompletedProcess(args, returncode=0)

    scheduler = ChaosScheduler(
        kill_interval=1.0,
        rng=rng,
        get_containers_fn=lambda: ["c_timeout"],
        get_running_state_fn=mock_get_running_state,
        run_cmd_fn=mock_run_cmd,
        stop_timeout=0.05,
        recovery_timeout=0.08,
        poll_interval=0.02,
    )

    scheduler._trigger_kill_cycle()
    time.sleep(0.2)
    scheduler.stop(timeout=1.0)

    stats = scheduler.get_timing_stats()
    assert stats["successful_kills"] == 1
    assert stats["restart_attempts"] == 1
    assert stats["recovery_timeouts"] == 1
    assert stats["successful_restarts"] == 0

    event = scheduler.restart_events[0]
    assert event["status"] == "recovery_timeout"
    assert event["confirmed_running"] is False
    assert event["restarted"] is False
    assert "c_timeout" not in scheduler._recovering_containers


def test_scheduler_thread_alive_shutdown_timeout() -> None:
    """Verify stop() raises TimeoutError and preserves thread reference if thread does not stop."""
    scheduler = ChaosScheduler(
        kill_interval=1.0,
        rng=random.Random(42),
    )

    unblock = threading.Event()
    def stuck_loop() -> None:
        unblock.wait(timeout=2.0)

    stuck_thread = threading.Thread(target=stuck_loop, name="stuck-chaos-scheduler")
    stuck_thread.start()
    scheduler._thread = stuck_thread

    try:
        with pytest.raises(TimeoutError) as exc_info:
            scheduler.stop(timeout=0.02)
        assert "still alive" in str(exc_info.value)
        assert scheduler._thread is not None
        assert scheduler._thread.is_alive()
    finally:
        unblock.set()
        stuck_thread.join(timeout=1.0)
        scheduler._thread = None
        scheduler._recovery_pool.shutdown(wait=False)


def test_idempotent_repeated_shutdown() -> None:
    """Verify stop() is safe and idempotent to call multiple times."""
    scheduler = ChaosScheduler(
        kill_interval=0.05,
        rng=random.Random(42),
        get_containers_fn=lambda: ["c1"],
        get_running_state_fn=lambda c: True,
        run_cmd_fn=lambda args: subprocess.CompletedProcess(args, returncode=0),
    )

    scheduler.start()
    time.sleep(0.08)
    scheduler.stop(timeout=1.0)

    assert scheduler._thread is None
    # Calling stop a second and third time must not raise
    scheduler.stop(timeout=1.0)
    scheduler.stop(timeout=1.0)


def test_recovery_executor_shutdown_and_inflight_task_completion() -> None:
    """Verify stop() properly awaits completion of in-flight recovery tasks."""
    containers = ["c1"]
    task_started = threading.Event()
    task_done = threading.Event()

    def mock_run_cmd(args: list[str]) -> subprocess.CompletedProcess[str]:
        if "kill" in args:
            task_started.set()
            time.sleep(0.08)
            task_done.set()
            return subprocess.CompletedProcess(args, returncode=0)
        return subprocess.CompletedProcess(args, returncode=0)

    scheduler = ChaosScheduler(
        kill_interval=1.0,
        rng=random.Random(42),
        get_containers_fn=lambda: containers,
        get_running_state_fn=lambda c: True,
        run_cmd_fn=mock_run_cmd,
    )

    scheduler._trigger_kill_cycle()
    assert task_started.wait(timeout=1.0)

    scheduler.stop(timeout=2.0)

    assert task_done.is_set(), "In-flight task should have completed before stop() returned"
    assert "c1" not in scheduler._recovering_containers


def test_executor_submission_failure_lock_release_and_failed_dispatch() -> None:
    """Verify failed pool.submit releases recovering container lock and records failed dispatch."""
    scheduler = ChaosScheduler(
        kill_interval=1.0,
        rng=random.Random(42),
        get_containers_fn=lambda: ["c_submit_fail"],
        get_running_state_fn=lambda c: True,
    )

    def mock_submit_fail(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("simulated pool queue failure")

    scheduler._kill_pool.submit = mock_submit_fail  # type: ignore[assignment]

    scheduler._trigger_kill_cycle()

    assert "c_submit_fail" not in scheduler._recovering_containers

    assert len(scheduler.dispatch_events) >= 1
    failed_event = next(
        (e for e in scheduler.dispatch_events if e.get("status") == "dispatch_failed"), None
    )
    assert failed_event is not None
    assert "simulated pool queue failure" in failed_event.get("error", "")

    stats = scheduler.get_timing_stats()
    assert stats["actual_kill_attempts"] == 0
    assert stats["restart_attempts"] == 0


def test_shutdown_during_stop_polling_classifies_as_cancelled_by_shutdown() -> None:
    """Verify shutdown during container stop polling records cancelled_by_shutdown without start attempt."""
    stop_polled = threading.Event()

    def mock_get_running_state(cid: str) -> bool:
        stop_polled.set()
        return True

    scheduler = ChaosScheduler(
        kill_interval=1.0,
        rng=random.Random(42),
        get_containers_fn=lambda: ["c_stop_abort"],
        get_running_state_fn=mock_get_running_state,
        run_cmd_fn=lambda args: subprocess.CompletedProcess(args, returncode=0),
        stop_timeout=5.0,
        recovery_timeout=5.0,
        poll_interval=0.02,
    )

    scheduler._trigger_kill_cycle()
    assert stop_polled.wait(timeout=1.0)

    scheduler.stop(timeout=2.0)

    assert len(scheduler.restart_events) == 1
    event = scheduler.restart_events[0]
    assert event["status"] == "cancelled_by_shutdown"
    assert event["restarted"] is False
    assert event["stopped_observed"] is False
    assert event["start_attempted"] is False
    assert "c_stop_abort" not in scheduler._recovering_containers

    stats = scheduler.get_timing_stats()
    assert stats["cancelled_by_shutdown"] == 1
    assert stats["recovery_timeouts"] == 0
    assert stats["failed_restarts"] == 0
    assert stats["successful_restarts"] == 0
    assert stats["restart_attempts"] == 0  # No actual docker start attempted!
    assert stats["recovery_count"] == 1


def test_shutdown_during_recovery_polling_classifies_as_cancelled_by_shutdown() -> None:
    """Verify shutdown during restart recovery polling records cancelled_by_shutdown."""
    recovery_polled = threading.Event()
    running_state = [True]

    def mock_get_running_state(cid: str) -> bool:
        if not running_state[0]:
            recovery_polled.set()
        return running_state[0]

    def mock_run_cmd(args: list[str]) -> subprocess.CompletedProcess[str]:
        if "kill" in args:
            running_state[0] = False
            return subprocess.CompletedProcess(args, returncode=0)
        if "start" in args:
            return subprocess.CompletedProcess(args, returncode=0)
        return subprocess.CompletedProcess(args, returncode=0)

    scheduler = ChaosScheduler(
        kill_interval=1.0,
        rng=random.Random(42),
        get_containers_fn=lambda: ["c_recovery_abort"],
        get_running_state_fn=mock_get_running_state,
        run_cmd_fn=mock_run_cmd,
        stop_timeout=0.1,
        recovery_timeout=5.0,
        poll_interval=0.02,
    )

    scheduler._trigger_kill_cycle()
    assert recovery_polled.wait(timeout=1.0)

    scheduler.stop(timeout=2.0)

    assert len(scheduler.restart_events) == 1
    event = scheduler.restart_events[0]
    assert event["status"] == "cancelled_by_shutdown"
    assert event["restarted"] is False
    assert event["stopped_observed"] is True
    assert event["start_attempted"] is True
    assert "c_recovery_abort" not in scheduler._recovering_containers

    stats = scheduler.get_timing_stats()
    assert stats["cancelled_by_shutdown"] == 1
    assert stats["recovery_timeouts"] == 0
    assert stats["failed_restarts"] == 0
    assert stats["successful_restarts"] == 0
    assert stats["restart_attempts"] == 1
    assert stats["recovery_count"] == 1


# -----------------------------------------------------------------------------
# 2. Active Cancellation Cohort Tests
# -----------------------------------------------------------------------------


class MockEngineConnection:
    def __init__(self, effects_count: int = 0, db_status: str = "cancelled") -> None:
        self.effects_count = effects_count
        self.db_status = db_status

    def __enter__(self) -> MockEngineConnection:
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def execute(self, statement: Any, parameters: dict[str, Any] | None = None) -> Any:
        sql = str(statement)
        mock_result = MagicMock()
        if "execution_effects" in sql:
            mock_result.scalar_one.return_value = self.effects_count
        elif "FROM jobs" in sql:
            mock_result.scalar_one_or_none.return_value = self.db_status
        return mock_result


class MockEngine:
    def __init__(self, effects_count: int = 0, db_status: str = "cancelled") -> None:
        self.effects_count = effects_count
        self.db_status = db_status

    def connect(self) -> MockEngineConnection:
        return MockEngineConnection(self.effects_count, self.db_status)


def test_active_cancellation_only_after_job_confirmed_running() -> None:
    """Verify cancellation is NOT issued while job is queued; only after confirmed running."""
    call_log: list[tuple[str, str]] = []
    job_id = str(uuid.uuid4())
    status_sequence = ["queued", "queued", "running", "cancelled"]
    status_iter = iter(status_sequence)

    def mock_handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        method = request.method
        call_log.append((method, url))

        if method == "POST" and url.endswith("/jobs"):
            return httpx.Response(201, json={"id": job_id, "status": "queued"})
        if method == "GET" and f"/jobs/{job_id}" in url:
            curr = next(status_iter, "cancelled")
            return httpx.Response(200, json={"id": job_id, "status": curr})
        if method == "POST" and f"/jobs/{job_id}/cancel" in url:
            # Check that cancellation was called ONLY after at least one running state
            return httpx.Response(200, json={"id": job_id, "status": "running", "cancel_requested": True})
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler)
    client = httpx.Client(base_url="http://testserver", transport=transport)

    cohort = ActiveCancellationCohort(
        base_url="http://testserver",
        engine=MockEngine(effects_count=0, db_status="cancelled"),
        run_id="test_run",
        probe_count=1,
        probe_steps=5,
        probe_step_duration_ms=10.0,
        startup_timeout=2.0,
        completion_timeout=2.0,
        client=client,
    )

    result = cohort.run()

    assert result["passed"] is True
    assert result["probes_confirmed_running"] == 1
    assert result["cancellation_requests_accepted"] == 1
    assert result["jobs_finally_cancelled"] == 1

    # Verify call ordering: POST /jobs, then GET /jobs until running, then POST /cancel
    methods = [m for m, u in call_log]
    assert methods[0] == "POST"  # create
    assert methods[1] == "GET"   # poll queued
    assert methods[2] == "GET"   # poll queued
    assert methods[3] == "GET"   # poll running
    assert methods[4] == "POST"  # cancel AFTER running!


def test_active_cancellation_final_state_verification() -> None:
    """Verify cohort confirms all probes reach cancelled with zero effects."""
    job_ids = [str(uuid.uuid4()) for _ in range(4)]
    job_id_iter = iter(job_ids)

    def mock_handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        method = request.method
        if method == "POST" and url.endswith("/jobs"):
            jid = next(job_id_iter)
            return httpx.Response(201, json={"id": jid, "status": "queued"})
        if method == "GET":
            # Return running then cancelled
            return httpx.Response(200, json={"id": "job", "status": "cancelled"})
        if method == "POST" and "/cancel" in url:
            return httpx.Response(200, json={"id": "job", "status": "running"})
        return httpx.Response(404)

    # For running confirmation, return "running" first
    running_map: dict[str, int] = {}
    def mock_handler_sequence(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        method = request.method
        if method == "POST" and url.endswith("/jobs"):
            jid = str(uuid.uuid4())
            return httpx.Response(201, json={"id": jid, "status": "queued"})
        if method == "GET":
            jid = url.split("/")[-1]
            count = running_map.get(jid, 0)
            running_map[jid] = count + 1
            if count == 0:
                # Add slight delay so cohort execution overlaps with multiple chaos scheduler ticks
                time.sleep(0.06)
                return httpx.Response(200, json={"id": jid, "status": "running"})
            return httpx.Response(200, json={"id": jid, "status": "cancelled"})
        if method == "POST" and "/cancel" in url:
            return httpx.Response(200, json={"id": "job", "status": "running"})
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler_sequence)
    client = httpx.Client(base_url="http://testserver", transport=transport)

    cohort = ActiveCancellationCohort(
        base_url="http://testserver",
        engine=MockEngine(effects_count=0, db_status="cancelled"),
        run_id="test_run",
        probe_count=4,
        probe_steps=5,
        probe_step_duration_ms=10.0,
        startup_timeout=2.0,
        completion_timeout=2.0,
        client=client,
    )

    result = cohort.run()
    assert result["passed"] is True
    assert result["probes_submitted"] == 4
    assert result["probes_confirmed_running"] == 4
    assert result["cancellation_requests_accepted"] == 4
    assert result["jobs_finally_cancelled"] == 4
    assert result["cancelled_jobs_with_effects"] == 0


def test_active_cancellation_zero_execution_effects_violation() -> None:
    """Verify that a probe with non-zero execution effects fails the invariant."""
    def mock_handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        method = request.method
        if method == "POST" and url.endswith("/jobs"):
            return httpx.Response(201, json={"id": "job_with_effect", "status": "queued"})
        if method == "GET":
            return httpx.Response(200, json={"id": "job_with_effect", "status": "cancelled"})
        if method == "POST" and "/cancel" in url:
            return httpx.Response(200, json={"id": "job_with_effect", "status": "running"})
        return httpx.Response(404)

    running_map: dict[str, int] = {}
    def mock_handler_seq(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        method = request.method
        if method == "POST" and url.endswith("/jobs"):
            return httpx.Response(201, json={"id": "job_with_effect", "status": "queued"})
        if method == "GET":
            count = running_map.get("c", 0)
            running_map["c"] = count + 1
            if count == 0:
                return httpx.Response(200, json={"id": "job_with_effect", "status": "running"})
            return httpx.Response(200, json={"id": "job_with_effect", "status": "cancelled"})
        if method == "POST" and "/cancel" in url:
            return httpx.Response(200, json={"id": "job_with_effect", "status": "running"})
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler_seq)
    client = httpx.Client(base_url="http://testserver", transport=transport)

    # Mock DB returns 1 effect for the cancelled job!
    cohort = ActiveCancellationCohort(
        base_url="http://testserver",
        engine=MockEngine(effects_count=1, db_status="cancelled"),
        run_id="test_run",
        probe_count=1,
        probe_steps=5,
        probe_step_duration_ms=10.0,
        startup_timeout=2.0,
        completion_timeout=2.0,
        client=client,
    )

    result = cohort.run()
    assert result["passed"] is False
    assert result["cancelled_jobs_with_effects"] == 1


def test_active_cancellation_startup_timeout_handling() -> None:
    """Verify startup timeout is recorded if probe never transitions to running."""
    def mock_handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        method = request.method
        if method == "POST" and url.endswith("/jobs"):
            return httpx.Response(201, json={"id": "stuck_queued", "status": "queued"})
        if method == "GET":
            # Never transitions to running!
            return httpx.Response(200, json={"id": "stuck_queued", "status": "queued"})
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler)
    client = httpx.Client(base_url="http://testserver", transport=transport)

    cohort = ActiveCancellationCohort(
        base_url="http://testserver",
        engine=MockEngine(effects_count=0, db_status="queued"),
        run_id="test_run",
        probe_count=1,
        probe_steps=5,
        probe_step_duration_ms=10.0,
        startup_timeout=0.2,
        completion_timeout=0.5,
        client=client,
    )

    result = cohort.run()
    assert result["passed"] is False
    assert result["startup_timeouts"] == 1
    assert result["probes_confirmed_running"] == 0
    assert result["cancellation_requests_accepted"] == 0


def test_active_cancellation_completion_timeout_handling() -> None:
    """Verify completion timeout is recorded if probe never reaches terminal state."""
    def mock_handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        method = request.method
        if method == "POST" and url.endswith("/jobs"):
            return httpx.Response(201, json={"id": "stuck_running", "status": "queued"})
        if method == "GET":
            # Always running, never terminal!
            return httpx.Response(200, json={"id": "stuck_running", "status": "running"})
        if method == "POST" and "/cancel" in url:
            return httpx.Response(200, json={"id": "stuck_running", "status": "running"})
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler)
    client = httpx.Client(base_url="http://testserver", transport=transport)

    cohort = ActiveCancellationCohort(
        base_url="http://testserver",
        engine=MockEngine(effects_count=0, db_status="running"),
        run_id="test_run",
        probe_count=1,
        probe_steps=5,
        probe_step_duration_ms=10.0,
        startup_timeout=0.5,
        completion_timeout=0.2,
        client=client,
    )

    result = cohort.run()
    assert result["passed"] is False
    assert result["completion_timeouts"] == 1
    assert result["probes_confirmed_running"] == 1
    assert result["cancellation_requests_accepted"] == 1
    assert result["jobs_finally_cancelled"] == 0


# -----------------------------------------------------------------------------
# 3. Decoupled Execution, Saturation, Synchronization & Counter Tests
# -----------------------------------------------------------------------------


def test_kill_execution_continuing_while_recovery_executor_saturated() -> None:
    """Verify kills continue executing immediately even when the recovery pool is fully saturated."""
    containers = ["c1", "c2", "c3", "c4"]
    rng = random.Random(42)

    recovery_gate = threading.Event()
    kill_executed = threading.Event()
    running_states = {c: True for c in containers}

    def mock_get_running_state(cid: str) -> bool:
        return running_states.get(cid, True)

    def mock_run_cmd(args: list[str]) -> subprocess.CompletedProcess[str]:
        if "kill" in args:
            cid = args[-1]
            running_states[cid] = False
            if cid == "c3":
                kill_executed.set()
            return subprocess.CompletedProcess(args, returncode=0)
        if "start" in args:
            cid = args[-1]
            if cid in ("c1", "c2"):
                # Saturated recovery workers block here
                recovery_gate.wait(timeout=2.0)
            running_states[cid] = True
            return subprocess.CompletedProcess(args, returncode=0)
        return subprocess.CompletedProcess(args, returncode=0)

    # 2 recovery workers, 2 kill workers
    scheduler = ChaosScheduler(
        kill_interval=1.0,
        rng=rng,
        get_containers_fn=lambda: containers,
        get_running_state_fn=mock_get_running_state,
        run_cmd_fn=mock_run_cmd,
        max_kill_workers=2,
        max_recovery_workers=2,
        stop_timeout=0.05,
        recovery_timeout=0.2,
        poll_interval=0.01,
    )

    try:
        # 1. Dispatch c1 and c2 into recovery - saturating all 2 recovery workers
        with scheduler._lock:
            scheduler._recovering_containers.add("c1")
            scheduler._dispatched_kills_count += 1
            scheduler._recovering_containers.add("c2")
            scheduler._dispatched_kills_count += 1

        f1 = scheduler._kill_pool.submit(scheduler._execute_kill, "c1", "ts1", time.monotonic())
        f2 = scheduler._kill_pool.submit(scheduler._execute_kill, "c2", "ts2", time.monotonic())
        f1.result(timeout=1.0)
        f2.result(timeout=1.0)

        # Give a moment for c1 and c2 recovery tasks to start and block in _recovery_pool
        time.sleep(0.08)

        # 2. Both recovery workers are saturated. Now dispatch c3!
        with scheduler._lock:
            scheduler._recovering_containers.add("c3")
            scheduler._dispatched_kills_count += 1

        # c3 kill must execute on _kill_pool immediately, without waiting for c1/c2 recovery!
        f3 = scheduler._kill_pool.submit(scheduler._execute_kill, "c3", "ts3", time.monotonic())
        f3.result(timeout=1.0)
        assert kill_executed.is_set(), "Kill for c3 should execute immediately on _kill_pool!"

        stats = scheduler.get_timing_stats()
        assert stats["actual_kill_attempts"] == 3
        assert stats["successful_kills"] == 3

    finally:
        recovery_gate.set()
        scheduler.stop(timeout=2.0)


def test_bounded_inflight_work_and_cleanup_on_completion() -> None:
    """Verify in-flight kill and recovery futures sets are cleanly bounded and pruned on completion."""
    containers = ["c1", "c2"]
    rng = random.Random(42)

    scheduler = ChaosScheduler(
        kill_interval=1.0,
        rng=rng,
        get_containers_fn=lambda: containers,
        get_running_state_fn=lambda c: True,
        run_cmd_fn=lambda args: subprocess.CompletedProcess(args, returncode=0),
        stop_timeout=0.02,
        recovery_timeout=0.02,
        poll_interval=0.01,
    )

    scheduler._trigger_kill_cycle()
    time.sleep(0.1)
    scheduler.stop(timeout=1.0)

    # In-flight sets must be cleaned up
    with scheduler._lock:
        assert len(scheduler._in_flight_kill_futures) == 0
        assert len(scheduler._in_flight_recovery_futures) == 0
        assert len(scheduler._recovering_containers) == 0


def test_all_probes_confirmed_running_before_any_cancellation_begins() -> None:
    """Deterministic barrier test: prove every probe is confirmed running before ANY cancel request is issued."""
    event_log: list[tuple[str, str, float]] = []
    log_lock = threading.Lock()

    job_ids = [f"job_{i}" for i in range(4)]
    poll_counts: dict[str, int] = {jid: 0 for jid in job_ids}
    cancelled_jids: set[str] = set()

    def mock_handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        method = request.method

        if method == "POST" and url.endswith("/jobs"):
            body = request.read().decode("utf-8")
            idx = int(body.split('"index":')[1].split(",")[0].strip())
            jid = job_ids[idx]
            return httpx.Response(201, json={"id": jid, "status": "queued"})

        if method == "GET" and "/jobs/" in url:
            jid = url.split("/")[-1]
            with log_lock:
                if jid in cancelled_jids:
                    return httpx.Response(200, json={"id": jid, "status": "cancelled"})

                poll_counts[jid] += 1
                count = poll_counts[jid]

            # Stagger transition to running:
            # probe 0 running on 1st poll, probe 1 on 2nd, probe 2 on 3rd, probe 3 on 4th
            idx = job_ids.index(jid)
            if count <= idx:
                return httpx.Response(200, json={"id": jid, "status": "queued"})

            with log_lock:
                event_log.append(("RUNNING", jid, time.monotonic()))
            return httpx.Response(200, json={"id": jid, "status": "running"})

        if method == "POST" and "/cancel" in url:
            jid = url.split("/")[-2]
            with log_lock:
                cancelled_jids.add(jid)
                event_log.append(("CANCEL", jid, time.monotonic()))
            return httpx.Response(200, json={"id": jid, "status": "running"})

        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler)
    client = httpx.Client(base_url="http://testserver", transport=transport)

    cohort = ActiveCancellationCohort(
        base_url="http://testserver",
        engine=MockEngine(effects_count=0, db_status="cancelled"),
        run_id="sync_barrier_test",
        probe_count=4,
        probe_steps=5,
        probe_step_duration_ms=10.0,
        startup_timeout=2.0,
        completion_timeout=2.0,
        client=client,
    )

    result = cohort.run()

    assert result["passed"] is True
    assert result["barrier_reached"] is True
    assert result["probes_confirmed_running"] == 4
    assert result["cancellation_requests_accepted"] == 4

    running_events = [(jid, t) for ev, jid, t in event_log if ev == "RUNNING"]
    cancel_events = [(jid, t) for ev, jid, t in event_log if ev == "CANCEL"]

    # Exactly 4 running observations and 4 cancel requests
    running_jids = set(jid for jid, t in running_events)
    assert running_jids == set(job_ids)
    assert len(cancel_events) == 4

    latest_running_ts = max(t for jid, t in running_events)
    earliest_cancel_ts = min(t for jid, t in cancel_events)

    # Invariant: Every probe confirmed running BEFORE the first cancel request was issued!
    assert latest_running_ts <= earliest_cancel_ts, (
        f"Barrier violated: latest running ({latest_running_ts}) > earliest cancel ({earliest_cancel_ts})"
    )


def test_startup_timeout_at_synchronization_barrier() -> None:
    """Verify startup timeout at barrier records timeout for stalled probes and sends zero cancel requests."""
    cancel_calls: list[str] = []

    def mock_handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        method = request.method

        if method == "POST" and url.endswith("/jobs"):
            body = request.read().decode("utf-8")
            idx = int(body.split('"index":')[1].split(",")[0].strip())
            return httpx.Response(201, json={"id": f"p_{idx}", "status": "queued"})

        if method == "GET" and "/jobs/" in url:
            jid = url.split("/")[-1]
            # p_0 and p_1 reach running, but p_2 is stuck in queued!
            if jid in ("p_0", "p_1"):
                return httpx.Response(200, json={"id": jid, "status": "running"})
            return httpx.Response(200, json={"id": jid, "status": "queued"})

        if method == "POST" and "/cancel" in url:
            cancel_calls.append(url)
            return httpx.Response(200, json={"id": "job", "status": "running"})

        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler)
    client = httpx.Client(base_url="http://testserver", transport=transport)

    cohort = ActiveCancellationCohort(
        base_url="http://testserver",
        engine=MockEngine(effects_count=0, db_status="queued"),
        run_id="barrier_timeout_test",
        probe_count=3,
        probe_steps=5,
        probe_step_duration_ms=10.0,
        startup_timeout=0.15,
        completion_timeout=0.5,
        client=client,
    )

    result = cohort.run()

    assert result["passed"] is False
    assert result["barrier_reached"] is False
    assert result["probes_confirmed_running"] == 2
    assert result["startup_timeouts"] == 1
    # Crucial: No cancellation requests must be issued if the barrier was not reached!
    assert len(cancel_calls) == 0
    assert result["cancellation_requests_accepted"] == 0

    p2 = next(p for p in result["probes"] if p["probe_index"] == 2)
    assert p2["startup_timeout"] is True
    assert "waiting for barrier" in (p2["error"] or "")


def test_active_cancellation_overlapping_with_scheduler_execution() -> None:
    """Verify active cancellation cohort runs concurrently with active chaos scheduler."""
    containers = [f"worker_{i}" for i in range(4)]
    rng = random.Random(42)

    scheduler = ChaosScheduler(
        kill_interval=0.03,
        rng=rng,
        get_containers_fn=lambda: containers,
        get_running_state_fn=lambda c: True,
        run_cmd_fn=lambda args: subprocess.CompletedProcess(args, returncode=0),
        stop_timeout=0.02,
        recovery_timeout=0.02,
        poll_interval=0.01,
    )

    running_map: dict[str, int] = {}
    def mock_handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        method = request.method
        if method == "POST" and url.endswith("/jobs"):
            jid = str(uuid.uuid4())
            return httpx.Response(201, json={"id": jid, "status": "queued"})
        if method == "GET":
            jid = url.split("/")[-1]
            count = running_map.get(jid, 0)
            running_map[jid] = count + 1
            if count == 0:
                # Add slight delay so cohort execution overlaps with multiple chaos scheduler ticks
                time.sleep(0.06)
                return httpx.Response(200, json={"id": jid, "status": "running"})
            return httpx.Response(200, json={"id": jid, "status": "cancelled"})
        if method == "POST" and "/cancel" in url:
            return httpx.Response(200, json={"id": "job", "status": "running"})
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler)
    client = httpx.Client(base_url="http://testserver", transport=transport)

    cohort = ActiveCancellationCohort(
        base_url="http://testserver",
        engine=MockEngine(effects_count=0, db_status="cancelled"),
        run_id="concurrent_overlap_test",
        probe_count=2,
        probe_steps=5,
        probe_step_duration_ms=10.0,
        startup_timeout=1.0,
        completion_timeout=1.0,
        client=client,
    )

    scheduler.start()
    try:
        ac_result = cohort.run()
    finally:
        scheduler.stop(timeout=2.0)

    stats = scheduler.get_timing_stats()
    # Both active cancellation passed and chaos kills were fired in parallel
    assert ac_result["passed"] is True
    assert ac_result["barrier_reached"] is True
    assert stats["actual_kill_attempts"] >= 1
    assert scheduler._thread is None


def test_correct_start_attempt_counters_all_scenarios() -> None:
    """Verify distinct start-attempt vs recovery counters across all 5 recovery lifecycle scenarios."""
    scheduler = ChaosScheduler(
        kill_interval=1.0,
        rng=random.Random(42),
    )

    # Scenario 1: Recovery timeout without start attempt (stopped never observed)
    scheduler.stop_timeout = 0.02
    scheduler.get_running_state_fn = lambda c: True  # never stops
    scheduler._execute_recovery("c_stop_timeout", "ts1")

    # Scenario 2: Shutdown cancellation without start attempt
    scheduler._stop_event.set()
    scheduler._execute_recovery("c_shutdown_nostart", "ts2")
    scheduler._stop_event.clear()

    # Scenario 3: Successful Docker auto-restart without manual start
    # Container stops (returns False once) then auto-restarts (returns True)
    c3_state = [False, True]
    scheduler.get_running_state_fn = lambda c: c3_state.pop(0) if c3_state else True
    scheduler._execute_recovery("c_autorestart", "ts3")

    # Scenario 4: Failed manual start
    c4_state = [False, False]
    scheduler.get_running_state_fn = lambda c: c4_state.pop(0) if c4_state else False
    scheduler.run_cmd_fn = lambda args: subprocess.CompletedProcess(args, returncode=1, stderr="exec error")
    scheduler._execute_recovery("c_failed_start", "ts4")

    # Scenario 5: Successful manual start
    c5_state = [False, False, True]
    scheduler.get_running_state_fn = lambda c: c5_state.pop(0) if c5_state else True
    scheduler.run_cmd_fn = lambda args: subprocess.CompletedProcess(args, returncode=0)
    scheduler.recovery_timeout = 0.1
    scheduler._execute_recovery("c_success_start", "ts5")

    stats = scheduler.get_timing_stats()

    # 5 recovery events total
    assert stats["recovery_count"] == 5

    # Exactly 2 manual start attempts (Scenario 4 and 5)
    assert stats["restart_attempts"] == 2
    assert stats["actual_start_attempts"] == 2

    # Auto restart (Scenario 3) + manual restart (Scenario 5) = 2 successful recoveries
    assert stats["auto_restarts"] == 1
    assert stats["manual_restarts"] == 1
    assert stats["successful_restarts"] == 2
    assert stats["successful_recoveries"] == 2

    # Failed starts (Scenario 4) = 1
    assert stats["failed_starts"] == 1

    # Recovery timeouts without start (Scenario 1) = 1
    assert stats["recovery_timeouts"] == 1

    # Cancelled by shutdown without start (Scenario 2) = 1
    assert stats["cancelled_by_shutdown"] == 1

    # Total failed restarts = failed_starts (1) + recovery_timeouts (1) = 2
    assert stats["failed_restarts"] == 2


def test_unexpected_recovery_exceptions() -> None:
    """Verify unhandled exceptions in recovery are recorded as unexpected_error and lock is released."""
    scheduler = ChaosScheduler(
        kill_interval=1.0,
        rng=random.Random(42),
    )

    def failing_running_state(cid: str) -> bool:
        raise RuntimeError("simulated kernel metadata corruption")

    scheduler.get_running_state_fn = failing_running_state

    with scheduler._lock:
        scheduler._recovering_containers.add("c_corrupt")

    scheduler._execute_recovery("c_corrupt", "ts1")

    # Lock must be released
    assert "c_corrupt" not in scheduler._recovering_containers

    assert len(scheduler.restart_events) == 1
    event = scheduler.restart_events[0]
    assert event["status"] == "unexpected_error"
    assert "simulated kernel metadata corruption" in (event["error"] or "")

    stats = scheduler.get_timing_stats()
    assert stats["unexpected_errors"] == 1
    assert stats["failed_restarts"] == 1


def test_shutdown_of_both_executors_and_timeout() -> None:
    """Verify stop() handles both _kill_pool and _recovery_pool, raising TimeoutError on stuck tasks."""
    scheduler = ChaosScheduler(
        kill_interval=1.0,
        rng=random.Random(42),
    )

    unblock = threading.Event()
    def stuck_task() -> None:
        unblock.wait(timeout=2.0)

    # Submit stuck task into recovery pool
    f = scheduler._recovery_pool.submit(stuck_task)
    with scheduler._lock:
        scheduler._in_flight_recovery_futures.add(f)

    try:
        with pytest.raises(TimeoutError) as exc_info:
            scheduler.stop(timeout=0.03)
        assert "recovery task(s) still in-flight" in str(exc_info.value)
    finally:
        unblock.set()
        scheduler.stop(timeout=1.0)
