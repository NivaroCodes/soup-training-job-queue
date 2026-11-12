"""PostgreSQL integration tests for WorkerRunner, leases, heartbeats, fencing, and recovery."""

from __future__ import annotations

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session, sessionmaker

from app.db import ExecutionEffect, Job, JobStatus
from app.repositories.worker_repository import WorkerRepository
from app.services.job_service import JobService
from app.worker.heartbeat import LeaseHeartbeat
from app.worker.runner import WorkerRunner


def _create_queued_job(
    session: Session,
    *,
    payload: dict | None = None,
    max_attempts: int = 3,
) -> Job:
    job, created = JobService.create_job(
        session,
        idempotency_key=f"worker-test-{uuid.uuid4()}",
        payload=payload if payload is not None else {"model": "resnet", "steps": 2, "step_duration_ms": 0},
        max_attempts=max_attempts,
    )
    assert created is True
    return job


def test_multiple_workers_claim_different_jobs_and_no_double_claim(
    session_factory: sessionmaker[Session],
    db_session: Session,
) -> None:
    jobs = [_create_queued_job(db_session) for _ in range(4)]
    expected_ids = {j.id for j in jobs}

    barrier = threading.Barrier(4)

    def _claim(idx: int) -> uuid.UUID | None:
        barrier.wait(timeout=5.0)
        with session_factory() as s:
            claimed = WorkerRepository.claim_next_job(
                s, worker_id=f"worker-{idx}", lease_ttl_seconds=30.0
            )
            return claimed.job_id if claimed is not None else None

    with ThreadPoolExecutor(max_workers=4) as pool:
        claimed_ids = list(pool.map(_claim, range(4)))

    assert all(cid is not None for cid in claimed_ids)
    assert set(claimed_ids) == expected_ids


def test_four_workers_racing_on_single_job_no_double_claim(
    session_factory: sessionmaker[Session],
    db_session: Session,
) -> None:
    job = _create_queued_job(db_session)
    barrier = threading.Barrier(4)

    def _claim(idx: int) -> uuid.UUID | None:
        barrier.wait(timeout=5.0)
        with session_factory() as s:
            claimed = WorkerRepository.claim_next_job(
                s, worker_id=f"racer-{idx}", lease_ttl_seconds=30.0
            )
            return claimed.job_id if claimed is not None else None

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(_claim, range(4)))

    non_none = [r for r in results if r is not None]
    assert non_none == [job.id]


def test_fencing_token_increments_across_claims_and_recovery(
    session_factory: sessionmaker[Session],
    db_session: Session,
) -> None:
    job = _create_queued_job(db_session, max_attempts=3)

    with session_factory() as s1:
        c1 = WorkerRepository.claim_next_job(
            s1, worker_id="w-1", lease_ttl_seconds=30.0
        )
        assert c1 is not None
        assert c1.fencing_token == 1
        assert c1.attempt == 1

    # Simulate lease expiration and recovery.
    db_session.execute(
        text(
            "UPDATE jobs SET lease_expires_at = clock_timestamp() - interval '5 seconds' WHERE id = :id"
        ),
        {"id": job.id},
    )
    db_session.commit()

    with session_factory() as sr:
        stats = WorkerRepository.recover_expired_jobs(sr)
        assert stats.requeued == 1

    # Recovery incremented fencing_token to 2; next claim increments to 3.
    with session_factory() as s2:
        c2 = WorkerRepository.claim_next_job(
            s2, worker_id="w-2", lease_ttl_seconds=30.0
        )
        assert c2 is not None
        assert c2.fencing_token == 3
        assert c2.attempt == 2


def test_valid_heartbeat_runs_independently_during_blocking_execution(
    session_factory: sessionmaker[Session],
    db_session: Session,
) -> None:
    job = _create_queued_job(
        db_session,
        payload={"model": "blocked-sim", "steps": 1, "step_duration_ms": 0},
    )

    execution_entered = threading.Event()
    allow_finish = threading.Event()

    def _blocking_executor(
        payload: dict,
        cancel_event: threading.Event,
        lease_lost_event: threading.Event,
    ) -> dict:
        execution_entered.set()
        assert allow_finish.wait(timeout=5.0)
        return {"blocked_ok": True}

    runner = WorkerRunner(
        session_factory=session_factory,
        worker_id="hb-worker",
        lease_ttl_seconds=5.0,
        heartbeat_interval_seconds=0.05,
        executor=_blocking_executor,
    )

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(runner.run_once)
        assert execution_entered.wait(timeout=5.0)

        # Wait deterministically until PostgreSQL observes at least one heartbeat update
        # while the executor thread is still blocked on allow_finish.
        initial_hb = db_session.scalar(
            select(Job.last_heartbeat_at).where(Job.id == job.id)
        )
        advanced = False
        for _ in range(100):
            db_session.expire_all()
            current_hb = db_session.scalar(
                select(Job.last_heartbeat_at).where(Job.id == job.id)
            )
            if current_hb is not None and initial_hb is not None and current_hb > initial_hb:
                advanced = True
                break
            threading.Event().wait(0.03)

        assert advanced is True
        allow_finish.set()
        assert future.result(timeout=5.0) is True

    db_session.expire_all()
    finished = db_session.get(Job, job.id)
    assert finished is not None
    assert finished.status == JobStatus.SUCCEEDED.value
    assert finished.execution_effect is not None
    assert finished.execution_effect.effect_payload == {"blocked_ok": True}


def test_expired_lease_rejects_heartbeat_completion_failure_and_cancellation(
    session_factory: sessionmaker[Session],
    db_session: Session,
) -> None:
    job = _create_queued_job(db_session)
    with session_factory() as s:
        claimed = WorkerRepository.claim_next_job(
            s, worker_id="expiring-w", lease_ttl_seconds=30.0
        )
        assert claimed is not None

    # Expire the lease in PostgreSQL without running recovery yet.
    db_session.execute(
        text(
            "UPDATE jobs SET lease_expires_at = clock_timestamp() - interval '2 seconds', cancel_requested = TRUE WHERE id = :id"
        ),
        {"id": job.id},
    )
    db_session.commit()

    with session_factory() as s:
        renewed, _ = WorkerRepository.renew_heartbeat(
            s,
            job_id=job.id,
            worker_id="expiring-w",
            fencing_token=claimed.fencing_token,
            lease_ttl_seconds=30.0,
        )
        assert renewed is False

        assert (
            WorkerRepository.complete_cooperative_cancellation(
                s,
                job_id=job.id,
                worker_id="expiring-w",
                fencing_token=claimed.fencing_token,
            )
            is False
        )
        assert (
            WorkerRepository.fail_job_non_retryable(
                s,
                job_id=job.id,
                worker_id="expiring-w",
                fencing_token=claimed.fencing_token,
                error_message="late fail",
            )
            is False
        )
        assert (
            WorkerRepository.handle_retryable_failure(
                s,
                job_id=job.id,
                worker_id="expiring-w",
                fencing_token=claimed.fencing_token,
                error_message="late retry",
            )
            == "aborted_stale"
        )

    # Even if cancel_requested is FALSE, completion must still fail on expired lease.
    db_session.execute(
        text("UPDATE jobs SET cancel_requested = FALSE WHERE id = :id"),
        {"id": job.id},
    )
    db_session.commit()

    with session_factory() as s:
        assert (
            WorkerRepository.complete_job_atomically(
                s,
                job_id=job.id,
                worker_id="expiring-w",
                fencing_token=claimed.fencing_token,
                effect_payload={"stale": True},
            )
            is False
        )

    effect_count = db_session.scalar(
        select(func.count(ExecutionEffect.id)).where(ExecutionEffect.job_id == job.id)
    )
    assert effect_count == 0


def test_zombie_worker_rejected_after_recovery_and_new_worker_completion(
    session_factory: sessionmaker[Session],
    db_session: Session,
) -> None:
    job = _create_queued_job(db_session, max_attempts=3)

    with session_factory() as s1:
        zombie_claim = WorkerRepository.claim_next_job(
            s1, worker_id="zombie-worker", lease_ttl_seconds=30.0
        )
        assert zombie_claim is not None

    # Expire zombie lease, recover, and let healthy worker-2 claim and complete.
    db_session.execute(
        text(
            "UPDATE jobs SET lease_expires_at = clock_timestamp() - interval '5 seconds' WHERE id = :id"
        ),
        {"id": job.id},
    )
    db_session.commit()

    with session_factory() as sr:
        stats = WorkerRepository.recover_expired_jobs(sr)
        assert stats.requeued == 1

    with session_factory() as s2:
        fresh_claim = WorkerRepository.claim_next_job(
            s2, worker_id="healthy-worker", lease_ttl_seconds=30.0
        )
        assert fresh_claim is not None
        assert fresh_claim.fencing_token > zombie_claim.fencing_token
        assert (
            WorkerRepository.complete_job_atomically(
                s2,
                job_id=job.id,
                worker_id="healthy-worker",
                fencing_token=fresh_claim.fencing_token,
                effect_payload={"winner": "healthy-worker"},
            )
            is True
        )

    # Zombie wakes up and attempts every mutation with stale token.
    with session_factory() as sz:
        renewed, _ = WorkerRepository.renew_heartbeat(
            sz,
            job_id=job.id,
            worker_id="zombie-worker",
            fencing_token=zombie_claim.fencing_token,
            lease_ttl_seconds=30.0,
        )
        assert renewed is False
        assert (
            WorkerRepository.complete_job_atomically(
                sz,
                job_id=job.id,
                worker_id="zombie-worker",
                fencing_token=zombie_claim.fencing_token,
                effect_payload={"winner": "zombie-worker"},
            )
            is False
        )
        assert (
            WorkerRepository.fail_job_non_retryable(
                sz,
                job_id=job.id,
                worker_id="zombie-worker",
                fencing_token=zombie_claim.fencing_token,
                error_message="zombie fail",
            )
            is False
        )

    db_session.expire_all()
    final_job = db_session.get(Job, job.id)
    assert final_job is not None
    assert final_job.status == JobStatus.SUCCEEDED.value
    assert final_job.execution_effect is not None
    assert final_job.execution_effect.worker_id == "healthy-worker"
    assert final_job.execution_effect.effect_payload == {"winner": "healthy-worker"}


def test_successful_atomic_effect_and_no_duplicate_committed_effects(
    session_factory: sessionmaker[Session],
    db_session: Session,
) -> None:
    job = _create_queued_job(
        db_session,
        payload={"model": "vit", "steps": 3, "step_duration_ms": 0},
    )

    runner = WorkerRunner(session_factory=session_factory, worker_id="exec-worker")
    assert runner.run_once() is True

    db_session.expire_all()
    completed = db_session.get(Job, job.id)
    assert completed is not None
    assert completed.status == JobStatus.SUCCEEDED.value
    assert completed.worker_id is None
    assert completed.lease_expires_at is None
    assert completed.completed_at is not None
    assert completed.execution_effect is not None
    assert completed.execution_effect.worker_id == "exec-worker"
    assert completed.execution_effect.fencing_token == 1
    assert completed.execution_effect.effect_payload["steps_completed"] == 3

    # Attempting a second completion commit must fail and leave exactly 1 effect.
    with session_factory() as s:
        assert (
            WorkerRepository.complete_job_atomically(
                s,
                job_id=job.id,
                worker_id="exec-worker",
                fencing_token=1,
                effect_payload={"duplicate": True},
            )
            is False
        )

    count = db_session.scalar(
        select(func.count(ExecutionEffect.id)).where(ExecutionEffect.job_id == job.id)
    )
    assert count == 1


def test_cooperative_cancellation_aborts_without_effect(
    session_factory: sessionmaker[Session],
    db_session: Session,
) -> None:
    job = _create_queued_job(db_session)
    started = threading.Event()

    def _cancellable_executor(
        payload: dict,
        cancel_event: threading.Event,
        lease_lost_event: threading.Event,
    ) -> dict:
        from app.worker.executor import JobCancelledError

        started.set()
        assert cancel_event.wait(timeout=5.0)
        raise JobCancelledError("Observed cancel_requested")

    runner = WorkerRunner(
        session_factory=session_factory,
        worker_id="cancel-worker",
        lease_ttl_seconds=30.0,
        heartbeat_interval_seconds=0.05,
        executor=_cancellable_executor,
    )

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(runner.run_once)
        assert started.wait(timeout=5.0)

        # Request cancellation via JobService (API path) while worker is running.
        with session_factory() as api_sess:
            updated = JobService.cancel_job(api_sess, job.id)
            assert updated.status == JobStatus.RUNNING.value
            assert updated.cancel_requested is True

        assert future.result(timeout=5.0) is True

    db_session.expire_all()
    cancelled = db_session.get(Job, job.id)
    assert cancelled is not None
    assert cancelled.status == JobStatus.CANCELLED.value
    assert cancelled.cancelled_at is not None
    assert cancelled.worker_id is None
    assert cancelled.lease_expires_at is None
    assert cancelled.execution_effect is None


def test_retryable_failure_precedence_and_requeue_to_failed(
    session_factory: sessionmaker[Session],
    db_session: Session,
) -> None:
    job = _create_queued_job(
        db_session,
        payload={"model": "flaky", "steps": 1, "step_duration_ms": 0, "simulate_failure": "retryable"},
        max_attempts=2,
    )
    runner = WorkerRunner(session_factory=session_factory, worker_id="retry-worker")

    # Attempt 1 (< max_attempts=2) -> requeued to queued.
    assert runner.run_once() is True
    db_session.expire_all()
    after_1 = db_session.get(Job, job.id)
    assert after_1 is not None
    assert after_1.status == JobStatus.QUEUED.value
    assert after_1.attempt == 1
    assert after_1.last_error == "Simulated retryable training failure."

    # Attempt 2 (== max_attempts=2) -> transitions to failed.
    assert runner.run_once() is True
    db_session.expire_all()
    after_2 = db_session.get(Job, job.id)
    assert after_2 is not None
    assert after_2.status == JobStatus.FAILED.value
    assert after_2.attempt == 2
    assert after_2.completed_at is not None

    # Retryable failure when cancel_requested = TRUE -> cancelled takes precedence.
    job_cancel = _create_queued_job(db_session, max_attempts=3)
    with session_factory() as s:
        claimed = WorkerRepository.claim_next_job(
            s, worker_id="retry-cancel-w", lease_ttl_seconds=30.0
        )
        assert claimed is not None

    with session_factory() as api_sess:
        JobService.cancel_job(api_sess, job_cancel.id)

    with session_factory() as s:
        outcome = WorkerRepository.handle_retryable_failure(
            s,
            job_id=job_cancel.id,
            worker_id="retry-cancel-w",
            fencing_token=claimed.fencing_token,
            error_message="transient err",
        )
        assert outcome == "cancelled"

    db_session.expire_all()
    final_cancelled = db_session.get(Job, job_cancel.id)
    assert final_cancelled is not None
    assert final_cancelled.status == JobStatus.CANCELLED.value


def test_non_retryable_failure_direct_and_precedence_over_cancellation(
    session_factory: sessionmaker[Session],
    db_session: Session,
) -> None:
    # 1. Non-retryable with remaining attempts goes straight to failed.
    job1 = _create_queued_job(
        db_session,
        payload={"model": "bad", "steps": 1, "step_duration_ms": 0, "simulate_failure": "non_retryable"},
        max_attempts=5,
    )
    runner = WorkerRunner(session_factory=session_factory, worker_id="nonretry-w")
    assert runner.run_once() is True

    db_session.expire_all()
    j1 = db_session.get(Job, job1.id)
    assert j1 is not None
    assert j1.status == JobStatus.FAILED.value
    assert j1.attempt == 1

    # 2. Non-retryable takes precedence over concurrent cancel_requested = TRUE.
    job2 = _create_queued_job(db_session, max_attempts=3)
    with session_factory() as s:
        c2 = WorkerRepository.claim_next_job(
            s, worker_id="nonretry-w2", lease_ttl_seconds=30.0
        )
        assert c2 is not None

    with session_factory() as api_sess:
        JobService.cancel_job(api_sess, job2.id)

    with session_factory() as s:
        assert (
            WorkerRepository.fail_job_non_retryable(
                s,
                job_id=job2.id,
                worker_id="nonretry-w2",
                fencing_token=c2.fencing_token,
                error_message="fatal error",
            )
            is True
        )

    db_session.expire_all()
    j2 = db_session.get(Job, job2.id)
    assert j2 is not None
    assert j2.status == JobStatus.FAILED.value
    assert j2.cancel_requested is True


def test_recovery_handles_cancel_retry_exhausted_and_active_leases_idempotently(
    session_factory: sessionmaker[Session],
    db_session: Session,
) -> None:
    job_cancel = _create_queued_job(db_session, max_attempts=3)
    job_requeue = _create_queued_job(db_session, max_attempts=3)
    job_exhaust = _create_queued_job(db_session, max_attempts=1)
    job_active = _create_queued_job(db_session, max_attempts=3)

    for j in (job_cancel, job_requeue, job_exhaust, job_active):
        with session_factory() as s:
            claimed = WorkerRepository.claim_next_job(
                s, worker_id=f"w-{j.id}", lease_ttl_seconds=30.0
            )
            assert claimed is not None

    db_session.execute(
        text(
            """
            UPDATE jobs
            SET lease_expires_at = clock_timestamp() - interval '5 seconds',
                cancel_requested = TRUE
            WHERE id = :id
            """
        ),
        {"id": job_cancel.id},
    )
    db_session.execute(
        text(
            "UPDATE jobs SET lease_expires_at = clock_timestamp() - interval '5 seconds' WHERE id IN (:id1, :id2)"
        ),
        {"id1": job_requeue.id, "id2": job_exhaust.id},
    )
    db_session.commit()

    with session_factory() as sr:
        stats1 = WorkerRepository.recover_expired_jobs(sr)
        assert stats1.cancelled == 1
        assert stats1.requeued == 1
        assert stats1.failed == 1

        # Running recovery again immediately is safe and recovers nothing.
        stats2 = WorkerRepository.recover_expired_jobs(sr)
        assert stats2.total == 0

    db_session.expire_all()
    assert db_session.get(Job, job_cancel.id).status == JobStatus.CANCELLED.value
    assert db_session.get(Job, job_requeue.id).status == JobStatus.QUEUED.value
    assert db_session.get(Job, job_exhaust.id).status == JobStatus.FAILED.value
    assert db_session.get(Job, job_active.id).status == JobStatus.RUNNING.value


def test_recovery_versus_completion_race_serializes_cleanly(
    session_factory: sessionmaker[Session],
    db_session: Session,
) -> None:
    job = _create_queued_job(db_session, max_attempts=3)

    with session_factory() as s:
        claimed = WorkerRepository.claim_next_job(
            s, worker_id="race-worker", lease_ttl_seconds=30.0
        )
        assert claimed is not None

    # Expire the lease, then simulate recovery holding the row lock while completion tries to commit.
    db_session.execute(
        text(
            "UPDATE jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE id = :id"
        ),
        {"id": job.id},
    )
    db_session.commit()

    recovery_locked = threading.Event()
    allow_recovery_commit = threading.Event()

    def _recovery_holding_lock() -> int:
        with session_factory() as sr:
            row = sr.execute(
                text(
                    """
                    SELECT id
                    FROM jobs
                    WHERE id = :id
                      AND status = 'running'
                      AND lease_expires_at <= clock_timestamp()
                    FOR UPDATE SKIP LOCKED
                    """
                ),
                {"id": job.id},
            ).one_or_none()
            assert row is not None
            recovery_locked.set()
            assert allow_recovery_commit.wait(timeout=5.0)
            res = sr.execute(
                text(
                    """
                    UPDATE jobs
                    SET status = 'queued',
                        worker_id = NULL,
                        lease_expires_at = NULL,
                        fencing_token = fencing_token + 1,
                        updated_at = clock_timestamp()
                    WHERE id = :id
                      AND status = 'running'
                      AND fencing_token = :token
                    """
                ),
                {"id": job.id, "token": claimed.fencing_token},
            )
            sr.commit()
            return res.rowcount

    def _late_completion() -> bool:
        assert recovery_locked.wait(timeout=5.0)
        with session_factory() as sc:
            return WorkerRepository.complete_job_atomically(
                sc,
                job_id=job.id,
                worker_id="race-worker",
                fencing_token=claimed.fencing_token,
                effect_payload={"raced": True},
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        rec_fut = pool.submit(_recovery_holding_lock)
        comp_fut = pool.submit(_late_completion)
        # Allow completion thread to block on FOR UPDATE behind recovery, then commit recovery.
        threading.Event().wait(0.05)
        allow_recovery_commit.set()
        requeued = rec_fut.result(timeout=5.0)
        completed = comp_fut.result(timeout=5.0)

    assert requeued == 1
    assert completed is False

    db_session.expire_all()
    recovered = db_session.get(Job, job.id)
    assert recovered is not None
    assert recovered.status == JobStatus.QUEUED.value
    assert recovered.fencing_token == claimed.fencing_token + 1

    effect_count = db_session.scalar(
        select(func.count(ExecutionEffect.id)).where(ExecutionEffect.job_id == job.id)
    )
    assert effect_count == 0
