"""PostgreSQL integration tests for Job and ExecutionEffect models, constraints, and Alembic."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import Settings, normalize_postgres_url
from app.db import ExecutionEffect, Job, JobStatus


def _make_job(
    *,
    idempotency_key: str | None = None,
    request_hash: str = "a" * 64,
    payload: dict | None = None,
    status: str = JobStatus.QUEUED.value,
    attempt: int = 0,
    max_attempts: int = 3,
    fencing_token: int = 0,
) -> Job:
    return Job(
        idempotency_key=idempotency_key or f"key-{uuid.uuid4()}",
        request_hash=request_hash,
        payload=payload if payload is not None else {"model": "resnet", "epochs": 5},
        status=status,
        attempt=attempt,
        max_attempts=max_attempts,
        fencing_token=fencing_token,
    )


def test_config_rejects_sqlite_and_missing_database_url() -> None:
    with pytest.raises(ValueError):
        normalize_postgres_url("sqlite:///./local.db")

    assert (
        normalize_postgres_url("postgresql://user:pass@localhost:5432/soup")
        == "postgresql+psycopg://user:pass@localhost:5432/soup"
    )

    unconfigured = Settings(DATABASE_URL=None)
    with pytest.raises(RuntimeError):
        unconfigured.require_database_url()


def test_job_and_execution_effect_defaults_and_persistence(db_session: Session) -> None:
    job = _make_job(idempotency_key="job-defaults-1")
    db_session.add(job)
    db_session.commit()
    db_session.refresh(job)

    assert isinstance(job.id, uuid.UUID)
    assert job.status == JobStatus.QUEUED.value
    assert job.attempt == 0
    assert job.max_attempts == 3
    assert job.fencing_token == 0
    assert job.cancel_requested is False
    assert job.worker_id is None
    assert job.lease_expires_at is None
    assert job.last_heartbeat_at is None
    assert job.cancel_requested_at is None
    assert job.last_error is None
    assert job.started_at is None
    assert job.completed_at is None
    assert job.cancelled_at is None
    assert job.finished_at is None
    assert job.created_at is not None
    assert job.updated_at is not None

    now = datetime.now(UTC)
    job.status = JobStatus.RUNNING.value
    job.attempt = 1
    job.fencing_token = 1
    job.worker_id = "worker-1"
    job.started_at = now
    job.lease_expires_at = now + timedelta(seconds=30)
    db_session.flush()

    effect = ExecutionEffect(
        job_id=job.id,
        worker_id="worker-1",
        fencing_token=1,
        effect_payload={"loss": 0.125, "steps": 100},
    )
    job.status = JobStatus.SUCCEEDED.value
    job.completed_at = now + timedelta(seconds=5)
    job.worker_id = None
    job.lease_expires_at = None
    db_session.add(effect)
    db_session.commit()
    db_session.refresh(job)
    db_session.refresh(effect)

    assert isinstance(effect.id, uuid.UUID)
    assert effect.job_id == job.id
    assert effect.effect_payload == {"loss": 0.125, "steps": 100}
    assert effect.created_at is not None
    assert job.finished_at == job.completed_at
    assert job.execution_effect is not None
    assert job.execution_effect.id == effect.id
    assert effect.job.id == job.id


def test_finished_at_hybrid_expression_supports_cancelled_and_completed(
    db_session: Session,
) -> None:
    t1 = datetime.now(UTC)
    completed_job = _make_job(status=JobStatus.SUCCEEDED.value)
    completed_job.completed_at = t1

    cancelled_job = _make_job(status=JobStatus.CANCELLED.value)
    cancelled_job.cancelled_at = t1 + timedelta(seconds=10)

    db_session.add_all([completed_job, cancelled_job])
    db_session.commit()

    rows = db_session.scalars(
        select(Job).where(Job.finished_at >= t1).order_by(Job.finished_at.asc())
    ).all()
    assert [row.id for row in rows] == [completed_job.id, cancelled_job.id]


def test_unique_idempotency_key_enforced(db_session: Session) -> None:
    db_session.add(_make_job(idempotency_key="dup-key"))
    db_session.commit()

    db_session.add(_make_job(idempotency_key="dup-key"))
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_unique_execution_effect_job_id_enforced(db_session: Session) -> None:
    job = _make_job(status=JobStatus.RUNNING.value, attempt=1, fencing_token=1)
    db_session.add(job)
    db_session.commit()

    db_session.add(
        ExecutionEffect(
            job_id=job.id,
            worker_id="worker-1",
            fencing_token=1,
            effect_payload={"epoch": 1},
        )
    )
    db_session.commit()

    db_session.add(
        ExecutionEffect(
            job_id=job.id,
            worker_id="worker-2",
            fencing_token=2,
            effect_payload={"epoch": 2},
        )
    )
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


@pytest.mark.parametrize("status", [s.value for s in JobStatus])
def test_all_valid_job_statuses_accepted(db_session: Session, status: str) -> None:
    job = _make_job(status=status)
    db_session.add(job)
    db_session.commit()
    assert job.status == status


@pytest.mark.parametrize("invalid_status", ["pending", "retrying", "done", ""])
def test_invalid_job_status_rejected(db_session: Session, invalid_status: str) -> None:
    db_session.add(_make_job(status=invalid_status))
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


@pytest.mark.parametrize(
    ("attempt", "max_attempts"),
    [
        (-1, 3),
        (0, 0),
        (1, 0),
        (4, 3),
    ],
)
def test_invalid_attempt_limits_rejected(
    db_session: Session, attempt: int, max_attempts: int
) -> None:
    db_session.add(_make_job(attempt=attempt, max_attempts=max_attempts))
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_negative_fencing_token_and_empty_keys_rejected(db_session: Session) -> None:
    db_session.add(_make_job(fencing_token=-1))
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()

    db_session.add(_make_job(idempotency_key="   "))
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()

    db_session.add(_make_job(request_hash="   "))
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_execution_effect_check_constraints_enforced(db_session: Session) -> None:
    job = _make_job(status=JobStatus.RUNNING.value, attempt=1, fencing_token=1)
    db_session.add(job)
    db_session.commit()

    db_session.add(
        ExecutionEffect(
            job_id=job.id,
            worker_id="worker-1",
            fencing_token=0,
            effect_payload={},
        )
    )
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()

    db_session.add(
        ExecutionEffect(
            job_id=job.id,
            worker_id="   ",
            fencing_token=1,
            effect_payload={},
        )
    )
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_foreign_key_and_restrict_delete_behavior(db_session: Session) -> None:
    # Inserting an ExecutionEffect for a nonexistent Job must fail FK check.
    orphan = ExecutionEffect(
        job_id=uuid.uuid4(),
        worker_id="worker-1",
        fencing_token=1,
        effect_payload={},
    )
    db_session.add(orphan)
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()

    # Deleting a Job that has a committed ExecutionEffect must fail ON DELETE RESTRICT.
    job = _make_job(status=JobStatus.SUCCEEDED.value, attempt=1, fencing_token=1)
    db_session.add(job)
    db_session.flush()
    effect = ExecutionEffect(
        job_id=job.id,
        worker_id="worker-1",
        fencing_token=1,
        effect_payload={"ok": True},
    )
    db_session.add(effect)
    db_session.commit()

    db_session.delete(job)
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()

    # Deleting ExecutionEffect first allows deleting the parent Job.
    db_session.delete(effect)
    db_session.flush()
    db_session.delete(job)
    db_session.commit()


def test_postgresql_partial_and_listing_indexes_exist(db_session: Session) -> None:
    rows = db_session.execute(
        text(
            """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'jobs'
            """
        )
    ).mappings().all()
    by_name = {row["indexname"]: row["indexdef"] for row in rows}

    assert "ix_jobs_queued_poll" in by_name
    assert "WHERE ((status)::text = 'queued'::text)" in by_name["ix_jobs_queued_poll"]

    assert "ix_jobs_running_lease_recovery" in by_name
    assert (
        "WHERE ((status)::text = 'running'::text)"
        in by_name["ix_jobs_running_lease_recovery"]
    )

    assert "ix_jobs_created_at_id" in by_name
    assert "ix_jobs_status_created_at_id" in by_name


def test_alembic_migration_is_reversible(
    db_session: Session, alembic_config: Config
) -> None:
    # Close current session transaction before running DDL via Alembic.
    db_session.commit()

    command.downgrade(alembic_config, "base")
    inspector = inspect(db_session.get_bind())
    assert "jobs" not in inspector.get_table_names()
    assert "execution_effects" not in inspector.get_table_names()

    command.upgrade(alembic_config, "head")
    inspector = inspect(db_session.get_bind())
    assert "jobs" in inspector.get_table_names()
    assert "execution_effects" in inspector.get_table_names()
