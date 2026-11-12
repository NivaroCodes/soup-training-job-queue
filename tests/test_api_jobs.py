"""PostgreSQL integration tests for the training job queue REST API."""

from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, func, select
from sqlalchemy.orm import Session, sessionmaker

from app.db import ExecutionEffect, Job, JobStatus


def test_create_job_requires_idempotency_key(client: TestClient) -> None:
    response = client.post("/jobs", json={"payload": {"epochs": 3}})
    assert response.status_code == 422

    blank_header = client.post(
        "/jobs",
        headers={"Idempotency-Key": "   "},
        json={"payload": {"epochs": 3}},
    )
    assert blank_header.status_code == 422

    mismatch_both = client.post(
        "/jobs",
        headers={"Idempotency-Key": "key-a"},
        json={"idempotency_key": "key-b", "payload": {"epochs": 3}},
    )
    assert mismatch_both.status_code == 422


def test_create_job_with_header_and_body_idempotency_key(client: TestClient) -> None:
    resp_header = client.post(
        "/jobs",
        headers={"Idempotency-Key": "hdr-key-1"},
        json={"payload": {"model": "vit", "epochs": 2}, "max_attempts": 4},
    )
    assert resp_header.status_code == 201
    body_h = resp_header.json()
    assert body_h["idempotency_key"] == "hdr-key-1"
    assert body_h["status"] == "queued"
    assert body_h["attempt"] == 0
    assert body_h["max_attempts"] == 4
    assert body_h["fencing_token"] == 0
    assert body_h["cancel_requested"] is False
    assert body_h["payload"] == {"model": "vit", "epochs": 2}
    assert len(body_h["request_hash"]) == 64

    resp_body = client.post(
        "/jobs",
        json={
            "idempotency_key": "body-key-1",
            "payload": {"model": "mlp", "lr": 0.001},
        },
    )
    assert resp_body.status_code == 201
    assert resp_body.json()["idempotency_key"] == "body-key-1"
    assert resp_body.json()["max_attempts"] == 3


def test_idempotent_repeated_requests_return_existing_job(
    client: TestClient, db_session: Session
) -> None:
    first = client.post(
        "/jobs",
        headers={"Idempotency-Key": "idem-replay-1"},
        json={"payload": {"b": 2, "a": {"y": 20, "x": 10}}, "max_attempts": 3},
    )
    assert first.status_code == 201

    # Different JSON key order must produce the exact same deterministic request_hash.
    second = client.post(
        "/jobs",
        headers={"Idempotency-Key": "idem-replay-1"},
        json={"payload": {"a": {"x": 10, "y": 20}, "b": 2}, "max_attempts": 3},
    )
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    assert second.json()["request_hash"] == first.json()["request_hash"]

    total = db_session.scalar(select(func.count(Job.id)))
    assert total == 1


def test_idempotency_conflict_returns_409(client: TestClient) -> None:
    created = client.post(
        "/jobs",
        headers={"Idempotency-Key": "conflict-key-1"},
        json={"payload": {"epochs": 5}, "max_attempts": 3},
    )
    assert created.status_code == 201

    different_payload = client.post(
        "/jobs",
        headers={"Idempotency-Key": "conflict-key-1"},
        json={"payload": {"epochs": 10}, "max_attempts": 3},
    )
    assert different_payload.status_code == 409

    different_attempts = client.post(
        "/jobs",
        headers={"Idempotency-Key": "conflict-key-1"},
        json={"payload": {"epochs": 5}, "max_attempts": 5},
    )
    assert different_attempts.status_code == 409


def test_get_job_and_unknown_job_404(
    client: TestClient, db_session: Session
) -> None:
    created = client.post(
        "/jobs",
        headers={"Idempotency-Key": "get-key-1"},
        json={"payload": {"task": "train"}},
    )
    assert created.status_code == 201
    job_id = created.json()["id"]

    fetched = client.get(f"/jobs/{job_id}")
    assert fetched.status_code == 200
    assert fetched.json()["id"] == job_id
    assert fetched.json()["execution_effect"] is None

    # Simulate a committed ExecutionEffect and verify GET includes it.
    job = db_session.get(Job, uuid.UUID(job_id))
    assert job is not None
    job.status = JobStatus.SUCCEEDED.value
    job.attempt = 1
    job.fencing_token = 1
    job.completed_at = datetime.now(UTC)
    db_session.add(
        ExecutionEffect(
            job_id=job.id,
            worker_id="worker-1",
            fencing_token=1,
            effect_payload={"accuracy": 0.98},
        )
    )
    db_session.commit()

    fetched_after = client.get(f"/jobs/{job_id}")
    assert fetched_after.status_code == 200
    assert fetched_after.json()["status"] == "succeeded"
    assert fetched_after.json()["execution_effect"]["worker_id"] == "worker-1"
    assert fetched_after.json()["execution_effect"]["effect_payload"] == {
        "accuracy": 0.98
    }

    missing = client.get(f"/jobs/{uuid.uuid4()}")
    assert missing.status_code == 404


def test_list_jobs_pagination_deterministic_ordering_and_status_filter(
    client: TestClient, db_session: Session
) -> None:
    created_ids: list[str] = []
    for idx in range(5):
        resp = client.post(
            "/jobs",
            headers={"Idempotency-Key": f"page-key-{idx}"},
            json={"payload": {"idx": idx}},
        )
        assert resp.status_code == 201
        created_ids.append(resp.json()["id"])

    # Mark two jobs with non-queued statuses to test filtering.
    job_1 = db_session.get(Job, uuid.UUID(created_ids[1]))
    job_3 = db_session.get(Job, uuid.UUID(created_ids[3]))
    assert job_1 is not None and job_3 is not None
    job_1.status = JobStatus.RUNNING.value
    job_1.attempt = 1
    job_1.fencing_token = 1
    job_1.worker_id = "worker-1"
    job_1.lease_expires_at = datetime.now(UTC) + timedelta(seconds=30)
    job_3.status = JobStatus.FAILED.value
    job_3.attempt = 1
    job_3.fencing_token = 1
    job_3.completed_at = datetime.now(UTC)
    db_session.commit()

    page_1 = client.get("/jobs", params={"limit": 2, "offset": 0})
    assert page_1.status_code == 200
    data_1 = page_1.json()
    assert data_1["total"] == 5
    assert data_1["limit"] == 2
    assert data_1["offset"] == 0
    assert [item["id"] for item in data_1["items"]] == created_ids[0:2]

    page_2 = client.get("/jobs", params={"limit": 2, "offset": 2})
    assert page_2.status_code == 200
    assert [item["id"] for item in page_2.json()["items"]] == created_ids[2:4]

    page_3 = client.get("/jobs", params={"limit": 2, "offset": 4})
    assert page_3.status_code == 200
    assert [item["id"] for item in page_3.json()["items"]] == created_ids[4:5]

    queued_only = client.get("/jobs", params={"status": "queued", "limit": 10, "offset": 0})
    assert queued_only.status_code == 200
    assert queued_only.json()["total"] == 3
    assert [item["id"] for item in queued_only.json()["items"]] == [
        created_ids[0],
        created_ids[2],
        created_ids[4],
    ]


def test_cancel_queued_job_transitions_directly_to_cancelled(
    client: TestClient,
) -> None:
    created = client.post(
        "/jobs",
        headers={"Idempotency-Key": "cancel-queued-1"},
        json={"payload": {"step": 1}},
    )
    assert created.status_code == 201
    job_id = created.json()["id"]

    cancelled = client.post(f"/jobs/{job_id}/cancel")
    assert cancelled.status_code == 200
    body = cancelled.json()
    assert body["status"] == "cancelled"
    assert body["cancelled_at"] is not None
    assert body["finished_at"] == body["cancelled_at"]
    assert body["cancel_requested"] is False

    unknown = client.post(f"/jobs/{uuid.uuid4()}/cancel")
    assert unknown.status_code == 404


def test_cancel_running_job_sets_cooperative_flag_idempotently(
    client: TestClient, db_session: Session
) -> None:
    created = client.post(
        "/jobs",
        headers={"Idempotency-Key": "cancel-running-1"},
        json={"payload": {"step": 2}},
    )
    assert created.status_code == 201
    job_id = created.json()["id"]

    job = db_session.get(Job, uuid.UUID(job_id))
    assert job is not None
    job.status = JobStatus.RUNNING.value
    job.attempt = 1
    job.fencing_token = 1
    job.worker_id = "worker-1"
    job.lease_expires_at = datetime.now(UTC) + timedelta(seconds=30)
    db_session.commit()

    first_cancel = client.post(f"/jobs/{job_id}/cancel")
    assert first_cancel.status_code == 200
    first_body = first_cancel.json()
    assert first_body["status"] == "running"
    assert first_body["cancel_requested"] is True
    assert first_body["cancel_requested_at"] is not None
    assert first_body["cancelled_at"] is None

    second_cancel = client.post(f"/jobs/{job_id}/cancel")
    assert second_cancel.status_code == 200
    second_body = second_cancel.json()
    assert second_body["status"] == "running"
    assert second_body["cancel_requested"] is True
    assert second_body["cancel_requested_at"] == first_body["cancel_requested_at"]


@pytest.mark.parametrize(
    "terminal_status",
    [JobStatus.SUCCEEDED.value, JobStatus.FAILED.value, JobStatus.CANCELLED.value],
)
def test_cancel_terminal_job_returns_200_without_mutation(
    client: TestClient, db_session: Session, terminal_status: str
) -> None:
    created = client.post(
        "/jobs",
        headers={"Idempotency-Key": f"cancel-term-{terminal_status}"},
        json={"payload": {"terminal": terminal_status}},
    )
    assert created.status_code == 201
    job_id = created.json()["id"]

    job = db_session.get(Job, uuid.UUID(job_id))
    assert job is not None
    job.status = terminal_status
    job.attempt = 1
    job.fencing_token = 1
    ts = datetime.now(UTC)
    if terminal_status == JobStatus.CANCELLED.value:
        job.cancelled_at = ts
    else:
        job.completed_at = ts
    db_session.commit()

    resp = client.post(f"/jobs/{job_id}/cancel")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == terminal_status
    assert body["cancel_requested"] is False
    if terminal_status == JobStatus.CANCELLED.value:
        assert body["cancelled_at"] is not None
        assert body["completed_at"] is None
    else:
        assert body["completed_at"] is not None
        assert body["cancelled_at"] is None


def test_concurrent_idempotent_requests_same_payload(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    key = f"concurrent-same-{uuid.uuid4()}"
    payload = {"model": "llama", "epochs": 10}

    def _submit(_: int) -> tuple[int, str]:
        resp = client.post(
            "/jobs",
            headers={"Idempotency-Key": key},
            json={"payload": payload},
        )
        return resp.status_code, resp.json()["id"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(_submit, range(8)))

    status_codes = [code for code, _ in results]
    returned_ids = {job_id for _, job_id in results}

    assert len(returned_ids) == 1
    assert status_codes.count(201) == 1
    assert status_codes.count(200) == 7

    with session_factory() as verify_session:
        count = verify_session.scalar(
            select(func.count(Job.id)).where(Job.idempotency_key == key)
        )
        assert count == 1


def test_concurrent_idempotent_requests_conflicting_payloads(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    key = f"concurrent-conflict-{uuid.uuid4()}"

    def _submit(epochs: int) -> int:
        resp = client.post(
            "/jobs",
            headers={"Idempotency-Key": key},
            json={"payload": {"epochs": epochs}},
        )
        return resp.status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        codes = list(pool.map(_submit, [1, 2]))

    assert sorted(codes) == [201, 409]

    with session_factory() as verify_session:
        count = verify_session.scalar(
            select(func.count(Job.id)).where(Job.idempotency_key == key)
        )
        assert count == 1


def test_create_job_unique_triggers_no_post_commit_select_or_lazy_load(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    engine = session_factory.kw["bind"]
    executed: list[str] = []

    def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        executed.append(statement.strip().upper())

    event.listen(engine, "before_cursor_execute", _before_cursor_execute)
    try:
        resp = client.post(
            "/jobs",
            headers={"Idempotency-Key": f"no-lazy-{uuid.uuid4()}"},
            json={"payload": {"model": "opt-a", "epochs": 1}},
        )
    finally:
        event.remove(engine, "before_cursor_execute", _before_cursor_execute)

    assert resp.status_code == 201
    body = resp.json()
    assert body["execution_effect"] is None
    assert body["created_at"] is not None
    assert body["updated_at"] is not None

    select_stmts = [s for s in executed if s.startswith("SELECT")]
    insert_stmts = [s for s in executed if s.startswith("INSERT")]
    assert len(select_stmts) == 1
    assert len(insert_stmts) == 1
    assert not any("EXECUTION_EFFECTS" in s for s in executed)
