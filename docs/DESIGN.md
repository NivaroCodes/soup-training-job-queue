# Design Note: Training-Job Queue Service

> **Status:** Approved Design Note (Architecture Review #3 Finalized; **Pending Final Approval, Implementation, and Verification**)  
> **Companion Specification:** Detailed SQL contracts, serialization mechanics, and implementation risks live in [ARCHITECTURE.md](ARCHITECTURE.md). No application code, migrations, or tests have been implemented yet.

---

## 1. Architecture & Stack

- **API (`FastAPI`):** Stateless HTTP service exposing idempotent job submission (`POST /jobs`), status lookup (`GET /jobs/{job_id}`), offset/limit paginated listing (`GET /jobs`), and cancellation (`POST /jobs/{job_id}/cancel`). Idempotency stores a unique `idempotency_key` with a deterministic `request_hash`: matching payloads return the existing job; mismatched payloads return `HTTP 409 Conflict`.
- **Database (`PostgreSQL` via synchronous `SQLAlchemy 2.x` + `Psycopg 3` + `Alembic`):** Single source of truth coordinating job state, leases, fencing tokens, cancellation flags, and committed measurable side effects (`execution_effects` with `UNIQUE(job_id)`).
- **Workers (`>= 4` independent processes):** Separate processes/containers using fixed-interval PostgreSQL polling to claim jobs, emit periodic heartbeats, check cooperative cancellation points, and commit side effects atomically.

## 2. Job Lifecycle & Failure Policy

```text
queued -> running -> succeeded | failed | cancelled
```

Claiming a job (`queued -> running`) increments `attempt` by `1` and `fencing_token` by `1`:
- **Retryable execution failure:** Consumes the current attempt and follows strict precedence: (1) if `cancel_requested = TRUE`, transition `running -> cancelled`; (2) else if `attempt < max_attempts`, transition `running -> queued`; (3) else (`attempt >= max_attempts`), transition `running -> failed`. Both `running -> queued` and retryable-exhausted `running -> failed` enforce `cancel_requested = FALSE` alongside `id`, `status = 'running'`, `worker_id`, `fencing_token`, and `lease_expires_at > clock_timestamp()` (`rowcount == 1`), rolling back and re-reading on `0` rows (switching to cooperative cancellation if `cancel_requested = TRUE`, or aborting if lease/ownership was lost or the job is already terminal).
- **Non-retryable execution failure:** Consumes the current attempt and **takes precedence over a concurrent `cancel_requested = TRUE`**, transitioning directly `running -> failed` regardless of `max_attempts` while enforcing `id`, `status = 'running'`, `worker_id`, `fencing_token`, and `lease_expires_at > clock_timestamp()` (`rowcount == 1` or rollback; never overwrites a terminal state).
- **Worker crash / lease expiration:** Consumed at claim; recovery checks `cancel_requested = TRUE` **first** (`running -> cancelled`), else requeues (`running -> queued`) while `attempt < max_attempts`, or marks `failed` when `attempt >= max_attempts`.
- **Terminal states (`succeeded`, `failed`, `cancelled`):** Immutable; no further transitions or side-effect commits are permitted.

## 3. Claiming, Leases & Fencing

- **Atomic Claiming:** Workers claim one queued job per transaction via `SELECT ... WHERE status = 'queued' ORDER BY created_at ASC, id ASC LIMIT 1 FOR UPDATE SKIP LOCKED`, preventing concurrent double-claims.
- **Authoritative Time & Leases:** All lease assignment, heartbeat renewal, expiration detection (`lease_expires_at <= clock_timestamp()`), and recovery use PostgreSQL **`clock_timestamp()`**—never `now()`, which freezes at transaction start and would misreport validity in long-running or lock-delayed transactions.
- **Fencing + Lease Validity:** Because `fencing_token` alone leaves a pre-recovery window after lease expiry, every worker state mutation (heartbeats, completion, failure, requeue, and cancellation completion) requires `status = 'running'`, `worker_id`, matching `fencing_token`, **and** `lease_expires_at > clock_timestamp()`, serialized against recovery by PostgreSQL row locks.

## 4. Cancellation Semantics

- **Queued:** API executes an atomic conditional `UPDATE` (`queued -> cancelled`).
- **Running:** API runs a conditional `UPDATE` setting `cancel_requested = TRUE` and `cancel_requested_at = COALESCE(cancel_requested_at, clock_timestamp())` where `status = 'running'`. If `0` rows match, the API re-reads the row (`404` if missing, retry `queued` cancel if reclaimed, or return terminal state unchanged) and never overwrites a terminal state. Workers abort cooperatively at safe checkpoints and commit `running -> cancelled` under full fencing + lease-validity + `cancel_requested = TRUE` guards (`rowcount == 1` or rollback).
- **Terminal:** `POST /jobs/{job_id}/cancel` returns **`HTTP 200 OK`** with the current job representation unchanged (idempotent).

## 5. Execution Guarantees & Key Risks

- **Bounded Retries:** Duplicate execution attempts are possible after crashes or retryable failures, retries are bounded by `max_attempts`, and successful completion is not guaranteed.
- **Scoped Database Effects:** Workers commit `execution_effects` and the fenced, lease-validated `running -> succeeded` update (`cancel_requested = FALSE`, `rowcount == 1` or rollback) in one ACID transaction. Exactly-once committed database effects are scoped strictly to this PostgreSQL transaction, and `UNIQUE(job_id)` enforces at-most-one committed effect per job.
- **Top Unresolved Risks (Untested):** Synchronous worker heartbeat decoupling during blocking compute, `lease_ttl`/`heartbeat_interval`/`max_attempts` tuning, polling load across `>= 4` workers, DB connection pool sizing, and external non-DB side effects outside the transaction boundary. See [ARCHITECTURE.md](ARCHITECTURE.md) for full SQL and risk analysis.
