# Design Note: Training-Job Queue Service

> **Status:** Implemented & Verified (PostgreSQL 16, FastAPI, 4 Uvicorn processes, independent workers, Alembic migrations; 77 tests passing). Companion specification: [ARCHITECTURE.md](ARCHITECTURE.md).

---

### 1. Architecture Rationale: PostgreSQL as Single Source of Truth
PostgreSQL coordinates job states, leases, fencing tokens, cancellation flags, and execution effects in one engine. ACID transactions, row-level locking (`FOR UPDATE SKIP LOCKED`), and mature indexes eliminate external brokers (Redis/RabbitMQ) or consensus tools (etcd). SQLite was evaluated and rejected because database-level lock contention causes `SQLITE_BUSY` errors under concurrent multi-process writes and cannot scale across containerized worker nodes.

### 2. Atomic Job Claiming (`SELECT FOR UPDATE SKIP LOCKED`)
Workers poll PostgreSQL independently (`0.2s` interval) and claim jobs using `SELECT ... WHERE status = 'queued' ORDER BY created_at ASC, id ASC LIMIT 1 FOR UPDATE SKIP LOCKED;`. `SKIP LOCKED` bypasses rows locked by concurrent workers, eliminating contention and double claims. In the same transaction, the worker increments `attempt` and `fencing_token`, sets `worker_id`, records `started_at`, sets `lease_expires_at = clock_timestamp() + lease_ttl`, and commits `status = 'running'`.

### 3. Worker Leases & Heartbeat Renewal
Jobs are protected by time-to-live leases (`30s` TTL). A dedicated background heartbeat thread renews `lease_expires_at` periodically (`10s` interval) via a separate database session. All lease assignment, renewal, and expiry checks strictly use PostgreSQL server **`clock_timestamp()`**—never client clocks or transaction-constant `now()`, which freezes at transaction start.

### 4. Dead-Worker Detection & Recovery
A periodic recovery loop sweeps for expired jobs (`status = 'running' AND lease_expires_at <= clock_timestamp()`). Recovery enforces strict precedence: (1) if `cancel_requested = TRUE`, transition to `cancelled`; (2) else if `attempt < max_attempts`, requeue to `queued`; (3) else (`attempt >= max_attempts`), mark as `failed`.

### 5. Fencing Tokens & Stale-Worker Rejection
To prevent split-brain writes from zombie workers (paused by GC or OS freezes that wake after lease expiry and recovery), jobs carry a monotonically increasing `fencing_token`. Every worker update conditionally checks `fencing_token`, `worker_id`, `status = 'running'`, and `lease_expires_at > clock_timestamp()`. If recovery reclaimed the job, the token changed and lease expired; the stale worker's update matches `rowcount == 0`, triggering an immediate rollback without committing late effects.

### 6. Cancellation Semantics
- **Queued:** Atomically updated `queued -> cancelled` directly by the API.
- **Running (Cooperative):** API sets `cancel_requested = TRUE`. Workers check this flag at safe checkpoints, aborting execution and committing `running -> cancelled` under fencing/lease guards with zero execution effects.
- **Terminal:** Completed, failed, or cancelled jobs return `HTTP 200 OK` unchanged (idempotent no-op).

### 7. Delivery Guarantees, Side Effects & Limitations
- **At-Least-Once Execution Attempts:** Duplicate execution attempts can occur following worker crashes or retryable failures.
- **Bounded Retries:** Successful execution is **not** guaranteed; retries are strictly bounded by `max_attempts`.
- **At-Most-One Committed Database Effect:** Measurable effects are persisted in `execution_effects` with `UNIQUE(job_id)`.
- **Atomic State and Effect Commit:** The `execution_effects` record and `running -> succeeded` transition commit inside the **same atomic PostgreSQL ACID transaction** (`rowcount == 1` or rollback).
- **Non-DB External Side Effects:** Exactly-once guarantees apply exclusively to database effects within PostgreSQL. External actions (S3 uploads, webhooks, GPU compute) sit outside the ACID boundary, may execute partially before crashes, and require external idempotency.

### 8. Retry Policy & Terminal States
Retryable failures requeue while `attempt < max_attempts` unless `cancel_requested = TRUE` (which takes precedence and cancels). Non-retryable failures transition directly to `failed`, taking precedence over cancellation while holding a valid lease. Terminal states (`succeeded`, `failed`, `cancelled`) are final and immutable.
