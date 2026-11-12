# Detailed Architecture & Concurrency Specification

> **Status:** Approved Technical Specification (Architecture Review #3 Applied; **Pending Final Approval, Implementation, and Verification**)  
> **Scope:** Detailed SQL contracts, concurrency serialization, failure policy, and implementation-level risks complementing the one-page summary in [DESIGN.md](DESIGN.md). No application code, migrations, or tests have been implemented or executed yet.

---

## 1. Core Tables & API Contracts

### Database Entities (PostgreSQL via Synchronous `SQLAlchemy 2.x` + `Psycopg 3` + `Alembic`)
- **`jobs`:** Authoritative job state (`id`, `status`, `idempotency_key` unique, `request_hash`, `payload`, `attempt`, `max_attempts`, `fencing_token`, `worker_id`, `lease_expires_at`, `last_heartbeat_at`, `cancel_requested`, `cancel_requested_at`, `last_error`, `created_at`, `started_at`, `completed_at`, `cancelled_at`).
- **`execution_effects`:** Dedicated table storing measurable job outputs/metrics with **`UNIQUE(job_id)`**, `worker_id`, and `fencing_token`.

### API Endpoints (`FastAPI`)
- **`POST /jobs` (Idempotent Creation):** Computes a deterministic `request_hash` of the payload. Same `idempotency_key` + same `request_hash` returns the existing job representation; same `idempotency_key` + different `request_hash` returns **`HTTP 409 Conflict`**.
- **`GET /jobs/{job_id}`:** Returns current job state (`404 Not Found` if missing).
- **`GET /jobs`:** Lists jobs using **offset/limit pagination** with deterministic ordering.
- **`POST /jobs/{job_id}/cancel`:** Requests cancellation (see Section 6). Returns **`HTTP 200 OK`** with the current job representation unchanged when the job is already in a terminal state (`succeeded`, `failed`, `cancelled`), or `404 Not Found` if the job does not exist.

---

## 2. Atomic Job Claiming

Independent worker processes (`>= 4`) poll PostgreSQL at a fixed interval and claim one queued job per transaction:

```sql
BEGIN;

SELECT id, fencing_token, attempt
FROM jobs
WHERE status = 'queued'
ORDER BY created_at ASC, id ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;

-- If a row is returned, update it in the same transaction:
UPDATE jobs
SET status = 'running',
    worker_id = :worker_id,
    fencing_token = fencing_token + 1,
    attempt = attempt + 1,
    lease_expires_at = clock_timestamp() + :lease_ttl,
    last_heartbeat_at = clock_timestamp(),
    started_at = COALESCE(started_at, clock_timestamp())
WHERE id = :job_id;

COMMIT;
```

- `FOR UPDATE` acquires an exclusive row lock; `SKIP LOCKED` lets concurrent workers bypass rows already locked by other in-flight claim transactions.
- `LIMIT 1` precedes `FOR UPDATE SKIP LOCKED` per PostgreSQL `SELECT` syntax.
- `attempt` is incremented once on every claim (`queued -> running`), consuming one attempt out of `max_attempts`.

---

## 3. Authoritative PostgreSQL Time & Worker Leases

### `clock_timestamp()` vs. `now()`
- All lease assignment, heartbeat renewal, expiration checks, and recovery operations **must use `clock_timestamp()`**, never application wall clocks and never `now()`.
- In PostgreSQL, `now()` (alias for `transaction_timestamp()`) returns the start time of the current transaction and stays frozen until `COMMIT`/`ROLLBACK`. In multi-statement transactions or transactions blocked on row locks, `now()` under-reports elapsed time and could treat an expired lease as valid. `clock_timestamp()` reflects real wall-clock time when each statement executes.

### Heartbeat Renewal
While executing, the owning worker periodically extends its lease:

```sql
UPDATE jobs
SET lease_expires_at = clock_timestamp() + :lease_ttl,
    last_heartbeat_at = clock_timestamp()
WHERE id = :job_id
  AND status = 'running'
  AND worker_id = :worker_id
  AND fencing_token = :held_token
  AND lease_expires_at > clock_timestamp()
RETURNING cancel_requested;
```

- If `0` rows are updated, the worker no longer owns a valid lease and **must abort execution immediately**.
- Dead workers are detected passively when `status = 'running' AND lease_expires_at <= clock_timestamp()`.

---

## 4. Failure Policy & Dead Worker Recovery

### Failure Classification and Attempt Accounting
Because `attempt` is incremented at claim time (`queued -> running`), every claim consumes one attempt. Subsequent transitions depend on the failure class:

| Event | Attempt Consumed? | Precedence & Next State Rule |
| :--- | :--- | :--- |
| **Retryable execution failure** (e.g., transient I/O or temporary dependency failure) | Yes (consumed at claim) | **1.** If `cancel_requested = TRUE`, transition `running -> cancelled` (Section 6.B).<br>**2.** Else if `attempt < max_attempts`, transition `running -> queued` (requeue).<br>**3.** Else (`attempt >= max_attempts`), transition `running -> failed`. |
| **Non-retryable execution failure** (e.g., invalid payload or deterministic domain/runtime error) | Yes (consumed at claim) | **Takes precedence over a concurrent `cancel_requested = TRUE`** while holding a valid lease and `fencing_token`: transitions directly `running -> failed` regardless of `max_attempts` (no retry; never overwrites an already-terminal state). |
| **Worker crash / lease expiration** (`lease_expires_at <= clock_timestamp()`) | Yes (consumed at claim) | Recovery checks `cancel_requested` first: if `TRUE`, transition `running -> cancelled`. Else if `attempt < max_attempts`, transition `running -> queued` (`attempt` unchanged until next claim). Else (`attempt >= max_attempts`), transition `running -> failed`. |

### Retryable Failure Transition SQL & Zero-Row Handling

For any retryable failure, the conditional `UPDATE` **must enforce `AND cancel_requested = FALSE`** so a concurrent cancellation cannot be overwritten by either `running -> queued` or `running -> failed`:

1. **Retryable with remaining budget (`attempt < max_attempts`, `running -> queued`):**
   ```sql
   BEGIN;

   SELECT id, status, worker_id, fencing_token, lease_expires_at, cancel_requested, attempt, max_attempts
   FROM jobs
   WHERE id = :job_id
   FOR UPDATE;

   UPDATE jobs
   SET status = 'queued',
       last_error = :error_message,
       worker_id = NULL,
       lease_expires_at = NULL
   WHERE id = :job_id
     AND status = 'running'
     AND worker_id = :worker_id
     AND fencing_token = :held_token
     AND lease_expires_at > clock_timestamp()
     AND cancel_requested = FALSE
     AND attempt < max_attempts;

   -- Require rowcount == 1; if 0 rows were updated, ROLLBACK and inspect current state.
   COMMIT;
   ```

2. **Retryable with exhausted budget (`attempt >= max_attempts`, `running -> failed`):**
   ```sql
   BEGIN;

   SELECT id, status, worker_id, fencing_token, lease_expires_at, cancel_requested, attempt, max_attempts
   FROM jobs
   WHERE id = :job_id
   FOR UPDATE;

   UPDATE jobs
   SET status = 'failed',
       last_error = :error_message,
       completed_at = clock_timestamp(),
       worker_id = NULL,
       lease_expires_at = NULL
   WHERE id = :job_id
     AND status = 'running'
     AND worker_id = :worker_id
     AND fencing_token = :held_token
     AND lease_expires_at > clock_timestamp()
     AND cancel_requested = FALSE
     AND attempt >= max_attempts;

   -- Require rowcount == 1; if 0 rows were updated, ROLLBACK and inspect current state.
   COMMIT;
   ```

**Zero-Row Handling (`rowcount == 0`):**
If either retryable failure `UPDATE` affects zero rows, the worker rolls back the transaction, re-reads the current job row (`SELECT id, status, worker_id, fencing_token, lease_expires_at, cancel_requested FROM jobs WHERE id = :job_id`), and branches strictly on the observed state:
- **Cancellation was requested (`status = 'running'` and `cancel_requested = TRUE`, with matching `worker_id`, matching `fencing_token`, and `lease_expires_at > clock_timestamp()`):** Follow the cooperative cancellation completion path (Section 6.B) to transition `running -> cancelled`.
- **Ownership or lease was lost (`worker_id != :worker_id`, `fencing_token != :held_token`, `lease_expires_at <= clock_timestamp()`, or job was already reclaimed to `queued`):** Abort immediately without modifying the job.
- **Job is already in a terminal state (`succeeded`, `failed`, `cancelled`) or missing:** Abort without modifying the job—never overwrite a terminal state.

### Non-Retryable Failure Transition SQL (`running -> failed`)
Because a non-retryable failure **takes precedence over a concurrent `cancel_requested = TRUE`** on a valid lease and `fencing_token`, its conditional `UPDATE` omits `cancel_requested = FALSE` while still requiring `status = 'running'`, ownership, fencing, and live lease validity:

```sql
BEGIN;

SELECT id, status, worker_id, fencing_token, lease_expires_at
FROM jobs
WHERE id = :job_id
FOR UPDATE;

UPDATE jobs
SET status = 'failed',
    last_error = :error_message,
    completed_at = clock_timestamp(),
    worker_id = NULL,
    lease_expires_at = NULL
WHERE id = :job_id
  AND status = 'running'
  AND worker_id = :worker_id
  AND fencing_token = :held_token
  AND lease_expires_at > clock_timestamp();

-- Require rowcount == 1; if 0 rows were updated (e.g. lease/ownership lost
-- or job already terminal), ROLLBACK and abort without modifying the job.
COMMIT;
```

### Dead Worker Recovery Sweep
Recovery selects expired jobs using valid PostgreSQL clause ordering:

```sql
SELECT id, attempt, max_attempts, cancel_requested, fencing_token
FROM jobs
WHERE status = 'running'
  AND lease_expires_at <= clock_timestamp()
ORDER BY lease_expires_at ASC, id ASC
LIMIT :batch_size
FOR UPDATE SKIP LOCKED;
```

For each locked expired row, recovery applies the decision order:
1. If `cancel_requested = TRUE`: set `status = 'cancelled'`, `cancelled_at = clock_timestamp()`, `worker_id = NULL`, `lease_expires_at = NULL`, `fencing_token = fencing_token + 1`. **Never requeue.**
2. Else if `attempt < max_attempts`: set `status = 'queued'`, `worker_id = NULL`, `lease_expires_at = NULL`, `fencing_token = fencing_token + 1`.
3. Else (`attempt >= max_attempts`): set `status = 'failed'`, record lease-expiration error in `last_error`, `completed_at = clock_timestamp()`, `worker_id = NULL`, `lease_expires_at = NULL`, `fencing_token = fencing_token + 1`.

---

## 5. Fencing Tokens & Completion/Recovery Serialization

Checking only `status = 'running' AND fencing_token = :held_token` leaves an unsafe window between lease expiration (`lease_expires_at <= clock_timestamp()`) and the moment recovery increments `fencing_token`. Therefore, **all** worker-side transitions (heartbeat, `succeeded`, `failed`, `running -> queued`, and `cancelled`) must enforce `lease_expires_at > clock_timestamp()` alongside `status = 'running'`, `worker_id`, and `fencing_token`.

PostgreSQL row-level locking (`FOR UPDATE` / `UPDATE` under `READ COMMITTED`) strictly serializes worker mutations and recovery:
1. **Worker locks first, lease valid:** `lease_expires_at > clock_timestamp()` holds; worker commits. Recovery later sees a terminal state or renewed lease and skips/ignores the row.
2. **Worker locks first, lease expired (before recovery runs):** `lease_expires_at > clock_timestamp()` evaluates to `FALSE`; the conditional `UPDATE` matches `0` rows and rolls back, leaving the row for recovery.
3. **Recovery locks first:** Recovery commits the state transition and increments `fencing_token`. Any waiting worker transaction re-evaluates against the committed row version, matches `0` rows, and rolls back.

---

## 6. Cancellation Semantics & Race Serialization

### A. API-Side Cancellation (`POST /jobs/{job_id}/cancel`)
1. **If `queued`:** Attempt direct cancellation:
   ```sql
   UPDATE jobs
   SET status = 'cancelled',
       cancelled_at = clock_timestamp()
   WHERE id = :job_id
     AND status = 'queued'
   RETURNING *;
   ```
   Serialized with worker claiming (`FOR UPDATE`): whichever commits first wins.
2. **If not `queued` (or claimed concurrently), attempt running-job cancellation:**
   ```sql
   UPDATE jobs
   SET cancel_requested = TRUE,
       cancel_requested_at = COALESCE(cancel_requested_at, clock_timestamp())
   WHERE id = :job_id
     AND status = 'running'
   RETURNING *;
   ```
   - Never overwrites `succeeded`, `failed`, or `cancelled`.
   - `COALESCE(cancel_requested_at, clock_timestamp())` preserves the original request timestamp if `cancel` is called repeatedly while `running`.
3. **Zero-row fallback (`SELECT` re-read):** If the conditional `UPDATE` updates `0` rows, the API re-reads the row (`SELECT * FROM jobs WHERE id = :job_id`) to determine what happened:
   - **Row not found:** Return `404 Not Found`.
   - **Row is `queued`** (e.g., reclaimed from a dead worker between step 1 and step 2): Loop/retry the `queued -> cancelled` conditional update.
   - **Row is terminal (`succeeded`, `failed`, `cancelled`):** Return **`HTTP 200 OK`** with the current terminal job representation unchanged (idempotent response; never mutates terminal state).
4. **Serialization with concurrent completion:** Because both this API `UPDATE` and the worker's completion transaction (`SELECT ... FOR UPDATE` + conditional `UPDATE`) acquire an exclusive row lock on the `jobs` row, PostgreSQL serializes them:
   - If the API `UPDATE` commits first, `cancel_requested = TRUE` is visible when the worker locks the row for completion, causing `AND cancel_requested = FALSE` to fail and roll back `execution_effects`.
   - If the worker's completion commits `succeeded` first, the API `UPDATE ... WHERE id = :job_id AND status = 'running'` affects `0` rows; the API re-reads the row, observes `status = 'succeeded'`, and returns `200 OK` with the completed job.

### B. Worker-Side Cooperative Cancellation (`running -> cancelled`)
Workers poll `cancel_requested` via heartbeat responses and safe checkpoints. Upon observing `cancel_requested = TRUE`, the worker aborts without writing `execution_effects` and executes:

```sql
BEGIN;

SELECT id, status, worker_id, fencing_token, lease_expires_at, cancel_requested
FROM jobs
WHERE id = :job_id
FOR UPDATE;

UPDATE jobs
SET status = 'cancelled',
    cancelled_at = clock_timestamp(),
    worker_id = NULL,
    lease_expires_at = NULL
WHERE id = :job_id
  AND status = 'running'
  AND worker_id = :worker_id
  AND fencing_token = :held_token
  AND lease_expires_at > clock_timestamp()
  AND cancel_requested = TRUE;

-- Require rowcount == 1; if 0 rows updated, ROLLBACK.
COMMIT;
```

---

## 7. Execution Guarantees & Completion Transaction

### Bounded Retry Semantics
- Because expired jobs (with `cancel_requested = FALSE` and `attempt < max_attempts`) and retryable execution failures are requeued, **duplicate execution attempts are possible**.
- **Retries are bounded by `max_attempts`:** once `attempt >= max_attempts` (or on non-retryable failure / cancellation), no further attempts are scheduled.
- **Successful execution is not guaranteed:** jobs may terminate in `failed` or `cancelled` without producing completion side effects.

### Completion Transaction (9-Step Logical Sequence)
1. Begin transaction.
2. Lock the job row (`SELECT ... FOR UPDATE`).
3. Verify `status` is `'running'`.
4. Verify `fencing_token` matches `:held_token` (and `worker_id = :worker_id`).
5. Verify lease has not expired (`lease_expires_at > clock_timestamp()`).
6. Verify cancellation has not been requested (`cancel_requested = FALSE`).
7. Insert `execution_effects` row (`job_id`, `worker_id`, `fencing_token`, metrics/output).
8. Update job to `succeeded`.
9. Verify exactly one job row was updated (`rowcount == 1`) and commit; if any validation fails or `0` rows are updated, **roll back the entire transaction (including the `execution_effects` insert)**.

```sql
BEGIN;

SELECT id, status, worker_id, fencing_token, lease_expires_at, cancel_requested
FROM jobs
WHERE id = :job_id
FOR UPDATE;

-- Validate steps 3-6 in transaction; roll back immediately if any check fails.

INSERT INTO execution_effects (job_id, worker_id, fencing_token, ...)
VALUES (:job_id, :worker_id, :held_token, ...);

UPDATE jobs
SET status = 'succeeded',
    completed_at = clock_timestamp(),
    worker_id = NULL,
    lease_expires_at = NULL
WHERE id = :job_id
  AND status = 'running'
  AND worker_id = :worker_id
  AND fencing_token = :held_token
  AND lease_expires_at > clock_timestamp()
  AND cancel_requested = FALSE;

-- Verify rowcount == 1; if 0 rows updated, ROLLBACK (including execution_effects).
COMMIT;
```

### Atomicity & Boundary Guarantees
- Committing `execution_effects` and `status = 'succeeded'` in one ACID transaction guarantees that neither orphaned effects nor effect-less `succeeded` jobs can become visible.
- **Exactly-once committed database effects are scoped strictly to this PostgreSQL transaction**, and **`UNIQUE(job_id)` provides at-most-one committed effect per job** at the schema level.
- External side effects (files, logs, HTTP calls, CPU cycles) lie outside PostgreSQL's transaction boundary and may recur across duplicate execution attempts unless made idempotent externally.

---

## 8. Implementation-Level Limitations & Unresolved Risks

All behaviors above are **pending implementation and testing** and carry the following engineering risks:
1. **Heartbeat Decoupling in Synchronous Workers:** With synchronous `SQLAlchemy 2.x` + `Psycopg 3`, long blocking steps on the worker thread would stall heartbeats unless heartbeats run on a dedicated thread (with its own DB connection) or workloads yield between short checkpoints.
2. **Parameter Tuning:** `lease_ttl`, `heartbeat_interval`, `poll_interval`, `recovery_interval`, and `max_attempts` are not yet calibrated; misconfiguration risks false lease expirations or slow crash recovery.
3. **Polling Load vs. Latency:** Fixed-interval polling across `>= 4` workers balances DB query rate against pickup latency and requires load testing.
4. **Connection Pool Contention:** API requests, worker loops, background heartbeat threads, and recovery sweeps share PostgreSQL connection limits.
5. **External Side Effects:** Non-DB side effects remain outside PostgreSQL's ACID boundary.
6. **Offset/Limit Pagination Drift:** Concurrent inserts can shift page boundaries under offset/limit pagination on `GET /jobs`.
7. **Unverified Runtime Behavior:** No migrations, API handlers, workers, Docker Compose setup, or `pytest` concurrency/crash tests have been implemented or run yet.
