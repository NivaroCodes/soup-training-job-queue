# Chaos Testing & Evidence Audit Report

> **Historical Stage Verdict (`AGY-009`):** **`DON'T SHIP at 100 RPS yet`** *(at the time of `AGY-009`, no 100 RPS sustained load test had been executed; see [PERFORMANCE_REPORT.md](PERFORMANCE_REPORT.md) for subsequent `AGY-011` optimizations and `AGY-012` 100 RPS sustained validation)*.
> **Scope:** Evidence-based audit of Docker Compose orchestration, the original `48` PostgreSQL-backed `pytest` tests (`AGY-008`/`AGY-009`; later expanded to `49` tests in `AGY-011`), a 100-job smoke run, a 500-job/8-worker chaos pilot, and a 10,000-job/8-worker `SIGKILL` chaos run.

---

## 1. Executive Summary

The `soup-training-job-queue` service was verified across three complementary evidence layers:
1. **Automated PostgreSQL Integration Suite (`pytest`):** `48` tests passed at the time of `AGY-008`/`AGY-009` (`tests/test_database_models.py`, `tests/test_api_jobs.py`, `tests/test_worker_engine.py`), verifying schema constraints, Alembic reversibility, API idempotency/cancellation, and deterministic worker leasing, fencing, failure precedence, and recovery races. *(Later expanded to `49` passing tests in `AGY-011`.)*
2. **Docker Compose Smoke Run (`run_20261112_095909_seed42_55d8b3`):** `100` jobs across `4` independent worker containers with chaos disabled completed in `4.697s` (`100` succeeded, `100` execution effects, `0` errors, all 6 invariants `PASS`).
3. **Docker Compose Chaos Pilot & Full Chaos Run:**
   - **Pilot (`run_20261112_100147_seed42_442bd4`):** `500` jobs across `8` worker containers, `1` `SIGKILL` + `1` restart, `500` succeeded, `500` effects, all 6 invariants `PASS`.
   - **Full Chaos (`run_20261112_100304_seed42_9b42c4`):** `10,000` logical jobs across `8` worker containers over `121.739s`, `13` `SIGKILL` events + `13` restarts, `1,500` duplicate idempotent `POST` requests, `20` `HTTP 409` probes, `1,000` concurrent `POST /cancel` requests -> `9,986` succeeded (`9,986` effects), `14` cancelled, `0` lost jobs, `0` duplicate effects, `0` stuck jobs, all 6 invariants `PASS`. *(Note on cancellation coverage: While 1,000 concurrent cancellation requests were issued during Run C, the short duration of simulated jobs (~30 ms) meant that almost all jobs finished before cancellation requests arrived, resulting in exactly 14 actual cancellation transitions. This verified race safety with completion and terminal state immutability, but active cooperative abortion of in-flight running jobs is deterministically covered by the pytest integration suite rather than by high-churn chaos timing.)*

**Audit Caveat:** The chaos runs demonstrate correctness and crash-recovery integrity under the tested workload (`job_steps=2`, `step_duration_ms=15.0`, effective `SIGKILL` cadence `~9.1s` due to synchronous container restart wait), **not** a strict 2.0s wall-clock kill cadence, sustained 100 RPS throughput, tail latency (p95/p99), or connection-pool saturation.

---

## 2. Test Environment

- **Host OS:** Windows (PowerShell) with Docker Desktop (`Docker Compose v5.3.1`)
- **Python Runtime:** Python `3.12.0` (`uv` lockfile `uv.lock`)
- **Database (Docker Compose):** `postgres:16-alpine` on container port `5432` (host-mapped to `5433`, persistent volume `pg_data`)
- **Database (Local `pytest`):** Local PostgreSQL service on `127.0.0.1:5432` (`soup_training_test`)
- **API Server:** `uvicorn app.main:app --host 0.0.0.0 --port 8000` (FastAPI + synchronous SQLAlchemy 2.x + Psycopg 3)

---

## 3. Docker Compose Architecture

Defined in [docker-compose.yml](../docker-compose.yml) and [Dockerfile](../Dockerfile):
- **`postgres`:** `postgres:16-alpine` with persistent `pg_data` volume and `pg_isready` healthcheck (`interval: 2s`, `retries: 15`).
- **`migrate`:** One-shot `alembic upgrade head` gated on `postgres` (`service_healthy`), `restart: "no"`.
- **`api`:** FastAPI/Uvicorn gated on `postgres` (`service_healthy`) and `migrate` (`service_completed_successfully`), `/health` check, `restart: unless-stopped`. During Runs A–C (`AGY-008`), `api` ran `1` Uvicorn process with `DB_POOL_SIZE=10`, `DB_MAX_OVERFLOW=20` *(subsequently tuned to `--workers 4` and `DB_POOL_SIZE=5`, `DB_MAX_OVERFLOW=5` in `AGY-011`; see [PERFORMANCE_REPORT.md](PERFORMANCE_REPORT.md))*.
- **`worker`:** `python -m app.worker` gated on `postgres` (`service_healthy`) and `migrate` (`service_completed_successfully`), `deploy.replicas: 4` by default (scaled to `8` via `--scale worker=8`), `restart: unless-stopped`.

---

## 4. Worker Configuration

- **Default Compose Settings (`docker-compose.yml`):** `WORKER_POLL_INTERVAL_SECONDS=0.2`, `WORKER_LEASE_TTL_SECONDS=30.0`, `WORKER_HEARTBEAT_INTERVAL_SECONDS=10.0`, `WORKER_RECOVERY_INTERVAL_SECONDS=5.0`, `WORKER_RECOVERY_BATCH_SIZE=50`.
- **Worker Identity:** Each container generates a unique `worker_id` (`worker-<container_hostname>-<pid>-<rand8>`).
- **Workload Parameters Used in Runs A–C:** `job_steps=2`, `step_duration_ms=15.0` (~30 ms simulated compute per job), `max_attempts=5`, `concurrency=32` HTTP submission threads.

---

## 5. Chaos Harness Design

Implemented in [scripts/chaos_harness.py](../scripts/chaos_harness.py) and verified by unit tests in [tests/test_chaos_harness.py](../tests/test_chaos_harness.py):
- **Deterministic Seeding:** Uses `random.Random(seed)` (`seed=42` in Runs A–C) to select duplicate (`15%`), cancellation (`10%`), and `409` conflict-probe (`20`) indices, as well as random `SIGKILL` worker targets.
- **Concurrent Workload:** Submits unique jobs (`idempotency_key = "<run_id>:job:<idx>"`), concurrent duplicate `POST /jobs`, `409` conflict probes (reused key with tampered payload), and concurrent `POST /jobs/{id}/cancel` requests via `ThreadPoolExecutor(max_workers=32)`.
- **Decoupled Chaos Scheduler (AGY-021 / AGY-022 / AGY-024 / AGY-025):** Replaced synchronous kill-and-wait loop with a non-blocking `ChaosScheduler` class. The scheduler fires `docker kill -s SIGKILL` on a strict monotonic timer (`time.monotonic()`), maintains a thread-safe `_recovering_containers` tracking set to prevent re-killing workers already restarting, skips missed ticks without burst execution, and separates kill execution (`_kill_pool`) from container restart recovery (`_recovery_pool`). This separation prevents recovery polling backpressure from delaying actual kill ticks. Structured restart accounting separates actual `docker start` attempts (`restart_attempts`) from total recovery events (`recovery_count`), auto-restarts (`auto_restarts`), manual restarts (`manual_restarts`), failed starts (`failed_starts`), recovery timeouts (`recovery_timeouts`), and shutdown cancellations (`cancelled_by_shutdown`).
- **Synchronized Active Cancellation Cohort (AGY-021 / AGY-025):** To address the low active-cancellation coverage in Run C (~30 ms jobs finished before cancellation arrived, yielding only 14 cancellations), the harness includes a dedicated `ActiveCancellationCohort`. The cohort submits long-running probe jobs (40 steps of 500 ms = 20s total, key prefix `<run_id>:active_cancel_probe:<idx>`) and enforces a synchronization barrier: all probes must be confirmed `running` before ANY cancellation request is released. If the barrier is satisfied, cancellation requests are dispatched concurrently, terminal `cancelled` states are verified, and 0 execution effects are confirmed. In the main harness, active cancellation probes run concurrently while background SIGKILL chaos is active.
- **Invariants Verified:** Evaluates 7 formal correctness invariants post-run:
  1. `no_lost_jobs`: `succeeded + failed + cancelled == total_db_jobs == submitted`
  2. `at_most_one_execution_effect`: `0 <= effects <= 1` per job
  3. `no_orphaned_execution_effects`: every effect corresponds to a `succeeded` job
  4. `no_stuck_running_jobs`: 0 jobs remaining in `running` or `queued` state
  5. `monotonic_fencing_tokens`: max fencing token >= attempts count
  6. `terminal_state_stability`: zero changes upon re-polling terminal jobs
  7. `active_cancellation_integrity` *(AGY-021)*: 100% of probed active cancellation jobs reach terminal `cancelled` with 0 execution effects
- **Artifact Preservation:** Writes immutable `<run_id>.log` and `<run_id>.json` files in `artifacts/chaos/` without overwriting prior runs, returning non-zero exit code if any invariant fails. Both `.json` and `.log` artifacts are committed to Git. JSON files preserve structured results; paired `.log` files preserve human-readable console output.

---

## 6. Test Methodology

Evidence is separated into four distinct tiers:
1. **Unit & PostgreSQL Integration Tests (`pytest`):** Deterministic multi-threaded (`threading.Barrier`, `threading.Event`) and SQL-level tests verifying constraints, migrations, API races, and worker/recovery edge cases that may occur rarely under random timing.
2. **Smoke Test (Run A):** End-to-end Docker Compose verification with 4 workers and `--no-chaos` to validate clean container startup, migration ordering, API/worker connectivity, and drain.
3. **Chaos Pilot (Run B):** 500-job / 8-worker run with `SIGKILL` enabled to validate harness kill/restart logging and invariant queries before scaling up.
4. **Full Chaos Test (Run C):** 10,000-job / 8-worker run with concurrent duplicates, `409` probes, cancellations, and repeated `SIGKILL` + restarts.

---

## 7. Results by Run

| Run | Run ID | Jobs | Workers | Seed | Chaos (`kill_interval`) | Duration | Final Counts | Effects | Kills / Restarts | HTTP Errors | Overall |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :--- | :---: | :---: | :---: | :---: |
| **Run A (Smoke)** | `run_20261112_095909_seed42_55d8b3` | 100 | 4 | 42 | Disabled (`--no-chaos`) | 4.697s | `succeeded: 100` | 100 | 0 / 0 | 0 | **PASS** |
| **Run B (Pilot)** | `run_20261112_100147_seed42_442bd4` | 500 | 8 | 42 | Enabled (`2.0s`) | 10.573s | `succeeded: 500` | 500 | 1 / 1 | 0 | **PASS** |
| **Run C (Full Chaos)** | `run_20261112_100304_seed42_9b42c4` | 10,000 | 8 | 42 | Enabled (`2.0s`*) | 121.739s | `succeeded: 9986`, `cancelled: 14` | 9,986 | 13 / 13 | 0 | **PASS** |

*\*Audit Note on Run C Kill Cadence:* Although `--kill-interval` was `2.0s`, `_chaos_loop` in `scripts/chaos_harness.py` waits synchronously for each killed container to restart (`~6.4s` on Docker Desktop) before beginning the next `2.0s` sleep, resulting in `13` `SIGKILL`s over `121.739s` (~1 kill every `~9.1s`). Consequently, Run C does not satisfy a strict 2.0s wall-clock kill cadence; decoupling kill timing from restart wait is scheduled for AGY-021.

---

## 8. Invariant Verification

All six invariants were verified directly against PostgreSQL after queue drain in Runs A, B, and C:

| Invariant | What It Checks | How It Is Checked (PostgreSQL Query / Harness Check) | Run A | Run B | Run C | Evidence Source |
| :--- | :--- | :--- | :---: | :---: | :---: | :--- |
| `no_lost_jobs` | Every submitted logical job has exactly one DB row and zero HTTP submission errors. | `COUNT(*)` on `jobs` for `run_id` prefix == `--jobs` AND `len(http_errors) == 0`. | PASS (`100/100`) | PASS (`500/500`) | PASS (`10000/10000`) | Run A/B/C `.json` + `.log` (both tracked) |
| `no_duplicate_side_effects` | No job has more than one committed `execution_effects` row. | `SELECT e.job_id FROM execution_effects e JOIN jobs j ... GROUP BY e.job_id HAVING COUNT(*) > 1` == `0` (backed by `uq_execution_effects_job_id`). | PASS (`0`) | PASS (`0`) | PASS (`0`) | Run A/B/C `.json` + `.log` (both tracked) |
| `no_stuck_running_or_queued_jobs` | All jobs drained to terminal states after recovery window. | `running == 0` AND `queued == 0` for `run_id` prefix after drain. | PASS (`0/0`) | PASS (`0/0`) | PASS (`0/0`) | Run A/B/C `.json` + `.log` (both tracked) |
| `consistent_final_states` | `succeeded` jobs have 1 effect and `cancel_requested = FALSE`; non-succeeded jobs have 0 effects. | `LEFT JOIN` / `JOIN` queries checking `succeeded_missing_effect == 0`, `non_succeeded_with_effect == 0`, `succeeded_with_cancel == 0`, `effects == succeeded`. | PASS (`100=100`) | PASS (`500=500`) | PASS (`9986=9986`) | Run A/B/C `.json` + `.log` (both tracked) |
| `fencing_integrity` | No `execution_effects` row carries a stale or invalid `fencing_token`. | `execution_effects.fencing_token != jobs.fencing_token OR execution_effects.fencing_token < 1` count == `0`. | PASS (`0`) | PASS (`0`) | PASS (`0`) | Run A/B/C `.json` + `.log` (both tracked) |
| `idempotency_and_409_conflict` | Repeated identical POSTs return the original job ID; mismatched payload reuses return `HTTP 409`. | Client-side verification of `dup_resp.id == original_id` and `conflict_resp.status_code == 409` (`20` probes/run). | PASS (`15` dup, `20` 409) | PASS (`75` dup, `20` 409) | PASS (`1500` dup, `20` 409) | Run A/B/C `.json` + `.log` (both tracked) |

---

## 9. Silent Failure Verification

| Silent Failure Case | Protection Mechanism | Evidence Tier & Test / Source | Observed Result |
| :--- | :--- | :--- | :--- |
| **Double claim** | `SELECT ... FOR UPDATE SKIP LOCKED` + atomic `UPDATE ... WHERE status = 'queued'` | **Integration (`pytest`):** `test_multiple_workers_claim_different_jobs_and_no_double_claim`, `test_four_workers_racing_on_single_job_no_double_claim` + **Runs A–C** | **VERIFIED:** 4-thread barrier race claimed each job once; 0 duplicate claims across 10,000 jobs. |
| **Zombie worker after lease expiry** | `fencing_token` + `lease_expires_at > clock_timestamp()` + `rowcount == 1` on all worker mutations; `UNIQUE(job_id)` on `execution_effects` | **Integration (`pytest`):** `test_expired_lease_rejects_...`, `test_zombie_worker_rejected_...` + **Run C** (`13` kills, `0` fencing mismatches) | **VERIFIED:** Expired/stale workers rejected with `0` stale effects committed. |
| **Cancel / complete race** | Row lock (`SELECT ... FOR UPDATE`) serializes API `cancel_requested = TRUE` vs. worker `cancel_requested = FALSE` check | **Integration (`pytest`):** `test_cooperative_cancellation_aborts_without_effect`, `test_running_cancellation_commits_cleanly`, `test_retryable_failure_...` + **Run C** (`1,000` cancels sent -> `14` cancelled, `9,986` succeeded, `0` `succeeded_with_cancel`) | **VERIFIED:** Completed jobs never overwritten; cancelled jobs never committed `execution_effects`. In Run C, 14 actual cancellations occurred; full cooperative running cancellation coverage relies on deterministic pytest integration tests. |
| **Duplicate request retry** | `UNIQUE(idempotency_key)` + SHA-256 `request_hash` + `IntegrityError` rollback/re-read | **Integration (`pytest`):** `test_concurrent_idempotent_requests_same_payload`, `..._conflicting_payloads` + **Run C** (`1,500` dupes, `20` 409 probes) | **VERIFIED:** `0` mismatches, `0` 409 failures. |
| **Transaction isolation (`READ COMMITTED`)** | Exclusive row locks cause waiting transactions to re-evaluate `WHERE` predicates against committed row state | **Integration (`pytest`):** `test_recovery_versus_completion_race_serializes_cleanly` | **VERIFIED:** Completion blocked behind recovery, re-evaluated after commit, and rolled back cleanly. |
| **Clock skew assumptions** | All lease grants, renewals, and expiry checks use PostgreSQL server `clock_timestamp()` | **Design assumption (not experimentally skew-tested):** SQL inspection of `worker_repository.py` / `job_repository.py` | **NOT EXPERIMENTALLY TESTED:** Assumes single authoritative PostgreSQL primary clock; container clock skew not injected. |

---

## 10. Known Limitations

1. **Short Simulated Job Duration (~30 ms) & Cancellation Coverage:** In Runs A–C (`job_steps=2`, `step_duration_ms=15.0`), most jobs completed rapidly before concurrent `POST /cancel` requests arrived (`14` of `1,000` cancelled in Run C; `0` of `10` in Run A; `0` of `50` in Run B). While this confirms that concurrent cancellation requests do not corrupt completed states or double-commit effects, it does not constitute high-volume testing of active cooperative abortion in running workers. That behavior is validated deterministically in the pytest suite (`tests/test_worker_engine.py`).
2. **Synchronous Restart Wait Reduces Kill Frequency:** `_chaos_loop` waits for container restart (~6.4s) before the next `2.0s` interval, yielding `13` kills in `121.7s` (~1 every `9.1s`) rather than every `2.0s` wall-clock. Run C validates recovery from repeated SIGKILLs, but does not satisfy a strict 2-second kill cadence; decoupling kill timing from restart wait is scheduled for AGY-021.
3. **Simulated Work Only:** `SimulatedJobExecutor` performs in-memory steps and commits only to PostgreSQL `execution_effects`.
4. **No Performance / Latency Profiling:** Runs A–C did not measure API p50/p95/p99 latency, sustained 100 RPS arrival rates, or PostgreSQL connection-pool saturation.

---

## 11. What the Harness Does NOT Catch

- **Network partitions** between API, workers, and PostgreSQL.
- **Mid-transaction TCP failures** (dropped/stalled packets right before `COMMIT` acknowledgment).
- **PostgreSQL primary failover** or replication lag.
- **External side-effect systems** (S3/object stores, webhooks, or non-transactional queues).
- **Long-term database growth and maintenance** (table/index bloat, `VACUUM` overhead, WAL retention).
- **Clock skew between independent database nodes** (if PostgreSQL were clustered/multi-primary).

---

## 12. What Surprised Us and What Still Concerns Us

What surprised us most during chaos testing was how subtle harness timing interactions can distort fault-injection goals: configured for a `2.0s` `SIGKILL` interval, the harness actually executed kills only every `~9.1s` because synchronous container restart polling blocked the loop, meaning the system faced less frequent crash trauma than intended. Furthermore, despite firing `1,000` concurrent cancellation requests, the ~30 ms duration of simulated jobs meant almost all jobs finished before cancellation could hit them, yielding only `14` actual cancellations and leaving active cooperative cancellation validation almost entirely dependent on deterministic `pytest` tests rather than chaos-scale churn.

*(Resolution Note — AGY-021 / AGY-025):* Both harness limitations were systematically addressed and hardened in `scripts/chaos_harness.py`:
1. **Cadence Decoupling & Dual-Pool Backpressure Fix:** Replaced the synchronous kill loop with a non-blocking `ChaosScheduler` using monotonic scheduling, a thread-safe `_recovering_containers` tracking set, and separate execution pools (`_kill_pool` for SIGKILL dispatch and `_recovery_pool` for asynchronous container restart polling). This ensures recovery polling never delays scheduled kill execution ticks.
2. **Synchronized Active Cancellation Cohort:** Implemented `ActiveCancellationCohort` with a deterministic synchronization barrier: submits long-running probe jobs (40 steps of 500 ms = 20s planned duration, key prefix `<run_id>:active_cancel_probe:<idx>`), polls until *every* probe is confirmed `running`, and releases concurrent `POST /cancel` requests only after the barrier is satisfied. In the harness orchestration, active cancellation probes run concurrently while background chaos is active.
3. **Structured Accounting & Verification:** Corrected start-attempt accounting semantics (`restart_attempts` measures actual manual `docker start` attempts, cleanly separated from auto-restarts, failed starts, recovery timeouts, and shutdown cancellations). Both subsystems are validated via 28 unit tests in `tests/test_chaos_harness.py` (`77` total tests passing in `pytest`). Note that live container re-run of this enhanced harness awaits developer startup of Docker Desktop, as the local Docker daemon remained offline during this pass.

What still concerns us for production is the behavior under conditions Docker Compose cannot simulate: unmitigated network partitions between workers and PostgreSQL, query tail latency when the `jobs` table reaches millions of rows (given `GET /jobs` performs an exact `count()`), and external side effects that lack PostgreSQL transaction atomicity.

---

## 13. Production Readiness Assessment

**Historical `AGY-009` Provisional Verdict: `DON'T SHIP at 100 RPS yet`**

- **Crash-Recovery & Correctness:** Demonstrated strong evidence across `48` PostgreSQL `pytest` tests (`49` in `AGY-011`) and 10,000-job / 8-worker `SIGKILL` chaos testing (`0` lost jobs, `0` duplicate effects, `0` stuck jobs, `0` fencing mismatches).
- **Historical Gap & Follow-Up:** At the time of `AGY-009`, sustained 100 RPS load testing had **not** been executed yet. That performance gap was subsequently investigated, optimized, and validated in `AGY-010`–`AGY-013`—see [PERFORMANCE_REPORT.md](PERFORMANCE_REPORT.md) for the updated 100 RPS verdict.

---

## 14. Reproduction Instructions (Windows PowerShell)

```powershell
# 1. Run full PostgreSQL pytest suite
.venv\Scripts\pytest.exe -v

# 2. Start 4-worker Docker Compose stack and verify workers
docker compose up -d --build
docker compose ps

# 3. Smoke run (100 jobs, 4 workers, no chaos)
.venv\Scripts\python.exe scripts/chaos_harness.py --jobs 100 --workers 4 --no-chaos --clean-db

# 4. Scale to 8 workers and run 10,000-job SIGKILL chaos test
docker compose up -d --scale worker=8
.venv\Scripts\python.exe scripts/chaos_harness.py --jobs 10000 --workers 8 --kill-interval 2.0 --seed 42 --clean-db
```

---

## 15. Evidence Artifact Index

- **Smoke Run (Run A):** [run_20261112_095909_seed42_55d8b3.json](../artifacts/chaos/run_20261112_095909_seed42_55d8b3.json)
- **Chaos Pilot (Run B):** [run_20261112_100147_seed42_442bd4.json](../artifacts/chaos/run_20261112_100147_seed42_442bd4.json)
- **Full 10,000-Job Chaos Run (Run C):** [run_20261112_100304_seed42_9b42c4.json](../artifacts/chaos/run_20261112_100304_seed42_9b42c4.json)
- **Test Suites:** [test_database_models.py](../tests/test_database_models.py), [test_api_jobs.py](../tests/test_api_jobs.py), [test_worker_engine.py](../tests/test_worker_engine.py), [test_chaos_harness.py](../tests/test_chaos_harness.py)

*(Note on artifact formats: Structured chaos evidence is preserved in tracked `.json` artifacts under `artifacts/chaos/`, with paired human-readable `.log` console outputs. Both formats are committed to Git.)*
