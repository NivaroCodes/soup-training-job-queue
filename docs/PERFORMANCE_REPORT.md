# Performance Baseline & 100 RPS Validation Report

> **Verdict:** **`SHIP at 100 RPS`** (verified across `AGY-011`, `AGY-012`, and `AGY-013` evidence reconciliation)
> **Stage:** `AGY-013` — Sustained Load Evidence Reconciliation (`AGY-012` counts, warm-up accounting, and telemetry windows reconciled against PostgreSQL and raw `.json` and `.log` artifacts, both tracked in Git).
> **Git Revision:** `90758d713c3a906a7cbff0f79e6249f4d072e43a` (+ `AGY-011` optimizations)

---

## 1. Executive Summary

We benchmarked the unmodified Docker Compose stack (`api`, `postgres:16-alpine`, `8` independent `worker` containers) across a `10 RPS` low-load baseline and `25`, `50`, and `100 RPS` for all five API operations plus a representative mixed workload (`19` total runs, all artifacts preserved in `artifacts/perf/`).

- **25 RPS & 50 RPS:** All endpoints and the mixed workload sustained target RPS with `0` errors and `p99 <= 36.5 ms`.
- **100 RPS:**
  - Read, idempotent-replay, and cancel endpoints sustained `100 RPS` (`post_duplicate`: `100.01 RPS`, `p99=51.21 ms`; `get_job`: `100.0 RPS`, `p99=110.19 ms`; `get_list`: `99.88 RPS`, `p99=68.41 ms`; `post_cancel`: `99.98 RPS`, `p99=15.72 ms`).
  - **`POST /jobs` (unique keys) failed to sustain 100 RPS:** achieved **`86.2 / 100.0 RPS`**, with latency degrading **36x** (`p50=558.35 ms`, `p95=1267.05 ms`, `p99=1683.93 ms`, `max=3201.76 ms`).
  - **Mixed workload at 100 RPS** achieved `99.98 RPS` but suffered severe tail-latency degradation (`p95=339.32 ms`, `p99=541.32 ms`, `max=910.80 ms`).
- **Correctness Under Load:** Across all `19` runs, there were `0` HTTP `4xx/5xx` errors, `0` connection errors/timeouts, `0` duplicate `execution_effects`, and `0` inconsistent job states.
- **Verdict Update (`AGY-011`):** Following Optimization A (removing the redundant post-commit `SELECT` + lazy-load in `JobService.create_job()`) and Optimization B (scaling Uvicorn to `4` processes with bounded DB connection pools), `post_unique` at `100 RPS` improved from `86.20 RPS` (`p99=1683.93 ms`) to **`99.97 RPS` (`p99=21.36 ms`)**, and `mixed` at `100 RPS` improved to **`99.98 RPS` (`p99=23.35 ms`)** with `0` errors and `0` correctness regressions (`49 passed` in `pytest`). Updated verdict: **`SHIP at 100 RPS`**.

---

## 2. Phase 1 — Architecture & Query Plan Audit

### Runtime & Pool Configuration
- **API (`docker-compose.yml`):** Single `uvicorn app.main:app --host 0.0.0.0 --port 8000` process (`workers=1`), synchronous FastAPI routes (AnyIO default 40-thread pool), `DB_POOL_SIZE=10`, `DB_MAX_OVERFLOW=20` (30 max connections).
- **Workers (`8` containers):** `WORKER_POLL_INTERVAL_SECONDS=0.2`, `WORKER_LEASE_TTL_SECONDS=30.0`, `WORKER_HEARTBEAT_INTERVAL_SECONDS=10.0`, `WORKER_RECOVERY_INTERVAL_SECONDS=5.0`, `DB_POOL_SIZE=5`, `DB_MAX_OVERFLOW=10`.
- **PostgreSQL (`postgres:16-alpine`):** `max_connections = 100`, pre-populated with `10,000` rows from Run C (`9,986` `succeeded`, `14` `cancelled`).

### Verified `EXPLAIN` Query Plans (Pre-Benchmark)
- `claim_next_job`: `Limit -> LockRows -> Index Scan using ix_jobs_queued_poll on jobs` (uses partial index).
- `list_jobs_all`: `Limit -> Index Only Scan using ix_jobs_created_at_id on jobs` (plus a separate `SELECT count(jobs.id)` on every request).
- `list_jobs_filtered`: `Limit -> Index Only Scan using ix_jobs_status_created_at_id on jobs`.
- `recover_expired`: `Limit -> LockRows -> Sort -> Index Scan using ix_jobs_status_created_at_id on jobs` (falls back to `ix_jobs_running_lease_recovery` when running rows exist).

---

## 3. Load Testing Methodology

Implemented in [scripts/load_test.py](../scripts/load_test.py):
- **Target Rates:** `25`, `50`, and `100 RPS` (plus a `10 RPS` low-load mixed baseline).
- **Warm-up & Duration:** `2s` warm-up + `8s` measured for individual endpoints; `3s` warm-up + `15s` measured for `mixed` (`concurrency=64`, `seed=42`).
- **Payload & Scoping:** `payload={"model": "perf", "idx": ..., "steps": 2, "step_duration_ms": 15.0}` (~30ms simulated execution). All test keys use unique `perf_<run_id>:` prefixes so existing database data is preserved without destructive truncation.
- **Mixed Workload Distribution:** `40%` `post_unique`, `15%` `post_duplicate`, `20%` `get_job`, `15%` `get_list`, `10%` `post_cancel`.
- **Telemetry:** Per-request client latency (`p50`, `p95`, `p99`, `max`), HTTP status/error counts, `0.5s`-interval `pg_stat_activity` sampling (`active_conns`, `total_conns`, `lock_waits`, `max_active_query_ms`), `docker stats --no-stream` snapshot, and post-drain correctness checks.

---

## 4. Baseline Results

### Low-Load & Mixed Workload (`mixed`)

| Target RPS | Achieved RPS | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) | 2xx / 4xx / 5xx | Conn Err / Timeout | PG Max Conns (Active / Locks) | Jobs Succeeded (Avg Wait ms) | Dup FX / Inconsistent | Artifact |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| 10 | 10.09 | 15.39 | 22.62 | 24.79 | 26.24 | 80 / 0 / 0 | 0 / 0 | 13 (0 / 0) | 97 (43.76) | 0 / 0 | `perf_mixed_10rps_20261112_101941_7c9ad5` |
| 25 | 25.05 | 14.45 | 20.40 | 23.66 | 25.28 | 375 / 0 / 0 | 0 / 0 | 13 (2 / 0) | 229 (45.28) | 0 / 0 | `perf_mixed_25rps_20261112_102430_876b08` |
| 50 | 50.04 | 13.71 | 18.01 | 23.97 | 33.59 | 750 / 0 / 0 | 0 / 0 | 13 (1 / 0) | 394 (46.06) | 0 / 0 | `perf_mixed_50rps_20261112_102452_92116f` |
| **100** | **99.98** | **18.82** | **339.32** | **541.32** | **910.80** | 1500 / 0 / 0 | 0 / 0 | 23 (3 / 0) | 744 (45.58) | 0 / 0 | `perf_mixed_100rps_20261112_102515_25341e` |

### Independent Endpoint Benchmarks (`25 / 50 / 100 RPS`)

| Scenario | Target RPS | Achieved RPS | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) | PG Max Conns (Active / Locks) | Jobs Succeeded | Artifact |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| `post_unique` | 25 | 25.07 | 15.98 | 20.83 | 26.51 | 33.94 | 13 (1 / 0) | 250 | `perf_post_unique_25rps_20261112_101956_d1e852` |
| `post_unique` | 50 | 50.02 | 15.35 | 30.93 | 36.49 | 46.28 | 13 (2 / 0) | 500 | `perf_post_unique_50rps_20261112_102010_31df57` |
| **`post_unique`** | **100** | **86.20** | **558.35** | **1267.05** | **1683.93** | **3201.76** | **23 (3 / 0)** | **1000** | `perf_post_unique_100rps_20261112_102024_97a33d` |
| `post_duplicate` | 25 | 25.08 | 9.92 | 13.16 | 14.91 | 15.09 | 13 (1 / 0) | 50 (seed) | `perf_post_duplicate_25rps_20261112_102049_3cabc5` |
| `post_duplicate` | 50 | 50.05 | 9.47 | 19.79 | 32.27 | 38.84 | 14 (1 / 0) | 50 (seed) | `perf_post_duplicate_50rps_20261112_102104_122274` |
| `post_duplicate` | 100 | 100.01 | 8.96 | 19.72 | 51.21 | 87.90 | 13 (1 / 0) | 50 (seed) | `perf_post_duplicate_100rps_20261112_102118_f04ebd` |
| `get_job` | 25 | 25.09 | 9.22 | 13.08 | 14.62 | 41.23 | 13 (0 / 0) | 50 (seed) | `perf_get_job_25rps_20261112_102133_d76c5f` |
| `get_job` | 50 | 50.06 | 9.20 | 12.20 | 13.33 | 16.01 | 13 (1 / 0) | 50 (seed) | `perf_get_job_50rps_20261112_102147_47a91c` |
| `get_job` | 100 | 100.00 | 8.69 | 71.55 | 110.19 | 162.30 | 17 (1 / 0) | 50 (seed) | `perf_get_job_100rps_20261112_102202_1afc69` |
| `get_list` | 25 | 25.07 | 16.34 | 22.42 | 26.01 | 31.96 | 13 (1 / 0) | — | `perf_get_list_25rps_20261112_102229_9d8572` |
| `get_list` | 50 | 50.02 | 15.15 | 21.12 | 36.06 | 71.25 | 13 (1 / 0) | — | `perf_get_list_50rps_20261112_102243_a3fecb` |
| `get_list` | 100 | 99.88 | 19.02 | 44.21 | 68.41 | 102.29 | 14 (2 / 0) | — | `perf_get_list_100rps_20261112_102257_2afd62` |
| `post_cancel` | 25 | 25.09 | 11.35 | 18.79 | 26.13 | 32.03 | 13 (1 / 0) | 270 | `perf_post_cancel_25rps_20261112_102311_2af246` |
| `post_cancel` | 50 | 50.05 | 10.77 | 14.79 | 18.01 | 26.67 | 13 (1 / 0) | 520 | `perf_post_cancel_50rps_20261112_102329_61718f` |
| `post_cancel` | 100 | 99.98 | 9.63 | 12.14 | 15.72 | 17.08 | 14 (2 / 0) | 1020 | `perf_post_cancel_100rps_20261112_102351_17ae49` |

*(All 19 runs recorded `0` 4xx, `0` 5xx, `0` connection errors, `0` timeouts, `0` duplicate effects, and `0` inconsistent states.)*

---

## 5. Bottleneck Analysis (Evidence-Based)

Based on the measured telemetry during `post_unique` at `100 RPS`:
1. **PostgreSQL is NOT the bottleneck:**
   - `max_total_conns = 23` (vs `100` `max_connections` limit), `max_active_conns = 3`, `max_lock_waits = 0`, `max_active_query_ms = 1.26 ms`.
   - `postgres-1` container CPU was `8.22%` and memory `104.4 MiB / 7.571 GiB`.
2. **Workers are NOT the bottleneck:**
   - All `8` workers stayed at `0.90%–0.95%` CPU and `~48.6 MiB` memory, draining `1000/1000` jobs with `avg_queue_wait_ms = 51.77 ms`.
3. **Primary Bottleneck — Single-Process API Write Path (`api-1`):**
   - `api-1` CPU reached `33.90%` (single Uvicorn worker process with Python GIL + synchronous AnyIO threadpool), while `post_unique` throughput plateaued at **`86.2 RPS`** (`p50=558.35 ms`).
   - Inspection of `JobService.create_job()` ([app/services/job_service.py](../app/services/job_service.py)) shows **redundant database round-trips** on every new job:
     1. `SELECT` by `idempotency_key` (`get_by_idempotency_key` with `selectinload`).
     2. `INSERT INTO jobs` + `COMMIT`.
     3. **Post-commit `SELECT` by `job.id` + `selectinload(execution_effect)`** (`get_by_id`) even though a newly created `queued` job already has all columns populated on `job` (via `RETURNING` / ORM `refresh`) and cannot have an `execution_effect`.
   - When write latency rises past `~10 ms` under `100 RPS` arrival, the single Uvicorn worker queues requests behind its synchronous threadpool/GIL and extra DB round-trips.

---

## 6. Applied Optimizations & Before/After Comparison (`AGY-011`)

### Optimization A — Remove Redundant Post-Commit Read (`app/services/job_service.py`)
- **Change:** Replaced `JobRepository.get_by_id(session, job.id)` after `session.commit()` with `set_committed_value(job, "execution_effect", None)` and returned `(job, True)`.
- **Why:** `session_factory` uses `expire_on_commit=False`, and PostgreSQL `INSERT ... RETURNING` already populates `id`, `created_at`, and `updated_at`. Setting `execution_effect` to `None` prevents Pydantic `JobResponse.model_validate(job)` from triggering a lazy-load `SELECT` on `execution_effects`.
- **Verification:** Added `test_create_job_unique_triggers_no_post_commit_select_or_lazy_load` confirming exactly `1` `SELECT` + `1` `INSERT` and `0` `execution_effects` queries.

### Optimization B — Uvicorn Process Scaling & Connection Budget (`docker-compose.yml`)
- **Change:** Scaled Uvicorn from `1` to `4` worker processes (`--workers 4`) and tightened per-process DB pool limits:
  - **API (`4` processes):** `DB_POOL_SIZE=5`, `DB_MAX_OVERFLOW=5` -> `4 * 10 = 40` max API connections.
  - **Workers (`8` containers):** `DB_POOL_SIZE=3`, `DB_MAX_OVERFLOW=2` -> `8 * 5 = 40` max worker connections.
  - **Headroom:** `100 (max_connections) - 40 - 40 = 20` connections reserved for `migrate`, admin, and monitoring.

### Before / Optimization A / Optimization A + B Comparison

| Scenario (Target RPS) | Stage | Achieved RPS | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) | PG Max Conns | Jobs Succeeded | Artifact |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| `post_unique` (25) | Baseline | 25.07 | 15.98 | 20.83 | 26.51 | 33.94 | 13 | 250 | `perf_post_unique_25rps_20261112_101956_d1e852` |
| `post_unique` (25) | Opt A | 25.08 | 11.95 | 14.99 | 17.06 | 20.44 | 14 | 250 | `perf_post_unique_25rps_20261112_103045_326ca9` |
| `post_unique` (25) | Opt A + B | 25.09 | 11.65 | 16.05 | 19.00 | 20.82 | 13 | 250 | `perf_post_unique_25rps_20261112_103237_f71e85` |
| `post_unique` (50) | Baseline | 50.02 | 15.35 | 30.93 | 36.49 | 46.28 | 13 | 500 | `perf_post_unique_50rps_20261112_102010_31df57` |
| `post_unique` (50) | Opt A | 50.05 | 11.18 | 13.82 | 15.76 | 22.26 | 15 | 500 | `perf_post_unique_50rps_20261112_103059_25460b` |
| `post_unique` (50) | Opt A + B | 50.05 | 11.21 | 15.50 | 22.71 | 33.10 | 13 | 500 | `perf_post_unique_50rps_20261112_103251_6b5a01` |
| `post_unique` (100) | Baseline | 86.20 | 558.35 | 1267.05 | 1683.93 | 3201.76 | 23 | 1000 | `perf_post_unique_100rps_20261112_102024_97a33d` |
| `post_unique` (100) | Opt A | 98.89 | 10.36 | 22.26 | 175.70 | 237.84 | 19 | 1000 | `perf_post_unique_100rps_20261112_103113_598e36` |
| **`post_unique` (100)** | **Opt A + B** | **99.97** | **10.69** | **16.90** | **21.36** | **31.66** | **15** | **1000** | `perf_post_unique_100rps_20261112_103305_d75990` |
| `mixed` (100) | Baseline | 99.98 | 18.82 | 339.32 | 541.32 | 910.80 | 23 | 744 | `perf_mixed_100rps_20261112_102515_25341e` |
| `mixed` (100) | Opt A | 99.99 | 11.69 | 23.73 | 72.56 | 116.06 | 23 | 744 | `perf_mixed_100rps_20261112_103127_e4ae2f` |
| **`mixed` (100)** | **Opt A + B** | **99.98** | **11.16** | **18.50** | **23.35** | **43.09** | **15** | **744** | `perf_mixed_100rps_20261112_103319_967f6d` |

*(All optimization runs recorded `0` 4xx/5xx, `0` connection errors/timeouts, `0` lock waits, `0` duplicate effects, and `0` inconsistent states. Full regression suite: `49 passed, 5 warnings in 2.75s`.)*

---

## 7. Sustained Load & Stability Validation (`AGY-012`)

### Environment Verified Prior to Run
- **API:** `1` container (`soup-training-job-queue-api-1`) running `4` Uvicorn worker processes (`PIDs 8, 9, 10, 11`), `DB_POOL_SIZE=5`, `DB_MAX_OVERFLOW=5`.
- **Workers:** `8` independent worker containers (`worker-1`..`worker-8`), `DB_POOL_SIZE=3`, `DB_MAX_OVERFLOW=2`.
- **PostgreSQL:** `postgres:16-alpine` (`max_connections=100`, healthy).

### Sustained 100 RPS Results (`150,000` Total Measured Requests)

| Workload | Duration (Warmup) | Target / Achieved RPS | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) | 2xx / 4xx / 5xx | Conn Err / Timeout | PG Max Conns (Active / Locks) | Measured Unique (Seed / Warmup / Total Run Succeeded) | Dup FX / Inconsistent | Restarts | Artifact |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **Mixed** (`40/15/20/15/10%`) | 15m (`120s`) | `100.0 / 99.97` | 15.38 | 33.82 | 47.82 | 416.69 | 90,000 / 0 / 0 | 0 / 0 | 28 (4 / 0) | `35,950` (`50` / `4,813` / `40,813`) | 0 / 0 | 0 | `perf_mixed_100rps_20261112_103730_1d7d1d` |
| **Unique `POST /jobs`** | 5m (`10s`) | `100.0 / 99.98` | 13.82 | 24.21 | 31.71 | 176.02 | 30,000 / 0 / 0 | 0 / 0 | 28 (4 / 0) | `30,000` (`0` / `1,000` / `31,000`) | 0 / 0 | 0 | `perf_post_unique_100rps_20261112_105450_db4a2b` |
| **Read-Heavy** (`50% GET /{id}`, `50% GET /jobs`) | 5m (`10s`) | `100.0 / 99.97` | 23.44 | 46.12 | 67.45 | 266.01 | 30,000 / 0 / 0 | 0 / 0 | 28 (5 / 0) | `0` (`50` / `0` / `50`) | 0 / 0 | 0 | `perf_read_heavy_100rps_20261112_110017_9b041c` |

### Job Count & Warm-Up Reconciliation (`AGY-013`)
All job counts in `AGY-012` are scoped per run via `idempotency_key LIKE '<run_id>:%'` (never using global database totals; `20,312` pre-AGY-012 rows remained untouched in PostgreSQL, bringing global total to `92,175` jobs):
- **Mixed (`perf_mixed_100rps_20261112_103730_1d7d1d`):** `12,000` warm-up requests (`4,813` unique warm-up jobs `wu:`) + `90,000` measured requests (`35,950` unique measured jobs `u:`) + `50` pre-seeded jobs (`seed:`) = **`40,813` succeeded jobs** and **`40,813` `execution_effects`**.
- **Unique `POST /jobs` (`perf_post_unique_100rps_20261112_105450_db4a2b`):** `1,000` warm-up requests (`1,000` `wu:` jobs) + `30,000` measured requests (`30,000` `u:` jobs) = **`31,000` succeeded jobs** and **`31,000` `execution_effects`**.
- **Read-Heavy (`perf_read_heavy_100rps_20261112_110017_9b041c`):** `50` pre-seeded jobs (`seed:`) + `1,000` warm-up read requests + `30,000` measured read requests = **`50` succeeded jobs** and **`50` `execution_effects`**.

### Telemetry Window Reconciliation (`AGY-013`)
- **Mixed (`16` recorded windows `W1`–`W16`):**
  - `W1` (`0` measured requests) occurred entirely inside the `120s` warm-up phase.
  - `W2` (`197` measured requests, `rps=3.28`, `p50=11.09 ms`, `p99=21.10 ms`) straddled the end of warm-up and the start of measurement.
  - `W3`–`W16` (`14` full ~62s intervals due to ~2.2s DB/Docker sampling overhead per 60s wait) recorded `6,211`–`6,236` requests/window (`103.52`–`103.93` window-denominator RPS), with **`p50` `11.60–16.88 ms`** and **`p99` `39.27–61.12 ms`**.
  - The final ~27s tail (`~2,566` requests) finished after `W16` and is included in the `90,000`-request aggregate (`p50=15.38 ms`, `p99=47.82 ms`).
- **Unique `POST /jobs` (`5` windows `W1`–`W5`) & Read-Heavy (`5` windows `W1`–`W5`):** `W1` includes the `10s` warm-up gap (`82.95` and `81.90` window RPS); `W2`–`W5` held `p50` `13.73–13.96 ms` / `p99` `30.44–32.58 ms` (Unique POST) and `p50` `23.00–23.74 ms` / `p99` `64.53–70.77 ms` (Read-Heavy).

### Per-Endpoint Breakdown (Sustained Mixed & Read-Heavy)
- **Mixed (15 min):**
  - `POST /jobs (unique)` (`35,950`): `p50=15.80 ms`, `p95=32.41 ms`, `p99=44.31 ms`, `0` errors.
  - `POST /jobs (duplicate)` (`13,496`): `p50=13.11 ms`, `p95=27.00 ms`, `p99=38.80 ms`, `0` errors.
  - `GET /jobs/{id}` (`17,972`): `p50=12.64 ms`, `p95=25.71 ms`, `p99=36.09 ms`, `0` errors.
  - `GET /jobs` (`13,488`): `p50=24.08 ms`, `p95=44.08 ms`, `p99=60.71 ms`, `0` errors.
  - `POST /jobs/{id}/cancel` (`9,094`): `p50=14.27 ms`, `p95=31.80 ms`, `p99=43.94 ms`, `0` errors. *(Note: in `mixed`, `post_cancel` targeted the `50` already-`succeeded` `seed_job_ids`, exercising terminal-job idempotent cancellation rather than active `queued`/`running` cancellation.)*
- **Read-Heavy (5 min):**
  - `GET /jobs/{id}` (`15,035`): `p50=13.90 ms`, `p95=31.01 ms`, `p99=49.52 ms`, `max=221.64 ms`, `0` errors.
  - `GET /jobs` (`14,965`): `p50=28.27 ms`, `p95=53.61 ms`, `p99=74.75 ms`, `max=266.01 ms`, `0` errors.

### Stability Conclusions
- **Latency Stability:** Across all `14` full measured windows (`W3`–`W16`) of the 15-minute mixed run, `p50` held steady at `11.60–16.88 ms` and `p99` at `39.27–61.12 ms`; across the 5-minute unique-write run (`W1`–`W5`), `p50` held at `13.61–13.96 ms` and `p99` at `30.44–32.58 ms`.
- **Throughput & Queue Drain:** Achieved `99.97–99.98 RPS` sustained; queue depth remained `<= 4` active jobs at every 60s sample and drained cleanly to `0` after each run (`40,813` succeeded in mixed, `31,000` in unique-write).
- **Resource & Connection Bounds:** `pg_max_conns` peaked at `28 / 100` (`max_active_conns=4–5`, `max_lock_waits=0`). Zero container restarts occurred across all 10 containers (`api-1` CPU `29.6%` mixed / `37.4%` write / `1.4%` read; `postgres-1` CPU `5.7%` mixed / `4.6%` write / `12.4%` read).

---

## 8. Remaining Limitations

- `GET /jobs` (`p50=28.27 ms` on ~88k rows, `postgres-1` CPU `12.35%`) executes `SELECT count(jobs.id)` on every request; cursor pagination or approximate/optional counts would be needed at multi-million-row scale.
- Simulated jobs use short `30 ms` execution (`job_steps=2`, `step_duration_ms=15.0`); longer GPU-style jobs would increase `running` concurrency without changing API submission throughput.
- External non-DB side effects and multi-node clock skew remain outside the PostgreSQL transaction/clock boundary.

---

## 9. Raw Artifact Index

All 19 baseline runs plus optimization and sustained load runs (30 total `.json` runs) are preserved as tracked JSON artifacts in [artifacts/perf/](../artifacts/perf/):
- Mixed: `perf_mixed_{10,25,50,100}rps_*.json`
- Unique POST: `perf_post_unique_{25,50,100}rps_*.json`
- Duplicate POST: `perf_post_duplicate_{25,50,100}rps_*.json`
- Get Job: `perf_get_job_{25,50,100}rps_*.json`
- List Jobs: `perf_get_list_{25,50,100}rps_*.json`
- Cancel Job: `perf_post_cancel_{25,50,100}rps_*.json`

*(Note on artifact formats: Structured performance metrics and telemetry are preserved in tracked `.json` artifacts under `artifacts/perf/`, with paired human-readable `.log` console outputs. Both formats are committed to Git.)*
