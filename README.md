# soup-training-job-queue

Fault-tolerant distributed training-job queue service built with Python 3.12, FastAPI, SQLAlchemy 2.x, PostgreSQL, Psycopg 3, Alembic, and Docker Compose.

---

## Key Features

- **PostgreSQL-backed job queue:** Durable single source of truth for job state, leases, fencing tokens, cancellation flags, and execution effects.
- **FastAPI REST API:** Stateless HTTP endpoints for idempotent job creation, status inspection, offset/limit paginated listing, and cancellation.
- **Independent worker processes:** Separate worker OS processes/containers polling PostgreSQL independently of the API tier.
- **Atomic job claiming:** `SELECT ... FOR UPDATE SKIP LOCKED` inside a single PostgreSQL transaction prevents concurrent double-claims.
- **Lease-based recovery and heartbeats:** Dedicated background heartbeat threads renew leases using PostgreSQL `clock_timestamp()`; expired leases are recovered automatically.
- **Fencing tokens:** Monotonically increasing `fencing_token` + live lease validity (`lease_expires_at > clock_timestamp()`) prevent stale workers from committing late results.
- **Idempotency:** Unique `idempotency_key` + deterministic SHA-256 `request_hash` returns the existing job on replay (`200 OK`) and `HTTP 409 Conflict` on payload mismatch.
- **Cooperative cancellation:** `queued` jobs cancel directly (`queued -> cancelled`); `running` jobs set `cancel_requested = TRUE` for safe-checkpoint worker abort; terminal jobs return `200 OK` unchanged.
- **Atomic database side effects:** `execution_effects` (`UNIQUE(job_id)`) and the `running -> succeeded` state transition commit in the same ACID PostgreSQL transaction.

---

## Architecture Overview

- **API (`api` service):** `uvicorn app.main:app` running `4` Uvicorn worker processes (`DB_POOL_SIZE=5`, `DB_MAX_OVERFLOW=5` per process). Exposes `GET /health`, `POST /jobs`, `GET /jobs`, `GET /jobs/{job_id}`, and `POST /jobs/{job_id}/cancel`.
- **Database (`postgres` service):** `postgres:16-alpine` (`max_connections=100`, persistent `pg_data` volume, `pg_isready` healthcheck). Schema migrations run via a one-shot `migrate` service (`alembic upgrade head`) before `api` and `worker` start.
- **Workers (`worker` service):** `python -m app.worker` (`deploy.replicas: 4` by default, scalable to `8` with `--scale worker=8` as used in chaos and load testing; `DB_POOL_SIZE=3`, `DB_MAX_OVERFLOW=2` per container).

---

## Prerequisites & Installation

- **Containerized Stack:** Docker Engine 24+ & Docker Compose v2 (recommended for full system reproduction).
- **Local Development / Pytest:** Python 3.12+, `uv` package manager, and a local PostgreSQL instance.

### Local Environment Setup

```powershell
# 1. Sync dependencies with uv (or use standard venv + pip)
uv sync

# 2. Configure local environment variables (.env.example provides defaults)
Copy-Item .env.example .env

# 3. Configure TEST_DATABASE_URL for pytest (pointing to local PostgreSQL)
$env:TEST_DATABASE_URL = "postgresql+psycopg://soup:soup@127.0.0.1:5433/soup_training"
```

---

## Quick Start (Docker Compose)

The documented, sanctioned startup procedure starts PostgreSQL 16, runs Alembic migrations automatically, launches the FastAPI API service (4 Uvicorn worker processes), and starts **4 independent worker processes** by default (`deploy.replicas: 4` in `docker-compose.yml`), satisfying the assignment requirement of at least four independent workers:

```bash
docker compose up
```

Optional detached build variant:

```bash
docker compose up -d --build
```

To scale to 8 worker containers (as used during the 10,000-job chaos and sustained 100 RPS load tests):

```bash
docker compose up -d --scale worker=8
```

Verify service health:

```powershell
docker compose ps
Invoke-RestMethod http://127.0.0.1:8000/health
```

*(Note: Docker Compose maps PostgreSQL to host port `5433` and container port `5432` to avoid conflicts with default host PostgreSQL instances. Do not commit `.env`.)*

---

## API Usage Examples

### 1. Create a Training Job (Idempotent)

```bash
curl -X POST http://127.0.0.1:8000/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: train-run-20261113-001" \
  -d '{
    "payload": {"model": "transformer-xl", "dataset": "c4-en", "epochs": 3},
    "max_attempts": 5
  }'
```

*Replays with identical payload return `HTTP 200 OK` with the existing job representation. Replays with a different payload return `HTTP 409 Conflict`.*

### 2. Get Job Status

```bash
curl http://127.0.0.1:8000/jobs/<job-id>
```

### 3. List Jobs with Deterministic Pagination

```bash
curl "http://127.0.0.1:8000/jobs?limit=10&offset=0&status=succeeded"
```

### 4. Cancel a Job

```bash
curl -X POST http://127.0.0.1:8000/jobs/<job-id>/cancel
```

*Queued jobs cancel immediately (`queued -> cancelled`). Running jobs cooperatively set `cancel_requested = TRUE` and abort at safe checkpoints with zero committed effects. Already-terminal jobs return `HTTP 200 OK` unchanged.*

---

## Running Tests

Configure `TEST_DATABASE_URL` (pointing to a dedicated PostgreSQL database; SQLite fallback is rejected by design) and run:

```powershell
.venv\Scripts\pytest.exe -v
```

Latest verified regression result: **`77 passed, 5 warnings in 5.08s`** against PostgreSQL:
- `tests/test_api_jobs.py` (14 passed): Idempotency header/body, conflict 409, status retrieval, deterministic pagination, cancellation states.
- `tests/test_chaos_harness.py` (28 passed): Decoupled dual-pool scheduler, absolute cadence, saturation independence, barrier synchronization, recovery accounting scenarios.
- `tests/test_database_models.py` (23 passed): Status constraints, attempt checks, reversible Alembic migrations, partial indexes, foreign keys.
- `tests/test_worker_engine.py` (12 passed): Concurrent atomic claims (`FOR UPDATE SKIP LOCKED`), lease renewals, dead worker recovery, zombie rejection, atomic side effect commits.

---

## Documentation Index

- [Design Note](docs/DESIGN.md) — Concise one-page architecture, database rationale, and delivery guarantee summary.
- [Detailed Architecture Specification](docs/ARCHITECTURE.md) — Complete SQL contracts, concurrency proofs, and failure policy.
- [Chaos Testing & Evidence Audit Report](docs/CHAOS_REPORT.md) — 100-job smoke, 500-job pilot, and 10,000-job / 8-worker `SIGKILL` chaos runs, silent-failure analysis, and first-person reflections.
- [Performance Baseline & 100 RPS Validation Report](docs/PERFORMANCE_REPORT.md) — Baseline, Optimization A/B results, and 25 minutes (`150,000` requests) of sustained 100 RPS validation.
- [AI Usage Record](docs/AI_USAGE.md) — Transparent stage-by-stage AI usage log, failed attempts, reliability assessment, and 26-item requirement coverage matrix.

*(Raw evidence files: Structured benchmark and chaos results are preserved as tracked `.json` files, with paired human-readable `.log` console outputs, under [artifacts/chaos/](artifacts/chaos/) and [artifacts/perf/](artifacts/perf/).)*

---

## Performance Summary & Production Verdict

In the tested Docker Compose configuration (`4` Uvicorn workers, `8` queue workers, `postgres:16-alpine`, simulated ~30 ms jobs):
- **15-min Sustained Mixed (100 RPS):** `99.97 RPS` (`90,000` measured requests), `p50=15.38 ms`, `p95=33.82 ms`, `p99=47.82 ms`, `0` errors, `pg_max_conns=28/100`, `0` container restarts.
- **5-min Unique `POST /jobs` (100 RPS):** `99.98 RPS` (`30,000` requests), `p50=13.82 ms`, `p95=24.21 ms`, `p99=31.71 ms`, `0` errors.
- **5-min Read-Heavy (100 RPS):** `99.97 RPS` (`30,000` requests), `p50=23.44 ms`, `p95=46.12 ms`, `p99=67.45 ms`, `0` errors.

**Production Verdict:** **`SHIP at 100 RPS`** for the tested workload and configuration. It is not an unverified claim of universal readiness for arbitrary unmeasured workloads.

---

## What Surprised Us and What Still Causes Concern

What surprised us most during chaos testing was how easily harness-level orchestration details can obscure actual fault injection: although configured for a `2.0s` `SIGKILL` interval, the original chaos loop executed kills only every `~9.1s` because synchronous container restart polling blocked the scheduler, subjecting the system to less frequent crash trauma than planned. Similarly, with ~30 ms job execution times, 1,000 concurrent cancellation requests yielded only 14 in-flight cancellations because jobs finished before cancellations arrived, requiring a dedicated synchronized barrier cohort to genuinely exercise active cooperative aborts.

What still causes concern for production is behavior outside our single-node containerized environment: unmitigated network partitions between workers and PostgreSQL, query tail latency when the `jobs` table reaches millions of rows (due to `SELECT count(jobs.id)` on paginated listing), and external side effects (S3 uploads, webhooks, GPU processes) that sit outside PostgreSQL ACID boundary and cannot be rolled back atomically on mid-flight worker termination.

---

## Known Limitations & Untested Scenarios

- **External Non-DB Side Effects:** Atomicity and exactly-once effects are strictly scoped to the PostgreSQL completion transaction (`execution_effects` with `UNIQUE(job_id)`). External operations require external idempotency mechanisms.
- **Clock & Network Assumptions:** All leases rely on single-node PostgreSQL `clock_timestamp()`. Multi-node clock skew and network partitions (`tc netem`) were not experimentally injected.
- **Pagination Scaling:** `GET /jobs` executes an exact count query on every request, which will degrade with table growth.
- **Docker Environment Availability:** In this final submission preparation pass, the local Docker Desktop daemon was offline (`pipe/dockerDesktopLinuxEngine` not found). Live container re-runs of the decoupled dual-pool chaos harness remain pending daemon startup, while all 28 harness unit tests and 77 total tests pass deterministically.

---

## Submission Checklist

| Requirement | Implementation / Evidence | Verdict |
| :--- | :--- | :---: |
| Idempotent job creation | `POST /jobs`, `request_hash`, `UNIQUE(idempotency_key)` | **PASS** |
| Job status inspection | `GET /jobs/{id}` | **PASS** |
| Paginated job listing | `GET /jobs` (`limit`/`offset`, deterministic order) | **PASS** |
| Cooperative cancellation | `POST /jobs/{id}/cancel`, checkpoints, terminal 200 | **PASS** |
| States & transitions | `queued -> running -> succeeded / failed / cancelled` | **PASS** |
| PostgreSQL rationale | PostgreSQL chosen; SQLite rejected (concurrency / multi-worker) | **PASS** |
| $\ge 4$ independent workers | `docker-compose.yml` (`replicas: 4` default, scalable to 8) | **PASS** |
| Measurable side effect | `execution_effects` table (`UNIQUE(job_id)`), atomic commit | **PASS** |
| `docker compose up` | Documented startup procedure in README | **PASS** |
| One-page design note | `docs/DESIGN.md` | **PASS** |
| 10,000-job chaos test | `artifacts/chaos/run_20261112_100304_seed42_9b42c4.json` (Run C) | **PASS** |
| 8 workers under chaos | Observed in Run B and Run C artifacts | **PASS** |
| Random SIGKILLs | Tested in Runs B and C; decoupled cadence in `ChaosScheduler` | **PARTIAL (Cadence ~9.1s in Run C; decoupled harness unit-verified)** |
| Concurrent duplicates | `1,500` duplicates tested in Run C, 0 mismatches | **PASS** |
| Parallel cancellation | `1,000` concurrent requests in Run C; active cohort synchronized | **PASS** |
| Invariant checks | 7 invariants evaluated and verified | **PASS** |
| Raw outputs preserved | 66 artifact files (`.json` + `.log`) committed in `artifacts/` | **PASS** |
| Silent failures explained | Double claim, zombie worker, races, clock skew in reports | **PASS** |
| 100 RPS verdict | Explicit **`SHIP at 100 RPS`** backed by 150k measured requests | **PASS** |
| AI tools disclosure | Detailed in `docs/AI_USAGE.md` | **PASS** |
