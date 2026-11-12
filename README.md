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

## Prerequisites

- **Docker & Docker Compose** (for containerized startup and chaos/load runs)
- **Python 3.12+**, **`uv`**, and **PostgreSQL** (for running `pytest` or scripts locally against a PostgreSQL test database)

---

## Quick Start

Start the full stack (PostgreSQL, Alembic migration, FastAPI API, and 4 default worker containers):

```bash
docker compose up
```

Optional detached/rebuild variant:

```bash
docker compose up -d --build
```

To scale workers to 8 containers (as used in the chaos and sustained load tests):

```bash
docker compose up -d --scale worker=8
```

Verify service health:

```powershell
docker compose ps
Invoke-RestMethod http://127.0.0.1:8000/health
```

*(Note: Compose uses its own internal `postgres` service on host port `5433` and container port `5432`. Do not commit `.env`; see `.env.example` for local environment variable placeholders.)*

---

## Running Tests

Configure `TEST_DATABASE_URL` (pointing to a dedicated PostgreSQL test database; SQLite fallback is disallowed) and run:

```powershell
.venv\Scripts\pytest.exe -v
```

Latest verified regression result: **`49 passed, 5 warnings`** against PostgreSQL (`tests/test_database_models.py`, `tests/test_api_jobs.py`, `tests/test_worker_engine.py`).

---

## Documentation

- [Design Note](docs/DESIGN.md) — Concise one-page architecture and guarantee summary.
- [Detailed Architecture Specification](docs/ARCHITECTURE.md) — Full SQL contracts, serialization proofs, and failure policy.
- [Chaos Testing & Evidence Audit Report](docs/CHAOS_REPORT.md) — 100-job smoke, 500-job pilot, and 10,000-job / 8-worker `SIGKILL` chaos results and silent-failure analysis.
- [Performance Baseline & 100 RPS Validation Report](docs/PERFORMANCE_REPORT.md) — Baseline, Optimization A/B results, and 25 minutes (`150,000` requests) of sustained 100 RPS validation.
- [AI Usage Record](docs/AI_USAGE.md) — Transparent stage-by-stage AI usage log, failed attempts, reliability assessment, and requirement coverage matrix.

---

## Performance Summary (Tested Configuration Only)

In the tested Docker Compose configuration (`4` Uvicorn workers, `8` queue workers, `postgres:16-alpine`, simulated ~30 ms jobs):
- **15-min Sustained Mixed (100 RPS):** `99.97 RPS` (`90,000` measured requests), `p50=15.38 ms`, `p95=33.82 ms`, `p99=47.82 ms`, `0` errors, `pg_max_conns=28/100`, `0` container restarts.
- **5-min Unique `POST /jobs` (100 RPS):** `99.98 RPS` (`30,000` requests), `p50=13.82 ms`, `p95=24.21 ms`, `p99=31.71 ms`, `0` errors.
- **5-min Read-Heavy (100 RPS):** `99.97 RPS` (`30,000` requests), `p50=23.44 ms`, `p95=46.12 ms`, `p99=67.45 ms`, `0` errors.

This supports **`SHIP at 100 RPS`** for the tested workload and configuration; it is not a claim of universal production readiness across arbitrary workloads or cluster topologies.

---

## Known Limitations

- **External Side Effects:** Exactly-once committed effects apply only inside the PostgreSQL completion transaction (`execution_effects` with `UNIQUE(job_id)`). External side effects (files, webhooks, compute) are outside the ACID boundary.
- **Clock & Network Assumptions:** Lease expiration relies on PostgreSQL server `clock_timestamp()`; multi-node clock skew and asymmetric network partitions (`tc netem`) were not experimentally injected.
- **Pagination Scaling:** `GET /jobs` runs `SELECT count(jobs.id)` on every call (`p50=28.27 ms` at ~88k rows), which will grow linearly with table size.
- **Chaos Kill Cadence:** In chaos Run C, synchronous container restart polling yielded an effective `SIGKILL` cadence of `~9.1s` (`13` kills in `121.7s`) rather than strict `2.0s` wall-clock intervals.
