# AI Usage Record

> **Status:** Sustained 100 RPS evidence reconciled (`AGY-013`). All `AGY-012` job counts (`40,813` mixed = `50 seed + 4,813 warm-up + 35,950 measured`; `31,000` unique = `1,000 warm-up + 30,000 measured`; `50` read-heavy seed) and telemetry windows (`W1`–`W16`) are verified against PostgreSQL and raw artifacts. Verdict: **`SHIP at 100 RPS`**.

This document records AI assistance used in `soup-training-job-queue`, clearly distinguishing **completed** activities from **planned** activities.

---

## 1. Tools Used

| Tool | Role | Status |
| :--- | :--- | :--- |
| **ChatGPT** | Initial assignment analysis, architecture discussion, repository structure planning, and Python environment setup guidance | Completed (initial setup & planning) |
| **Antigravity** | Engineering documentation (`AGY-000`–`AGY-004`), database (`AGY-005`), REST API (`AGY-006`), worker engine (`AGY-007`), Docker Compose + chaos (`AGY-008`), audit (`AGY-009`), 100 RPS baseline & opt (`AGY-010`–`AGY-011`), and sustained stability (`AGY-012`) | `150,000` sustained 100 RPS requests verified (`SHIP at 100 RPS`) |
| **Developer (Manual)** | PowerShell execution, Python 3.12 / `uv` setup, `.env` credential entry, Architecture Reviews #1–#3, and starting Docker Desktop + executing Runs A–C | Ongoing |

---

## 2. Initial Project Setup (Completed)

- **Scope:** Configured the local Python 3.12 environment and dependency manifest prior to design documentation and implementation.
- **Division of Work:**
  - ChatGPT assisted with planning the repository structure and configuring the Python development environment.
  - The developer manually executed PowerShell commands and verified Python 3.12, `uv`, and installed dependencies.
- **Observed Repository Artifacts from Setup:**
  - `pyproject.toml` (`requires-python = ">=3.12"`, runtime dependencies `alembic`, `fastapi`, `psycopg[binary]`, `pydantic-settings`, `sqlalchemy`, `uvicorn[standard]`, and dev dependencies `httpx`, `pytest`)
  - `uv.lock`
  - `.venv/`
  - `.gitignore`
  - `main.py` (starter PyCharm template script; no application code implemented)

---

## 3. Architecture Planning & Human Reviews (Completed)

- **Initial Conceptual Planning:** ChatGPT was used to analyze assignment requirements and discuss core architectural mechanisms (`SELECT FOR UPDATE SKIP LOCKED`, leases, fencing tokens, cancellation, and transactional side effects).
- **Initial Documentation (`AGY-000`):** Antigravity inspected the repository and drafted `docs/DESIGN.md` and `docs/AI_USAGE.md`, surfacing unresolved design decisions for human review.
- **Human Review #1 & Approved Decisions (`2026-11-11`):**
  The developer reviewed `docs/DESIGN.md` and `docs/AI_USAGE.md`, resolved open design decisions, and identified critical concurrency corrections incorporated in `AGY-001`:
  1. **Lease Expiration:** Requeue expired jobs while `attempt < max_attempts`; transition to `failed` once `max_attempts` is reached; use PostgreSQL `clock_timestamp()` as the authoritative clock.
  2. **Idempotency:** Same `idempotency_key` + same deterministic payload hash returns existing job; same key + different payload hash returns `HTTP 409 Conflict`.
  3. **Cancellation:** `queued` jobs transition directly to `cancelled`; `running` jobs set `cancel_requested = TRUE` for cooperative checkpoint cancellation; terminal jobs return their current representation unchanged; recovery checks `cancel_requested` **before** retry policy and transitions expired `cancel_requested = TRUE` jobs directly to `cancelled`.
  4. **Side Effects:** Dedicated `execution_effects` table with `UNIQUE(job_id)`, storing `worker_id` and `fencing_token`, committed atomically in one transaction with the `succeeded` transition.
  5. **Database Access & Workers:** Synchronous `SQLAlchemy 2.x` with `Psycopg 3`, independent worker processes, fixed-interval PostgreSQL polling, and offset/limit pagination.
  6. **Critical Lease-Validity Correction:** Require `lease_expires_at > clock_timestamp()` in addition to `status = 'running'` and `fencing_token` checks during heartbeat renewal, successful completion, failure transition, and cancellation completion, plus documenting the 9-step completion transaction and why `now()` must not be used for lease checks.
- **Human Review #2 & Final SQL/Guarantee Corrections (`2026-11-11`):**
  During review of the `AGY-001` revision, the developer identified SQL syntax ordering errors, incomplete conditional `UPDATE` examples, and overstated execution guarantee wording, resulting in `AGY-002`:
  1. **PostgreSQL `SELECT` Clause Ordering:** Corrected `FOR UPDATE SKIP LOCKED` placement to follow `ORDER BY ... LIMIT ...` in both job claiming and dead-worker recovery queries.
  2. **Explicit Conditional `UPDATE` & Rollback Checks:** Added the full conditional `UPDATE` statements with `id`, `status = 'running'`, `worker_id`, `fencing_token`, `lease_expires_at > clock_timestamp()`, and `cancel_requested` (`FALSE` for completion, `TRUE` for cancellation), requiring `rowcount == 1` or full transaction rollback (including `execution_effects` insertion).
  3. **Bounded Retry Semantics:** Replaced unconditional at-least-once language with precise bounded retry semantics (`max_attempts`), noting duplicate attempts are possible, success is not guaranteed, committed effects are scoped to the PostgreSQL completion transaction, and `UNIQUE(job_id)` enforces at-most-one committed effect per job.
- **Pre-Implementation Finalization & Document Split (`2026-11-11`, `AGY-003`):**
  Following Human Reviews #1 and #2, the developer requested a pre-implementation documentation pass to condense `docs/DESIGN.md` into a one-page design note, move detailed SQL/concurrency/risk sections into `docs/ARCHITECTURE.md`, document API running-job cancellation SQL and zero-row re-read handling, define the failure policy and `running -> failed` conditional `UPDATE`, and finalize `HTTP 200 OK` idempotent terminal cancellation responses.
- **Architecture Review #3 & Failure/Cancellation Precedence Fix (`2026-11-11`, `AGY-004`):**
  During Architecture Review #3 of `docs/ARCHITECTURE.md`, the reviewer caught an inconsistency where the retryable-exhausted `running -> failed` SQL omitted `AND cancel_requested = FALSE` and non-retryable precedence over concurrent cancellation was not explicitly stated:
  1. **Retryable Failure Precedence:** Clarified strict order: `cancel_requested = TRUE -> cancelled`, else `attempt < max_attempts -> queued`, else `failed`—requiring `AND cancel_requested = FALSE` on both retryable `UPDATE` queries (`running -> queued` and exhausted `running -> failed`).
  2. **Zero-Row Handling for Retryable Failures:** Documented rollback + state re-read (`cancelled` if `cancel_requested = TRUE` on valid lease, or abort without modifying the job if ownership/lease was lost or the job is already terminal).
  3. **Non-Retryable Failure Precedence:** Explicitly documented that non-retryable execution failures take precedence over a concurrent `cancel_requested = TRUE` while holding a valid lease and `fencing_token` (transitioning `running -> failed`, never overwriting an already-terminal state).

---

## 4. AI Interaction Log

### Completed Interactions

#### `CHATGPT-001` — Requirement Analysis, Architecture Discussion & Environment Setup
- **Date:** Prior to `2026-11-11` (exact timestamp not archived)
- **Task ID:** `CHATGPT-001`
- **Objective:** Analyze assignment requirements, discuss architecture, plan repository layout, and configure the local Python 3.12 + `uv` environment.
- **Prompt:** *(Original prompt text not archived in repository or conversation context; not reconstructed.)*
- **AI Output:** Guidance on assignment decomposition, PostgreSQL queue concurrency concepts, repository layout, and Python dependencies.
- **Decision (Accepted / Modified / Rejected):** Accepted stack and setup guidance.
- **Human Review:** Developer reviewed suggestions and executed environment setup manually.
- **Verification:** Developer manually executed PowerShell commands to verify Python 3.12, `uv`, and installed packages.
- **Evidence:** `pyproject.toml`, `uv.lock`, `.venv/` present in repository.

#### `AGY-000` — Initial Engineering Documentation Authoring
- **Date:** `2026-11-11`
- **Task ID:** `AGY-000`
- **Objective:** Inspect the repository structure and create initial `docs/DESIGN.md` and `docs/AI_USAGE.md` without modifying application or configuration files.
- **Prompt:**
```text
# Task: Create Project Engineering Documentation

You are working on the Soup Backend Engineer take-home assignment.

Repository: soup-training-job-queue

Your current task is to create two engineering documents:

1. `docs/DESIGN.md`
2. `docs/AI_USAGE.md`

IMPORTANT: This task is documentation only. Do not implement application code yet.

## 1. Project Context

We are building a training-job queue service with multiple independent workers.

Technology stack:
- Python 3.12
- FastAPI
- SQLAlchemy 2.x
- PostgreSQL
- Psycopg 3
- Alembic
- Docker Compose
- Pytest

The service must support:
- Creating jobs with idempotency keys.
- Retrieving job status.
- Listing jobs with pagination.
- Cancelling jobs.
- At least 4 workers.
- Measurable execution side effects.

Required job states:

`queued → running → succeeded / failed / cancelled`

## 2. DESIGN.md

Create a concise engineering design document covering:

### Architecture Overview
Describe the API, PostgreSQL, worker processes and their responsibilities.

### Job Lifecycle
Explain all states and permitted transitions.

### Atomic Job Claiming
Describe how PostgreSQL transactions and `SELECT FOR UPDATE SKIP LOCKED` can prevent multiple workers from claiming the same job.

### Worker Lease
Explain lease expiration, heartbeat and how dead workers are detected.

### Dead Worker Recovery
Describe what happens when a worker disappears while processing a job.

### Fencing Tokens
Explain how monotonically increasing fencing tokens can prevent stale workers from committing results after lease expiration.

### Cancellation Semantics
Distinguish cancellation before execution from cancellation while a job is running. Define how cancellation and completion races will be resolved.

### Execution Guarantees
Explain the distinction between:
- At-least-once execution attempts.
- Exactly-once committed database side effects.

Explain the transactional boundary and limitations of the guarantee.

### Known Limitations
Document assumptions and risks that still require testing.

Use precise technical terminology. Do not claim that any mechanism has already been implemented or tested.

Mark proposed decisions as design decisions pending implementation and verification.

## 3. AI_USAGE.md

Create an honest AI usage record.

Known facts:
- ChatGPT was used to analyze the assignment requirements, discuss architecture, plan the repository structure, and configure the Python development environment.
- The developer manually executed the PowerShell commands and verified Python 3.12, uv, and installed dependencies.
- Antigravity has been selected as the AI coding agent, but its implementation work has not yet started.

Include:
- Tools Used.
- Initial Project Setup.
- Architecture Planning.
- AI Interaction Log.
- Accepted Suggestions.
- Rejected or Modified Suggestions.
- Human Verification.
- Verification Evidence.

Do not invent prompts, test results, AI mistakes, or implementation work.

Clearly distinguish completed activities from planned activities.

Use a repeatable entry template for future Antigravity interactions:
- Date
- Task ID
- Objective
- Prompt
- AI Output
- Decision (Accepted / Modified / Rejected)
- Human Review
- Verification
- Evidence

## 4. Engineering Rules

- Documentation must be accurate and consistent with the assignment.
- Do not invent benchmark results.
- Do not claim production readiness.
- Do not claim tests passed.
- Do not silently introduce architectural decisions without explaining them.
- Keep the documents concise and professional.
- Prefer clear engineering explanations over generic AI-generated prose.

## 5. Before Finishing

1. Inspect the current repository structure.
2. Create only the two requested Markdown files.
3. Do not modify `pyproject.toml`, `main.py`, dependencies or other application files.
4. Summarize the created files and their contents.
5. Identify any unresolved design decisions that require human approval.
```
- **AI Output:** Created initial `docs/DESIGN.md` and `docs/AI_USAGE.md` and listed six unresolved design decisions for human review.
- **Decision (Accepted / Modified / Rejected):** **Modified** after Human Review #1.
- **Human Review:** Reviewer approved core structure and resolved the open design choices, while identifying a critical gap in the initial fencing/lease checks (fencing token alone left a pre-recovery expired-lease commit window), requiring `cancel_requested` evaluation before recovery retry logic, and mandating strict `clock_timestamp()` usage over `now()`.
- **Verification:** Directory inspection and `git status -s` confirmed only `docs/DESIGN.md` and `docs/AI_USAGE.md` were created; no application code or tests exist.
- **Evidence:** Initial `docs/DESIGN.md` and `docs/AI_USAGE.md` files and `git status -s` output (`?? docs/`).

#### `AGY-001` — Post-Review Architecture Documentation Update
- **Date:** `2026-11-11`
- **Task ID:** `AGY-001`
- **Objective:** Update `docs/DESIGN.md` and `docs/AI_USAGE.md` to reflect all approved architectural decisions, the `lease_expires_at > clock_timestamp()` correction, the 9-step completion transaction, `clock_timestamp()` vs. `now()` rationale, and Human Review #1 records.
- **Prompt:** Human Review #1 revision request (`# Task: Finalize Architecture Documentation After Human Review` — supplying approved decisions for lease expiration, idempotency, cancellation, `execution_effects`, synchronous DB access, lease-validity correction, 9-step completion transaction, `clock_timestamp()` requirement, and `AI_USAGE.md` updates).
- **AI Output:** Updated `docs/DESIGN.md` and `docs/AI_USAGE.md`.
- **Decision (Accepted / Modified / Rejected):** **Modified** after Human Review #2.
- **Human Review:** Reviewer caught `FOR UPDATE SKIP LOCKED` placed before `LIMIT` in PostgreSQL `SELECT` examples, requested explicit conditional `UPDATE` SQL and `rowcount == 1` rollback checks in Sections 7 and 8, and required replacing unconditional at-least-once phrasing with bounded retry and `UNIQUE(job_id)` at-most-one effect semantics.
- **Verification:** `git status -s` inspection confirming no application code or dependency modifications; no tests run.
- **Evidence:** Revised `docs/DESIGN.md` and `docs/AI_USAGE.md`.

#### `AGY-002` — Final SQL Clause Ordering and Execution Guarantee Corrections
- **Date:** `2026-11-11`
- **Task ID:** `AGY-002`
- **Objective:** Correct PostgreSQL `LIMIT ... FOR UPDATE SKIP LOCKED` clause ordering, strengthen conditional `UPDATE` and rollback SQL in Sections 7 and 8 of `docs/DESIGN.md`, clarify bounded retry and scoped database side-effect guarantees, and record Human Review #2 in `docs/AI_USAGE.md`.
- **Prompt:** Human Review #2 correction request (`# Task: Apply Final SQL and Execution Guarantee Corrections` — directing SQL clause ordering fixes, full conditional `UPDATE` predicates with `rowcount == 1` rollback checks in Sections 7 and 8, bounded-retry/at-most-one-effect clarification, and `AI_USAGE.md` updates).
- **AI Output:** Updated `docs/DESIGN.md` and `docs/AI_USAGE.md`.
- **Decision (Accepted / Modified / Rejected):** **Modified** in `AGY-003` (restructured into a one-page `DESIGN.md` plus `ARCHITECTURE.md` with finalized API cancellation, failure policy, and terminal HTTP status).
- **Human Review:** Reviewer approved the SQL ordering and completion/cancellation predicates, and requested splitting the long design doc into a one-page `DESIGN.md` + detailed `ARCHITECTURE.md` while adding API running-job cancellation SQL, explicit failure policy + `running -> failed` SQL, and a finalized terminal cancellation HTTP status.
- **Verification:** Inspected all SQL examples and `git status -s`; confirmed no application code or dependencies were modified and no tests were executed.
- **Evidence:** Revised `docs/DESIGN.md` and `docs/AI_USAGE.md`.

#### `AGY-003` — Documentation Split, API Cancellation Semantics, Failure Policy & Terminal HTTP Response
- **Date:** `2026-11-11`
- **Task ID:** `AGY-003`
- **Objective:** Split documentation into a one-page design note (`docs/DESIGN.md`) and a detailed technical specification (`docs/ARCHITECTURE.md`), document API-side running-job cancellation SQL and zero-row re-read handling, define retryable/non-retryable/lease-expiration failure policy with `running -> failed` conditional `UPDATE` SQL, finalize `HTTP 200 OK` for terminal job cancellations, and log `AGY-003` in `docs/AI_USAGE.md`. (Documentation only; no code, dependencies, migrations, or tests.)
- **Prompt:** Pre-implementation finalization prompt (`# Task: Finalize Design Documentation Before Implementation` — directing API cancellation SQL, failure policy, `running -> failed` SQL, terminal cancellation HTTP response finalization, splitting `DESIGN.md` and `ARCHITECTURE.md`, and updating `AI_USAGE.md`).
- **AI Output:** Condensed `docs/DESIGN.md` into a one-page design note, created `docs/ARCHITECTURE.md` with detailed SQL/concurrency/failure-policy/risk documentation, and updated `docs/AI_USAGE.md`.
- **Decision (Accepted / Modified / Rejected):** **Modified** after Architecture Review #3 (`AGY-004`).
- **Human Review:** Architecture Review #3 approved the split and API cancellation semantics, and identified one remaining inconsistency in `docs/ARCHITECTURE.md`: the `running -> failed` SQL did not guard retryable-exhausted failures against `cancel_requested = TRUE`, and non-retryable failure precedence over concurrent cancellation plus retryable zero-row handling needed explicit specification.
- **Verification:** Documentation-only inspection and `git status -s` check confirming no code, dependencies, migrations, tests, commits, or pushes occurred.
- **Evidence:** `docs/DESIGN.md`, `docs/ARCHITECTURE.md`, `docs/AI_USAGE.md`, and `git status -s`.

#### `AGY-004` — Failure vs. Cancellation Precedence & Retryable Zero-Row Handling
- **Date:** `2026-11-11`
- **Task ID:** `AGY-004`
- **Objective:** Resolve Architecture Review #3 inconsistency across `docs/DESIGN.md` and `docs/ARCHITECTURE.md` by clarifying retryable vs. non-retryable failure precedence against `cancel_requested`, separating retryable (`AND cancel_requested = FALSE`) and non-retryable `running -> failed` SQL, documenting zero-row rollback and re-read handling for retryable transitions, and recording Review #3 in `docs/AI_USAGE.md`.
- **Prompt:** Architecture Review #3 resolution request (`# Task: Resolve Final Failure and Cancellation Semantics` — documentation only).
- **AI Output:** Updated `docs/DESIGN.md`, `docs/ARCHITECTURE.md`, and `docs/AI_USAGE.md`.
- **Decision (Accepted / Modified / Rejected):** Pending final human approval.
- **Human Review:** Pending.
- **Verification:** Documentation-only inspection and `git status -s`; no application code, dependencies, migrations, tests, commits, or pushes were executed.
- **Evidence:** Updated `docs/DESIGN.md`, `docs/ARCHITECTURE.md`, `docs/AI_USAGE.md`, and `git status -s`.

#### `AGY-005` — Database Foundation Implementation (Models, Config, Alembic, Tests)
- **Date:** `2026-11-12`
- **Task ID:** `AGY-005`
- **Objective:** Implement the database foundation (`Job` and `ExecutionEffect` SQLAlchemy 2.x models, constraints and PostgreSQL indexes, environment-backed DB configuration, Alembic setup with initial reversible migration, and PostgreSQL integration tests) without implementing API endpoints, workers, or Docker Compose.
- **Prompt:** Database foundation implementation task (`# Task: Implement Database Foundation` — `app/` SQLAlchemy models/config/session, Alembic initial migration, PostgreSQL tests, and `AI_USAGE.md` update).
- **AI Output:**
  - Created `app/__init__.py`, `app/core/__init__.py`, `app/core/config.py` (environment-backed `Settings`, `postgresql+psycopg://` normalization, SQLite rejection, no hardcoded credentials).
  - Created `app/db/__init__.py`, `app/db/base.py`, `app/db/models.py` (`JobStatus`, `Job`, `ExecutionEffect`, `finished_at` hybrid property reconciling `completed_at`/`cancelled_at`, check/unique/FK constraints, partial polling & lease recovery indexes), and `app/db/session.py` (synchronous SQLAlchemy 2.x engine/session factory).
  - Created `.env.example`, `alembic.ini`, `alembic/env.py`, `alembic/script.py.mako`, and `alembic/versions/0001_initial_schema.py` (reversible initial migration).
  - Created `tests/__init__.py`, `tests/conftest.py`, and `tests/test_database_models.py` (23 test cases requiring PostgreSQL via `TEST_DATABASE_URL` or `DATABASE_URL`; fails explicitly rather than falling back to SQLite).
- **Decision (Accepted / Modified / Rejected):**
  - Reconciled `Job` terminal timestamps (`completed_at`, `cancelled_at` from `ARCHITECTURE.md`) with the prompt's `finished_at` by keeping `completed_at` and `cancelled_at` as database columns and exposing `finished_at` as a SQLAlchemy `@hybrid_property` (`coalesce(completed_at, cancelled_at)`).
  - Added `updated_at` column on `jobs` per task requirements; kept `cancel_requested_at` and `last_error` per `ARCHITECTURE.md`.
  - Used `ondelete="RESTRICT"` on `execution_effects.job_id` FK to prevent deleting jobs that have committed side effects.
  - Caught and fixed a double-prefix constraint-naming issue in `0001_initial_schema.py` (`ck_jobs_ck_jobs_*` -> `ck_jobs_*`) discovered via `alembic upgrade head --sql`.
  - Pending human review.
- **Human Review:** Pending.
- **Verification:**
  - `.venv\Scripts\alembic.exe upgrade head --sql` -> exited `0`; verified exact PostgreSQL DDL and constraint names.
  - `.venv\Scripts\alembic.exe downgrade 0001_initial_schema:base --sql` -> exited `0`; verified reversible `DROP TABLE` / `DROP INDEX` DDL.
  - `.venv\Scripts\python.exe -c "..."` (`Base.metadata.create_all` via `create_mock_engine('postgresql+psycopg://')`) -> exited `0`; confirmed 1:1 DDL parity with `0001_initial_schema.py`.
  - `.venv\Scripts\pytest.exe --collect-only -q` -> exited `0`; 23 tests collected.
  - `.venv\Scripts\pytest.exe -v` -> exited `1`; 1 passed (`test_config_rejects_sqlite_and_missing_database_url`), 22 errored at `postgres_url` fixture setup (`Failed: TEST_DATABASE_URL or DATABASE_URL must be set to a PostgreSQL database; SQLite fallback is disallowed.`).
- **Evidence:** Offline DDL output, mock-engine DDL output, and `pytest -v` output (`1 passed, 22 errors in 0.27s`).

#### `AGY-006` — REST API Implementation
- **Date:** `2026-11-12`
- **Task ID:** `AGY-006`
- **Objective:** Implement the FastAPI REST API (`POST /jobs`, `GET /jobs/{job_id}`, `GET /jobs`, `POST /jobs/{job_id}/cancel`) using the existing PostgreSQL + SQLAlchemy 2.x foundation, separating routes, schemas, business logic, and repository operations, and verify with PostgreSQL integration tests (without implementing workers or Docker Compose).
- **Prompt:** REST API implementation task (`AGY-006 — REST API Implementation`).
- **AI Output:**
  - Created `app/schemas/__init__.py` and `app/schemas/jobs.py` (`JobCreateRequest`, `ExecutionEffectResponse`, `JobResponse`, `JobListResponse`).
  - Created `app/repositories/__init__.py` and `app/repositories/job_repository.py` (`JobRepository` with `get_by_id`, `get_by_idempotency_key`, `create`, `list_jobs`, `cancel_queued_job`, `request_running_job_cancellation`).
  - Created `app/services/__init__.py` and `app/services/job_service.py` (`compute_request_hash`, `JobService` with concurrent-safe idempotency handling via `uq_jobs_idempotency_key` + `IntegrityError` rollback/re-read, and bounded-retry cancellation race handling).
  - Created `app/api/__init__.py`, `app/api/dependencies.py`, `app/api/routes/__init__.py`, `app/api/routes/jobs.py`, and `app/main.py`.
  - Updated `tests/conftest.py` (added `client` fixture with per-request PostgreSQL sessions) and created `tests/test_api_jobs.py` (13 API integration tests including `ThreadPoolExecutor` concurrent idempotency tests).
- **Decision (Accepted / Modified / Rejected):**
  - Accepted both `Idempotency-Key` header and `idempotency_key` JSON body field (requiring at least one, rejecting blank/mismatched values with `422`), returning `201 Created` on initial creation, `200 OK` on idempotent replay, and `409 Conflict` on payload/parameter mismatch.
  - Kept database schema and Alembic migrations unchanged; updated `updated_at = clock_timestamp()` in raw SQL cancellation updates.
  - Pending human review.
- **Human Review:** Pending.
- **Verification:**
  - `.venv\Scripts\pytest.exe -v` -> exited `0`; **`36 passed, 5 warnings in 2.40s`** against live PostgreSQL (`23` database foundation tests + `13` REST API integration tests).
- **Evidence:** `pytest -v` output (`36 passed, 5 warnings in 2.40s`).

#### `AGY-007` — Worker Engine, Leases, Fencing and Recovery
- **Date:** `2026-11-12`
- **Task ID:** `AGY-007`
- **Objective:** Implement independent PostgreSQL-backed worker processes (`>= 4` runnable simultaneously), background lease heartbeats with a dedicated DB session, monotonic fencing and lease-validity enforcement, deterministic `ExecutionEffect` atomic commits, cooperative cancellation checkpoints, retryable/non-retryable failure handling, and dead-worker lease recovery, verified with PostgreSQL integration tests.
- **Prompt:** Worker engine implementation task (`# AGY-007 — Worker Engine, Leases, Fencing and Recovery`).
- **AI Output:**
  - Updated `app/core/config.py` with worker polling, lease TTL, heartbeat interval, and recovery settings.
  - Created `app/repositories/worker_repository.py` (`ClaimedJob`, `RecoveryStats`, `WorkerRepository` with `claim_next_job`, `renew_heartbeat`, `complete_job_atomically`, `complete_cooperative_cancellation`, `handle_retryable_failure`, `fail_job_non_retryable`, and `recover_expired_jobs`).
  - Created `app/worker/__init__.py`, `app/worker/heartbeat.py` (`LeaseHeartbeat` daemon thread with dedicated PostgreSQL session), `app/worker/executor.py` (`SimulatedJobExecutor` and failure/cancellation exceptions), `app/worker/runner.py` (`WorkerRunner`, `generate_worker_id`), and `app/worker/__main__.py` (`python -m app.worker` CLI entrypoint).
  - Created `tests/test_worker_engine.py` (12 PostgreSQL integration tests covering multi-worker claiming, no double claim, fencing token increments, blocking-execution heartbeat, expired lease rejection, zombie worker rejection, atomic `ExecutionEffect` + completion, no duplicate effects, cooperative cancellation, retryable failure precedence, non-retryable failure precedence, dead-worker recovery, and recovery vs. completion race serialization).
- **Decision (Accepted / Modified / Rejected):**
  - Used `make_interval(secs => :lease_ttl)` with `clock_timestamp()` in PostgreSQL queries for authoritative lease math.
  - Kept `DESIGN.md` and `ARCHITECTURE.md` unchanged (implementation followed the approved design without deviation).
  - Pending human review.
- **Human Review:** Pending.
- **Verification:**
  - `.venv\Scripts\python.exe -m app.worker --help` -> exited `0`.
  - `.venv\Scripts\pytest.exe -v` -> exited `0`; **`48 passed, 5 warnings in 2.81s`** against live PostgreSQL (`13` API tests + `23` DB/Alembic tests + `12` worker/lease/fencing/recovery tests).
- **Evidence:** `python -m app.worker --help` output and `pytest -v` output (`48 passed, 5 warnings in 2.81s`).

#### `AGY-008` — Docker Compose, Multi-Process Execution and Chaos Harness
- **Date:** `2026-11-12`
- **Task ID:** `AGY-008`
- **Objective:** Implement Docker Compose orchestration (`postgres`, `migrate`, `api`, `>= 4` independent `worker` containers), a reproducible `SIGKILL` chaos harness with direct PostgreSQL invariant checks (`artifacts/chaos/`), and silent-failure documentation (`docs/CHAOS_REPORT.md`), then run smoke, chaos, and `pytest` verification.
- **Prompt:** Docker Compose and chaos harness task (`# AGY-008 — Docker Compose, Multi-Process Execution and Chaos Harness`).
- **AI Output:**
  - Created `.dockerignore`, `Dockerfile` (Python 3.12 + `uv sync --frozen --no-dev`), and `docker-compose.yml` (`postgres:16-alpine` with `pg_data` volume & healthcheck, one-shot `migrate`, `api` with healthcheck, `worker` with `deploy.replicas: 4` and scalable via `--scale worker=8`, using `COMPOSE_POSTGRES_PORT=5433` on host to avoid colliding with local PostgreSQL on `5432`).
  - Created `scripts/chaos_harness.py` (configurable jobs/workers/seed/kill-interval, concurrent duplicate idempotency requests, `409` conflict probes, concurrent cancellations, `SIGKILL` + restart loop, PostgreSQL invariant verifier, unique non-overwriting `.log` and `.json` outputs in `artifacts/chaos/`).
  - Created `docs/CHAOS_REPORT.md` documenting methodology, silent-failure verification matrix, and harness limitations.
- **Decision (Accepted / Modified / Rejected):**
  - Left `docs/DESIGN.md` and `docs/ARCHITECTURE.md` unchanged (no design deviation required).
  - Pending human review and live Docker execution.
- **Human Review:** Pending.
- **Verification:**
  - `docker compose config --quiet` -> exited `0` (valid Compose configuration).
  - `.venv\Scripts\python.exe scripts/chaos_harness.py --help` -> exited `0`.
  - `docker info --format '{{.ServerVersion}}'` -> exited `1` (`//./pipe/dockerDesktopLinuxEngine` not found; Docker Desktop daemon not running, blocking live `docker compose up` and chaos runs).
  - `.venv\Scripts\pytest.exe -v` -> exited `0`; **`48 passed, 5 warnings in 3.84s`** against local PostgreSQL.
- **Evidence:** `docker compose config --quiet`, `scripts/chaos_harness.py --help`, `docker info` error output, and `pytest -v` (`48 passed, 5 warnings in 3.84s`).

#### `AGY-009` — Evidence Documentation, AI Usage Audit & Take-Home Compliance
- **Date:** `2026-11-12`
- **Task ID:** `AGY-009`
- **Objective:** Audit all implementation files, `pytest` results, and `artifacts/chaos/` outputs (`run_20261112_095909_seed42_55d8b3`, `run_20261112_100147_seed42_442bd4`, `run_20261112_100304_seed42_9b42c4`), update `docs/CHAOS_REPORT.md` and `docs/AI_USAGE.md` with evidence-backed findings, add an AI reliability assessment, and produce the 26-item take-home requirement coverage matrix.
- **Prompt:** Evidence documentation, AI usage audit, and take-home compliance task (`# AGY-009`).
- **AI Output:** Rewrote `docs/CHAOS_REPORT.md` (14 required sections, 6-invariant table, `DON'T SHIP at 100 RPS yet` verdict) and updated `docs/AI_USAGE.md` with stage audit table, preserved failure history, AI reliability assessment, and 26-item coverage matrix.
- **Decision (Accepted / Modified / Rejected):** Pending human review.
- **Human Review:** Pending.
- **Verification:** Inspected all `.json` and `.log` artifacts in `artifacts/chaos/`, ran `git diff --check`, `git status -s`, and `.venv\Scripts\pytest.exe -v`.
- **Evidence:** `artifacts/chaos/*.json`, `artifacts/chaos/*.log`, `docs/CHAOS_REPORT.md`, `git diff --check`, and `pytest -v`.

#### `AGY-010` — Performance Baseline & 100 RPS Validation
- **Date:** `2026-11-12`
- **Task ID:** `AGY-010`
- **Objective:** Audit the unmodified architecture and query plans, define a reproducible 25/50/100 RPS load testing methodology, run baseline benchmarks across all 5 API operations and a mixed workload against Docker Compose, identify the first bottleneck from measured telemetry, and document findings in `docs/PERFORMANCE_REPORT.md` (stopping before Phase 6 optimization).
- **Prompt:** Performance baseline & 100 RPS validation task (`# AGY-010`).
- **AI Output:**
  - Created `scripts/load_test.py` (rate-paced `ThreadPoolExecutor` + `httpx` client, warm-up + measured phases, `pg_stat_activity` sampler, `docker stats` snapshot, non-destructive `perf_<run_id>:` key scoping, immutable `.json` and `.log` outputs in `artifacts/perf/`).
  - Executed `19` baseline runs (`10 RPS` mixed + `25/50/100 RPS` across `post_unique`, `post_duplicate`, `get_job`, `get_list`, `post_cancel`, and `mixed`) without modifying application code.
  - Created `docs/PERFORMANCE_REPORT.md` documenting query plans, all `19` baseline runs, bottleneck isolation (`api-1` single-process Uvicorn + redundant `JobService.create_job()` post-commit `SELECT`), proposed Phase 6 optimizations (not yet applied), and verdict **`DON'T SHIP at 100 RPS`**.
- **Decision (Accepted / Modified / Rejected):**
  - Proposed Phase 6 optimizations (removing redundant post-commit `SELECT` in `JobService.create_job()`, scaling Uvicorn `--workers 4`) documented for human review; **no optimization code applied yet**.
- **Human Review:** Pending.
- **Verification:**
  - Inspected `EXPLAIN` plans on `10,000`-row `jobs` table and `SHOW max_connections` (`100`).
  - Executed `19` runs via `scripts/load_test.py` against live Docker Compose (`artifacts/perf/`).
- **Evidence:** `artifacts/perf/*.json`, `artifacts/perf/*.log`, `docs/PERFORMANCE_REPORT.md`.

#### `AGY-011` — Targeted Performance Optimization (Optimization A & B)
- **Date:** `2026-11-12`
- **Task ID:** `AGY-011`
- **Objective:** Remove the redundant post-commit `SELECT` in `JobService.create_job()` (Optimization A) and scale Uvicorn to `4` worker processes with bounded DB pools (Optimization B), benchmarking each stage independently and verifying zero correctness regressions.
- **Prompt:** Targeted performance optimization task (`# AGY-011`).
- **AI Output:**
  - Updated `app/services/job_service.py` (Optimization A: replaced `get_by_id` after `commit()` with `set_committed_value(job, "execution_effect", None)`) and added `test_create_job_unique_triggers_no_post_commit_select_or_lazy_load` in `tests/test_api_jobs.py`.
  - Updated `docker-compose.yml` (Optimization B: `--workers 4` on `api`, `API` pool `5+5=10` per worker = `40` max, `Worker` pool `3+2=5` per container = `40` max; `80 < 100 max_connections`).
  - Executed `4` Opt A runs and `4` Opt A+B runs (`artifacts/perf/`), preserving all `19` baseline artifacts.
  - Updated `docs/PERFORMANCE_REPORT.md` and `docs/AI_USAGE.md`.
- **Decision (Accepted / Modified / Rejected):** Pending final review.
- **Human Review:** Pending.
- **Verification:**
  - `.venv\Scripts\pytest.exe -v` -> **`49 passed, 5 warnings in 2.75s`**.
  - Opt A (`100 RPS` `post_unique`): `98.89 RPS`, `p50=10.36 ms`, `p99=175.70 ms`.
  - Opt A+B (`100 RPS` `post_unique`): **`99.97 RPS`**, `p50=10.69 ms`, **`p99=21.36 ms`**; `mixed` (`100 RPS`): **`99.98 RPS`**, `p50=11.16 ms`, **`p99=23.35 ms`**.
- **Evidence:** `artifacts/perf/*.json`, `artifacts/perf/*.log`, `docs/PERFORMANCE_REPORT.md`, `pytest -v`.

#### `AGY-012` — Sustained Load and Stability Validation
- **Date:** `2026-11-12`
- **Task ID:** `AGY-012`
- **Objective:** Validate sustained 100 RPS stability without modifying application code or DB config: `15 min` mixed (`+2 min` warm-up), `5 min` unique `POST /jobs`, and `5 min` read-heavy (`GET /jobs/{id}` + `GET /jobs`), with windowed telemetry and post-run correctness checks.
- **Prompt:** Sustained load and stability validation task (`# AGY-012`).
- **AI Output:**
  - Extended `scripts/load_test.py` (test harness only) with `--scenario read_heavy`, per-endpoint latency/status breakdown, 60s `timeseries` window sampling, `unique_sub` vs `persisted_unique_measured` verification, and container `RestartCount` tracking.
  - Executed `3` sustained 100 RPS runs (`150,000` measured requests total) and updated `docs/PERFORMANCE_REPORT.md` and `docs/AI_USAGE.md`.
- **Decision (Accepted / Modified / Rejected):** Pending final review.
- **Human Review:** Pending.
- **Verification:**
  - `15 min` mixed (`perf_mixed_100rps_20261112_103730_1d7d1d`): `99.97 RPS`, `p50=15.38 ms`, `p95=33.82 ms`, `p99=47.82 ms`, `90,000` `2xx`, `0` errors, `35,950/35,950` unique persisted, `40,813` succeeded, `0` dup effects, `0` inconsistent, `pg_max_conns=28/100`, `0` restarts.
  - `5 min` unique POST (`perf_post_unique_100rps_20261112_105450_db4a2b`): `99.98 RPS`, `p50=13.82 ms`, `p95=24.21 ms`, `p99=31.71 ms`, `30,000/30,000` persisted, `31,000` succeeded, `0` errors, `0` restarts.
  - `5 min` read-heavy (`perf_read_heavy_100rps_20261112_110017_9b041c`): `99.97 RPS`, `p50=23.44 ms`, `p95=46.12 ms`, `p99=67.45 ms`, `30,000` `2xx`, `0` errors, `0` restarts.
- **Evidence:** `artifacts/perf/perf_*_100rps_20261112_1037*.{json,log}`, `1054*.{json,log}`, `1100*.{json,log}`, `docs/PERFORMANCE_REPORT.md`.

#### `AGY-013` — Sustained Load Evidence Reconciliation
- **Date:** `2026-11-12`
- **Task ID:** `AGY-013`
- **Objective:** Independently reconcile all `AGY-012` job counts, warm-up activity, database state counts, and telemetry windows against raw `.json`/`.log` artifacts and live PostgreSQL without modifying application code or database state.
- **Prompt:** Sustained load evidence reconciliation task (`# AGY-013`).
- **AI Output:**
  - Inspected all three `AGY-012` `.log` and `.json` files and queried PostgreSQL by `idempotency_key` prefix (`seed:`, `wu:`, `u:`).
  - Reconciled Mixed (`50 seed + 4,813 warm-up + 35,950 measured = 40,813`), Unique POST (`1,000 warm-up + 30,000 measured = 31,000`), Read-Heavy (`50 seed`), and pre-AGY-012 rows (`20,312`, total DB `92,175`).
  - Corrected Mixed telemetry window count in `docs/PERFORMANCE_REPORT.md` from "15 windows (`W3`–`W16`)" to `16` recorded windows (`W1` warm-up, `W2` straddling boundary, `W3`–`W16` = `14` full measured windows, plus ~27s unwindowed tail) and noted that Mixed `post_cancel` targeted terminal `seed_job_ids`.
- **Decision (Accepted / Modified / Rejected):** Pending human review.
- **Human Review:** Pending.
- **Verification:** Read-only PostgreSQL reconciliation query, raw `.json`/`.log` inspection, `git diff --check`, and `git status -s`.
- **Evidence:** `artifacts/perf/*.json`, `artifacts/perf/*.log`, `docs/PERFORMANCE_REPORT.md`, `git diff --check`.

---

### Stage-by-Stage Audit Summary (`AGY-005` to `AGY-013`)

| Stage | AI Tool | AI Contribution | Accepted Suggestions | Human Verification | Changes Made | Rejected Suggestions | Evidence | Outcome |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **AGY-005** (Database Foundation) | Antigravity | `Job`, `ExecutionEffect`, `Base`, `session.py`, `0001_initial_schema.py`, `test_database_models.py` | Synchronous SQLAlchemy 2.x + Psycopg 3, partial indexes, `UNIQUE(job_id)` | Developer configured `.env` PostgreSQL credentials via `getpass`; verified `pytest` | Fixed double-prefixed constraint names (`ck_jobs_ck_jobs_*`) caught by `alembic --sql` | None | `alembic upgrade head --sql`, `23` passed in `pytest` | **Verified** |
| **AGY-006** (REST API) | Antigravity | `app/schemas/`, `app/repositories/job_repository.py`, `app/services/job_service.py`, `app/api/`, `test_api_jobs.py` | `Idempotency-Key` header/body support, SHA-256 `request_hash`, `409` on mismatch, `200` on terminal cancel | Executed full `pytest` suite against PostgreSQL | None | None | `36` passed in `pytest -v` | **Verified** |
| **AGY-007** (Worker Engine) | Antigravity | `worker_repository.py`, `LeaseHeartbeat`, `SimulatedJobExecutor`, `WorkerRunner`, `test_worker_engine.py` | Dedicated-thread/session heartbeat, `clock_timestamp()` + `fencing_token`, recovery precedence | Executed `python -m app.worker --help` and full `pytest` suite | Fixed `test_recovery_versus_completion_race_serializes_cleanly` after `SKIP LOCKED` skipped locked row | Initial barrier-only race test assertion (`requeued == 1`) rejected/replaced | `48` passed in `pytest -v` | **Verified** |
| **AGY-008** (Docker Compose & Chaos Harness) | Antigravity | `Dockerfile`, `docker-compose.yml`, `scripts/chaos_harness.py`, initial `docs/CHAOS_REPORT.md` | 4-replica default worker service scalable to 8, host port `5433`, immutable `.log`/`.json` artifacts | Developer started Docker Desktop and executed Runs A, B, and C | None to code; `AGY-009` audited `~9.1s` effective kill cadence | None | `run_20261112_095909_seed42_55d8b3`, `run_20261112_100147_seed42_442bd4`, `run_20261112_100304_seed42_9b42c4` | **Partially Verified** (kill cadence `~9.1s` vs `2s`) |
| **AGY-009** (Evidence & Compliance Audit) | Antigravity | Audited `artifacts/chaos/`, updated `docs/CHAOS_REPORT.md` and `docs/AI_USAGE.md`, built 26-item matrix | `DON'T SHIP at 100 RPS yet` provisional verdict; explicit distinction of tested vs assumed behavior | Pending | Documented `_chaos_loop` cadence limitation and audit findings | None | `artifacts/chaos/*.json`, `artifacts/chaos/*.log`, `git diff --check`, `pytest -v` | **Verified** |
| **AGY-010** (Performance Baseline) | Antigravity | Audited `EXPLAIN` plans & pools, created `scripts/load_test.py`, ran `19` baseline tests, created `docs/PERFORMANCE_REPORT.md` | Isolated single-process API write bottleneck + redundant `JobService.create_job()` `SELECT` | Pending | None to app code (stopped before Phase 6 optimization per instructions) | None | `artifacts/perf/*.json`, `artifacts/perf/*.log`, `docs/PERFORMANCE_REPORT.md` | **Verified** (`DON'T SHIP at 100 RPS`) |
| **AGY-011** (Targeted Optimization) | Antigravity | Opt A (`set_committed_value` in `JobService.create_job`), Opt B (`--workers 4` + `80`-conn pool budget), `8` benchmark runs, `49` tests | Both optimizations verified one variable at a time; `0` correctness regressions | Pending | Added `test_create_job_unique_triggers_no_post_commit_select_or_lazy_load` | None | `artifacts/perf/*.json`, `pytest -v` (`49 passed`) | **Verified** (`SHIP at 100 RPS`) |
| **AGY-012** (Sustained Stability) | Antigravity | Extended `scripts/load_test.py` telemetry; ran `15m` mixed, `5m` unique POST, `5m` read-heavy (`150k` requests at `100 RPS`) | Flat latency, `pg_max_conns=28/100`, `0` restarts, `0` errors, `0` invariant violations | Pending | Window count wording corrected in `AGY-013` | None | `perf_mixed_100rps_*1d7d1d`, `perf_post_unique_100rps_*db4a2b`, `perf_read_heavy_100rps_*9b041c` | **Verified** (`SHIP at 100 RPS`) |
| **AGY-013** (Evidence Reconciliation) | Antigravity | Reconciled `seed:`/`wu:`/`u:` counts in PostgreSQL and corrected `W1`–`W16` window accounting in `PERFORMANCE_REPORT.md` | Exact integer reconciliation (`40,813`, `31,000`, `50`, `20,312` pre-existing) | Pending | No app/DB code modified | None | Read-only SQL query, `git diff --check` | **Verified** |

---

### Preserved History of Failed Attempts & Blockers

1. **`AGY-005` Initial Alembic Constraint Naming Bug:** `alembic upgrade head --sql` exposed double-prefixed constraint names (`ck_jobs_ck_jobs_valid_status`, `ck_execution_effects_ck_execution_effects_*`) because `Base.metadata.naming_convention` prefixed already-prefixed constraint names in `0001_initial_schema.py`. Corrected before live migration; verified with `alembic upgrade head --sql` and `Base.metadata.create_all` mock DDL.
2. **`AGY-005` Initial `pytest` Environment Failure (`1 passed, 22 errors`):** Before `.env` credentials were configured, `pytest -v` exited `1` because `postgres_url` explicitly refused SQLite fallback (`Failed: TEST_DATABASE_URL or DATABASE_URL must be set...`). Resolved when developer configured `.env` via local `getpass`.
3. **`AGY-007` Initial Race Test Failure (`47 passed, 1 failed`):** `test_recovery_versus_completion_race_serializes_cleanly` failed (`assert 0 == 1`) because `complete_job_atomically` acquired `FOR UPDATE` first and `recover_expired_jobs` used `SKIP LOCKED`, legitimately skipping the locked row. Rewritten to test deterministic lock-contention serialization (`48 passed`).
4. **`AGY-008` Initial Docker Daemon Blocker:** Docker Desktop daemon was not running during `AGY-008` (`//./pipe/dockerDesktopLinuxEngine` not found), blocking live Compose runs until the developer started Docker Desktop and executed Runs A–C.

---

### Planned Activities (Post-Submission / Future Hardening)

- All core take-home stages (`AGY-000`–`AGY-013`) and final audit/documentation stages (`AGY-014`–`AGY-017`) are complete, including the sustained 100 RPS load tests (`AGY-012`) and evidence reconciliation (`AGY-013`).
- Optional future hardening (out of scope for take-home submission): experimental multi-node clock-skew injection, network partition (`tc netem`) chaos, PostgreSQL failover testing, and a decoupled strict `2.0s` `SIGKILL` cadence run.

---

### Repeatable Entry Template for Future Antigravity Interactions

Use this template for every future implementation, migration, or testing task:

```markdown
#### `<TASK_ID>` — <Short Title>
- **Date:** YYYY-MM-DD
- **Task ID:** <TASK_ID>
- **Objective:** <What this task aims to build, fix, or verify>
- **Prompt:** <Exact prompt or reference to the prompt given to Antigravity>
- **AI Output:** <Summary of files created/modified or analysis produced>
- **Decision (Accepted / Modified / Rejected):** <Accepted | Modified | Rejected — include rationale if modified or rejected>
- **Human Review:** <Notes from developer code/design inspection>
- **Verification:** <Commands or tests manually executed by developer, e.g., pytest, alembic upgrade head, docker compose up>
- **Evidence:** <Observed output, commit hash, test summary, or file paths — record actual results only>
```

---

## 5. Accepted Suggestions

- **Environment & Stack (Completed Setup):**
  - Python `>= 3.12` managed with `uv` (`pyproject.toml`, `uv.lock`).
  - Core stack: `fastapi`, synchronous `sqlalchemy` (2.x), `psycopg[binary]` (Psycopg 3), `alembic`, `pydantic-settings`, `uvicorn[standard]`, `httpx`, and `pytest`.
- **Architecture Design (Approved in Human Review #1; Implemented in `AGY-005`–`AGY-008` & Optimized in `AGY-011`):**
  - PostgreSQL single-source-of-truth coordination using `SELECT ... FOR UPDATE SKIP LOCKED`.
  - Fixed-interval polling across `>= 4` independent worker processes and offset/limit pagination.
  - Idempotent job creation via deterministic payload hash with `HTTP 409 Conflict` on key/payload mismatch.
  - Cooperative running-job cancellation via `cancel_requested = TRUE`, direct cancellation for `queued` jobs, and `HTTP 200 OK` idempotent current-state response on terminal jobs.
  - Explicit failure policy distinguishing retryable failures, non-retryable failures, and lease expirations against `max_attempts`, with fenced + lease-validated `running -> failed` SQL.
  - Dedicated `execution_effects` table with `UNIQUE(job_id)`, `worker_id`, and `fencing_token` committed atomically with `succeeded`.
  - Documentation split between a one-page `docs/DESIGN.md` note and detailed `docs/ARCHITECTURE.md` specification.

---

## 6. Rejected or Modified Suggestions

- **Modified during Human Review #1 (`AGY-000` -> `AGY-001`):**
  1. **Fencing Check Alone Was Insufficient:** Initial `docs/DESIGN.md` checked `status = 'running' AND fencing_token = :held_token` on completion/transitions without requiring `lease_expires_at > clock_timestamp()`, which left a window for an expired worker to commit before recovery incremented `fencing_token`. **Modified** to require `lease_expires_at > clock_timestamp()` on heartbeat renewals, successful completion, failure transitions, and cancellation completion.
  2. **Recovery Ordering with Cancellation:** Initial draft did not mandate checking `cancel_requested` before retry policy during dead-worker recovery. **Modified** so recovery checks `cancel_requested = TRUE` first and transitions directly to `cancelled` rather than requeuing.
  3. **Authoritative Clock Precision:** Initial draft referenced `clock_timestamp() / now()` interchangeably in prose. **Modified** to mandate `clock_timestamp()` exclusively and explicitly prohibit `now()` for lease validity checks.
  4. **Open Options Resolved:** Replaced open design options (sync vs. async SQLAlchemy, retry vs. immediate fail on expiry, `409` vs. `422` on idempotency conflict, polling vs. `LISTEN/NOTIFY`, cursor vs. offset pagination, concrete side-effect table) with explicit human-approved decisions.

- **Modified during Human Review #2 (`AGY-001` -> `AGY-002`):**
  1. **PostgreSQL `SELECT` Clause Ordering:** `AGY-001` placed `FOR UPDATE SKIP LOCKED` before `LIMIT` in the job claiming (`Section 3`) and recovery (`Section 5`) SQL examples, which is invalid PostgreSQL syntax. **Modified** to `ORDER BY ... LIMIT ... FOR UPDATE SKIP LOCKED;`.
  2. **Conditional `UPDATE` & Rollback Enforcement in SQL Examples:** `AGY-001` omitted the full conditional `UPDATE` predicates in `Section 7.B` (updated only `WHERE id = :job_id` after `SELECT ... FOR UPDATE`) and described `Section 8`'s completion `UPDATE` only in prose steps. **Modified** to show explicit conditional `UPDATE` statements enforcing `id`, `status = 'running'`, `worker_id`, `fencing_token`, `lease_expires_at > clock_timestamp()`, and `cancel_requested` (`FALSE` for completion, `TRUE` for cancellation), requiring `rowcount == 1` or full transaction rollback.
  3. **Bounded Retry vs. Unconditional At-Least-Once Wording:** `AGY-001` stated unconditionally that execution attempts are at-least-once. **Modified** to document bounded retry semantics (`max_attempts`), noting duplicate attempts are possible, success is not guaranteed, committed database side effects are scoped to the PostgreSQL completion transaction, and `UNIQUE(job_id)` guarantees at-most-one committed effect per job.

- **Modified during Architecture Review #3 (`AGY-003` -> `AGY-004`):**
  1. **Retryable Failure SQL Missing `cancel_requested = FALSE` Guard:** `AGY-003` shared one `running -> failed` SQL block without `AND cancel_requested = FALSE` for both non-retryable and retryable-exhausted failures, contradicting the retryable precedence rule (`cancel_requested = TRUE -> cancelled` first). **Modified** to separate retryable transition SQL (both `running -> queued` and `running -> failed` enforcing `AND cancel_requested = FALSE`) from non-retryable `running -> failed` SQL, and to document zero-row rollback + state re-read handling.
  2. **Non-Retryable Failure Precedence Unstated:** Explicitly documented that non-retryable execution failures take precedence over a concurrent `cancel_requested = TRUE` while holding a valid lease and `fencing_token` (while never overwriting already-terminal states).

---

## 7. Human Verification

### Completed Human Verification
- Developer manually executed PowerShell commands to verify Python 3.12, `uv`, and dependency installation into `.venv/`.
- Developer completed Human Architecture Review #1 of `docs/DESIGN.md` and `docs/AI_USAGE.md`, resolving open architecture choices and correcting lease-validity and recovery-cancellation edge cases.
- Developer completed Human Review #2, catching PostgreSQL `LIMIT`/`FOR UPDATE SKIP LOCKED` clause ordering errors, requiring explicit conditional `UPDATE` SQL and rollback checks, and tightening bounded-retry / scoped-effect guarantees.
- Developer completed Architecture Review #3, identifying the retryable vs. cancellation precedence SQL inconsistency and non-retryable precedence ambiguity in `docs/ARCHITECTURE.md`.

### Pending Human Verification
- Final human review of `AGY-009`–`AGY-017` artifacts and documentation (`docs/CHAOS_REPORT.md`, `docs/PERFORMANCE_REPORT.md`, `docs/AI_USAGE.md`, `README.md`) prior to submission.

---

## 8. Verification Evidence

### Available Evidence (Observed in Repository & Command Runs)
- `pyproject.toml` and `uv.lock`: Unmodified; declare Python `>=3.12` and installed dependencies (`alembic`, `fastapi`, `psycopg[binary]`, `pydantic-settings`, `sqlalchemy`, `uvicorn[standard]`, `httpx`, `pytest`).
- `artifacts/chaos/run_20261112_095909_seed42_55d8b3.{json,log}`: 100-job / 4-worker smoke run (`PASS`, 4.697s).
- `artifacts/chaos/run_20261112_100147_seed42_442bd4.{json,log}`: 500-job / 8-worker chaos pilot (`PASS`, 10.573s, 1 `SIGKILL`, 1 restart).
- `artifacts/chaos/run_20261112_100304_seed42_9b42c4.{json,log}`: 10,000-job / 8-worker full chaos run (`PASS`, 121.739s, 9,986 succeeded, 14 cancelled, 13 `SIGKILL`s, 13 restarts, 1,500 duplicates, 20 `409` probes).
- `artifacts/perf/` & `docs/PERFORMANCE_REPORT.md`: `AGY-010` baseline (`19` runs), `AGY-011` Opt A+B (`8` runs), `AGY-012` sustained 100 RPS runs (`perf_mixed_100rps_20261112_103730_1d7d1d`, `perf_post_unique_100rps_20261112_105450_db4a2b`, `perf_read_heavy_100rps_20261112_110017_9b041c`; `150,000` measured requests, `0` errors, `0` invariant violations), and `AGY-013` PostgreSQL reconciliation.
- `.venv\Scripts\pytest.exe -v`: `49 passed, 5 warnings` against local PostgreSQL (post-`AGY-011` regression suite, up from `48 passed` in `AGY-007`).

### Not Yet Executed (No Evidence Available)
- Experimental container clock-skew injection, network partition (`tc netem`), or PostgreSQL failover tests.
- Decoupled strict `2.0s` `SIGKILL` cadence chaos re-run (Run C achieved `~9.1s` effective cadence due to synchronous container restart polling).

---

## 9. AI Reliability Assessment

- **Where AI-Generated Output Required Additional Verification:**
  - *Constraint naming in Alembic:* Static offline SQL (`alembic upgrade head --sql`) caught double-prefixed constraint names that looked plausible in Python code.
  - *Concurrency test assumptions:* `test_recovery_versus_completion_race_serializes_cleanly` initially assumed `SKIP LOCKED` would block and return `requeued == 1` instead of skipping a row locked by `complete_job_atomically`.
  - *Chaos kill cadence:* Inspecting Run C's raw `.json` timestamps revealed that synchronous container restart polling inside `_chaos_loop` stretched the effective `SIGKILL` cadence from `2.0s` to `~9.1s` (`13` kills in `121.7s`).
- **Assumptions Not Experimentally Verified:** PostgreSQL server `clock_timestamp()` prevents container clock skew by design, but multi-node clock drift, network partitions (`tc netem`), and PostgreSQL failover were not experimentally tested.
- **Key Engineering Takeaway:** AI-generated backend infrastructure accelerates boilerplate and SQL/test scaffolding, but requires strict human verification against raw execution artifacts, offline DDL output, and lock-ordering semantics rather than trusting summary claims.

---

## 10. Take-Home Requirement Coverage Matrix

| # | Requirement | Status | Concrete Evidence / Next Action |
| :---: | :--- | :--- | :--- |
| 1 | Job creation with idempotency key | **VERIFIED** | `app/api/routes/jobs.py`, `tests/test_api_jobs.py`, Run C (`10,000` jobs) |
| 2 | Job status retrieval | **VERIFIED** | `GET /jobs/{job_id}`, `test_get_job_and_unknown_job_404` |
| 3 | Paginated job listing | **VERIFIED** | `GET /jobs` (`limit`/`offset`), `test_list_jobs_pagination_deterministic_ordering_and_status_filter` |
| 4 | Job cancellation | **VERIFIED** | `POST /jobs/{job_id}/cancel`, `test_api_jobs.py`, `test_worker_engine.py`, Run C (`14` cancelled) |
| 5 | Required job states (`queued -> running -> succeeded / failed / cancelled`) | **VERIFIED** | `JobStatus`, `ck_jobs_valid_status`, `tests/test_database_models.py` |
| 6 | PostgreSQL or SQLite choice and rationale | **VERIFIED** | PostgreSQL chosen; SQLite rejected in `app/core/config.py` & `tests/conftest.py`; rationale in `DESIGN.md` |
| 7 | At least four independent workers | **VERIFIED** | `docker-compose.yml` (`replicas: 4`), Run A (`4` containers), Runs B/C (`8` containers) |
| 8 | Execution side effect | **VERIFIED** | `execution_effects` (`UNIQUE(job_id)`), `test_worker_engine.py`, Runs A–C |
| 9 | One-page design note | **VERIFIED** | `docs/DESIGN.md` |
| 10 | Worker claiming strategy | **VERIFIED** | `FOR UPDATE SKIP LOCKED` in `WorkerRepository.claim_next_job`, `test_worker_engine.py` |
| 11 | Dead worker detection | **VERIFIED** | `lease_expires_at <= clock_timestamp()` + `recover_expired_jobs`, `test_worker_engine.py` |
| 12 | Running cancellation semantics | **VERIFIED** | Cooperative `cancel_requested = TRUE` + checkpoints, `test_cooperative_cancellation_aborts_without_effect` |
| 13 | Delivery/effect guarantee and rationale | **VERIFIED** | `docs/DESIGN.md` §5, `docs/ARCHITECTURE.md` §7 (bounded retries + scoped ACID commit + `UNIQUE(job_id)`) |
| 14 | 10,000-job chaos harness | **VERIFIED** | `scripts/chaos_harness.py`, Run C (`run_20261112_100304_seed42_9b42c4`) |
| 15 | Eight workers | **VERIFIED** | Run B & Run C observed `8` distinct worker container IDs |
| 16 | Random SIGKILL approximately every two seconds | **PARTIALLY VERIFIED** | Run C ran `13` `SIGKILL`s with `--kill-interval 2.0`, but synchronous restart wait yielded `~9.1s` effective cadence; decouple kill timer from restart wait if strict 2s cadence is required |
| 17 | Concurrent duplicate POST requests | **VERIFIED** | `test_concurrent_idempotent_requests_same_payload`, Run C (`1,500` duplicates, `0` mismatches) |
| 18 | Concurrent cancellation requests | **VERIFIED** | Run C (`1,000` concurrent cancels -> `14` cancelled, `9,986` succeeded) |
| 19 | All required invariants | **VERIFIED** | All 6 invariants `PASS` in Runs A, B, and C `.json`/`.log` |
| 20 | Raw output and number of runs | **VERIFIED** | `3` complete runs preserved in `artifacts/chaos/` (`.json` + `.log`) |
| 21 | Failed-run preservation | **PARTIALLY VERIFIED** | Harness uses unique non-overwriting filenames even on failure (`finally` block), though no failed chaos run is in `artifacts/chaos/`; earlier failed `pytest` runs are recorded in `AI_USAGE.md` |
| 22 | Silent failure analysis | **VERIFIED** | `docs/CHAOS_REPORT.md` §9 |
| 23 | Production verdict at 100 RPS | **VERIFIED (`SHIP at 100 RPS`)** | `AGY-011` Opt A+B (`99.97 RPS` `post_unique`, `99.98 RPS` `mixed`), `AGY-012` sustained 100 RPS (`15m` mixed, `5m` unique POST, `5m` read-heavy; `0` errors, `0` invariant violations), and `AGY-013` reconciliation (`docs/PERFORMANCE_REPORT.md`) |
| 24 | What surprised us and what still concerns us | **VERIFIED** | `docs/CHAOS_REPORT.md` §§10–11, `docs/AI_USAGE.md` §9 |
| 25 | AI usage disclosure | **VERIFIED** | `docs/AI_USAGE.md` |
| 26 | `docker compose up` reproducibility | **VERIFIED** | `docker-compose.yml`, `Dockerfile`, Runs A–C |
