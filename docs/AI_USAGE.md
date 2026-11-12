# AI Usage Record

> **Status:** Architecture Reviews #1–#3 applied across `docs/DESIGN.md`, `docs/ARCHITECTURE.md`, and `docs/AI_USAGE.md` (documentation only; awaiting final human approval). Application code implementation, database migrations, and automated tests have **not** started.

This document records AI assistance used in `soup-training-job-queue`, clearly distinguishing **completed** activities from **planned** activities.

---

## 1. Tools Used

| Tool | Role | Status |
| :--- | :--- | :--- |
| **ChatGPT** | Initial assignment analysis, architecture discussion, repository structure planning, and Python environment setup guidance | Completed (initial setup & planning) |
| **Antigravity** | Engineering documentation authoring (`AGY-000`) and post-review specification updates (`AGY-001`–`AGY-004`); selected as AI coding agent for upcoming implementation | Documentation phase active (awaiting final human approval); **application code implementation not yet started** |
| **Developer (Manual)** | PowerShell execution, Python 3.12 / `uv` / dependency verification, Architecture Reviews #1–#3, and final decision approval | Ongoing |

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

---

### Planned Activities (Not Yet Started)

Application implementation with Antigravity has **not** started. Planned activities include:
- Final human approval of `docs/DESIGN.md`, `docs/ARCHITECTURE.md`, and `docs/AI_USAGE.md`.
- Designing and generating database models and initial `Alembic` migrations.
- Implementing FastAPI endpoints (`POST /jobs`, `GET /jobs/{job_id}`, `GET /jobs`, `POST /jobs/{job_id}/cancel`).
- Implementing independent worker processes (`>= 4` workers), fixed-interval polling, background/checkpointed heartbeats, cooperative cancellation, recovery sweeps, and `execution_effects` atomic completion.
- Setting up `Docker Compose` for API, PostgreSQL, and `>= 4` workers.
- Writing and running `pytest` integration, race-condition, and crash-recovery tests.

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
- **Architecture Design (Approved in Human Review #1; Pending Implementation):**
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
- Final human approval of `docs/DESIGN.md`, `docs/ARCHITECTURE.md`, and `docs/AI_USAGE.md`.
- Code review of all future application modules, Alembic migrations, and Docker Compose configuration.
- Independent execution of `alembic` migrations, `>= 4` worker runs, and `pytest` tests.

---

## 8. Verification Evidence

### Available Evidence (Observed in Repository)
- `pyproject.toml` and `uv.lock`: Confirm declared Python `>=3.12` and installed package lockfile for `fastapi`, `sqlalchemy`, `psycopg[binary]`, `alembic`, `pydantic-settings`, `uvicorn[standard]`, `httpx`, and `pytest`.
- `.venv/`: Present locally from developer setup.
- `main.py` and `.gitignore`: Starter scaffolding only; no queue application code exists.
- `docs/DESIGN.md`, `docs/ARCHITECTURE.md`, and `docs/AI_USAGE.md`: Documentation files only, reflecting all pre-implementation review decisions (`AGY-000` through `AGY-004`).

### Not Yet Executed (No Evidence Available)
- No `pytest` test suite exists or has been executed.
- No `alembic` migrations have been generated or applied.
- No PostgreSQL database, FastAPI server, Docker Compose stack, or worker processes have been started or tested.
