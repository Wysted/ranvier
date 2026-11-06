---
id: T13
title: Runs repository: state machine, claims, leases, cancel
part: 5
wave: E
depends_on: [T06]
unblocks: [T16, T21, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T13-runs-repository
status: done
---

## Goal
The `runs` repository over Postgres: transition rules, conditional claim, fenced lease heartbeat, cancellation flag, orphan requeue, and the shared error codes.

## Design references
- design.md §5 "Runs table", "Transitions" table, "Serialization, recovery and housekeeping", "Interrupts, cancellation, idempotency"
- design.md §6 "HTTP API" (error codes referenced: `thread_busy`, `thread_interrupted`, `flow_version_mismatch`, `too_many_runs`)
- design.md §14 open check "Lease expiry while the owner is alive"
- design.md §13 row "Run leases"

## Scope: files this task may create or edit
- packages/core/src/ranvier/_internal/runtime/runs.py
- packages/core/tests/unit/runtime/test_runs_transitions.py
- packages/core/tests/integration/runtime/test_runs.py
Shared files: none.

## Out of scope
- `execute_run` (T16)
- Periodic sweep scheduling and thread-idle queries beyond what is listed (T25 may append `idle_threads`)
- HTTP status mapping of error codes (T21)

## Acceptance criteria
- [ ] `RunStatus` enum, `Run` dataclass, `ErrorCode` enum (`run_timeout`, `flow_build_failed`, `flow_version_mismatch`, `thread_busy`, `thread_interrupted`, `too_many_runs`, `orphan_retries_exhausted`, `internal_error`, ...) and `RunError(code, message, retryable)` used by all later tasks
- [ ] Pure `can_transition(from, to)` encoding the design §5 table, unit tested exhaustively
- [ ] `RunsRepository`: `create` (pending), `get`, `list_for_thread` (newest first), `thread_pin` (flow, version, tenant of the first run), `claim(run_id, owner, lease_s)` returning claimed / lost / thread_busy (maps the unique-violation), `heartbeat(run_id, owner)` extending the lease and returning `cancel_requested`, `finish(run_id, owner, status, error)`, `request_cancel`, `requeue(run_id, owner)` (drain), `requeue_expired(max_attempts)` (to `pending`, or `error` with `orphan_retries_exhausted`), `delete_thread` (refused while running)
- [ ] Every mutation after the claim is fenced on `lease_owner` and `status='running'` and reports whether it applied, so an owner that lost its lease cannot write a terminal state
- [ ] Lease times come from the database clock (`now()`), not the app clock
- [ ] Integration tests: two concurrent claims -> one winner; second `running` row for a thread -> `thread_busy`; expired lease is reclaimable; cancel before claim; requeue_expired exhaustion
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Port ideas from `/home/wysted/code/agintis/src/agintis/modules/conversations/application/run_leases.py` and `domain/thread.py` (lease moves from a thread row to the run row).
- Raw SQL with psycopg `dict_row`; no ORM.
- No threads table (OD-3): thread identity is `thread_id` on runs.
- Conventions: follow the project's Python conventions and the decision log; never read `.env`; agintis is read-only reference.
