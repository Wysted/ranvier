---
id: T24
title: Procrastinate app and execute_run job
part: 8
wave: H
depends_on: [T16, S4]
unblocks: [T25, T26, T34, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T24-queue-worker
status: done
---

## Goal
The Procrastinate app factory, deferral by task name, and the worker task that runs `execute_run` with `lock=thread_id`.

## Design references
- design.md §5 "Serialization, recovery and housekeeping" table (rows per-thread order and exclusion)
- design.md §2 layout rows `queue.py`, `worker.py` and contract C4
- design.md OD-1 row
- design.md §8 rows "Run admission" (worker concurrency) and "Postgres connections"

## Scope: files this task may create or edit
- packages/core/src/ranvier/_internal/queue.py
- packages/core/src/ranvier/_internal/worker.py
- packages/core/tests/integration/test_queue_worker.py
Shared files (append-only, keep edits minimal to avoid merge conflicts):
- packages/core/src/ranvier/_internal/bootstrap.py (append only: build the Procrastinate app, set `container.queue` as a `RunQueue`; T33 appends in the same wave)

## Out of scope
- Sweep, prune, cron tasks and the `worker`/`crons` commands (T25)
- SIGTERM handling (T26)
- Schema creation in `migrate` (T25)

## Acceptance criteria
- [ ] `queue.py`: `make_app(settings)` (own pool sized by `database.procrastinate_pool_max`), task-name constants, `ProcrastinateRunQueue.defer(run_id, thread_id)` deferring BY NAME with `lock=thread_id` (no import of worker/runtime)
- [ ] `worker.py`: task `execute_run(run_id)` that runs `execute_run` from the container and drains the events; claim lost -> exits quietly; `thread_busy` -> retried with backoff; worker concurrency = `limits.max_active_runs`
- [ ] Integration test: two jobs on the same thread run one after the other; a job whose run was cancelled before claim no-ops; a normal run reaches `success`
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Read `docs/spikes/S4.md` first (periodic API, stalled jobs, version-specific API).
- Contract C4: `procrastinate` imported only in `queue` and `worker`.
- Worker processes build their own container; do not share objects with the API process.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
- From the T13 review (orchestrator decision, design §5): defer `execute_run` with `lock=thread_id` and `queueing_lock=run_id`, ignoring `AlreadyEnqueued`.
- From S4 (orchestrator): defer `execute_run` by name with `lock=thread_id`, `queueing_lock=run_id`, catch `AlreadyEnqueued`; never set `priority=` (FIFO per lock only with uniform priority); leave headroom in the `thread_busy` retry `max_attempts` (stalled requeues spend the same counter; `max_attempts=N` = N+1 executions) and keep its backoff short (a job waiting for retry blocks later jobs of the same thread). Never read the Procrastinate job status as the run status (a job can be `failed` while its run is `success`); W1's own `finish_job` may raise 'not in doing or todo' after a sweep closed its job: expected, decide whether to filter or document that log. Worker LISTEN connection is outside the pool (+1 in `log_connection_budget`).
