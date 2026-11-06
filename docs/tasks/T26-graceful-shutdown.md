---
id: T26
title: Graceful shutdown: drain, requeue, shutdown timeouts
part: 8
wave: I
depends_on: [T23, T24, S1]
unblocks: [T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T26-graceful-shutdown
status: done
---

## Goal
On SIGTERM stop admission, drain running runs with `RunControl.request_drain()`, release their leases for re-claim (never `pending`), defer them, and align server shutdown timeouts.

## Design references
- design.md §8 row "Graceful shutdown"
- design.md §5 "Transitions" row `running` (lease expired) -> `running` (new owner)
- design.md §14 open check on `RunControl`

## Scope: files this task may create or edit
- packages/core/tests/integration/test_shutdown.py
Shared files (append-only, keep edits minimal to avoid merge conflicts):
- packages/core/src/ranvier/_internal/runtime/runner.py (T16, append: registry of active runs + `drain_all(timeout)`)
- packages/core/src/ranvier/_internal/limits.py (T09, append `stop_admission()`)
- packages/core/src/ranvier/_internal/api/app.py (T23, lifespan shutdown hook)
- packages/core/src/ranvier/_internal/worker.py (shared with T25: keep to your own functions)
- packages/core/src/ranvier/_internal/cli.py (shared with T25: timeouts passed to uvicorn and the worker)

## Out of scope
- Sweep and recovery of crashed owners (T25)
- Changing run semantics or the transition table

## Acceptance criteria
- [ ] SIGTERM (or lifespan/worker shutdown): `stop_admission()` first; new inline runs get 503/429-style refusal with the standard error body
- [ ] Every active run gets `RunControl.request_drain()` (import and usage exactly as S1 documents); when `GraphDrained` arrives the runner calls `runs.release_for_drain` (lease expired now, attempt given back, token rotated; status stays `running`) and defers the job again; runs exceeding `limits.shutdown_grace_s` are left to the lease-expiry sweep
- [ ] uvicorn `timeout_graceful_shutdown` and Procrastinate `shutdown_graceful_timeout` are set from `limits.shutdown_grace_s`; a warning is logged at startup if it is smaller than the longest flow `timeout_s` or node timeout
- [ ] Integration test: start a slow multi-step flow, trigger shutdown, assert the run's lease is released (expired, attempt given back, token rotated), resumable from its checkpoint, and not executed twice
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- If S1 shows `RunControl` is unusable in the installed langgraph, stop and report; do not invent a different mechanism.
- Depends on T23/T24, so the API and worker entry points exist; touch them minimally.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
- From the T10 review (orchestrator): `Observability.shutdown()` blocks (up to ~2x10 s with the collector down): call it via `asyncio.to_thread` and count it inside `limits.shutdown_grace_s`.
- From the T13 review (orchestrator decision, design §5): drain = repository call that sets `lease_expires_at = now()` and `attempt - 1` for this process's runs, then defer; never write `pending`.
- From the final T16 review (orchestrator): N1 bound `await container.active_runs.settled()` with `asyncio.timeout_at(budget.deadline)` before closing pools (a terminal step is not bounded and an SSE consumer stuck at `yield` holds it); N2 with draining set, a disconnect cancels as `CancelledError` (hand-over) when the generator waits on `queue.get` but as `GeneratorExit` (CANCELLED) when it waits at `yield` — document it in the shutdown notes; N3 stop claiming (worker stops fetching, API stops admission) BEFORE `settled()`, since an in-flight `runs.claim` is outside ActiveRuns.
- From the T24 review (orchestrator): `run_worker` currently keeps Procrastinate's defaults `install_signal_handlers=True` and `shutdown_graceful_timeout=None`: on SIGTERM it waits forever for running jobs without `request_drain`. T26 passes `install_signal_handlers=False`, owns SIGTERM, calls `ActiveRuns.request_drain()`, and bounds the worker stop with the `ShutdownBudget`; await `ActiveRuns.settled()` (bounded by the deadline) before the queue app and pools close.
- From the final T23 review (orchestrator): DELETE /threads has a documented race (a run starting between `has_running` and `adelete_thread` loses its checkpoints); close it with a per-thread lock only if live-thread deletes become a real use case. `serve` logs 'leaked semaphore objects' from the CpuPool (forkserver) on SIGTERM: fix it in the shutdown path.
- From the final T25 review (orchestrator, include in this task; worker.py scope approved): log a warning when `delete_idle_thread` deletes 0 rows (the shortcut window became visible); in `fire_cron_job` defer also when `create_once` returned created=False (a retried attempt after a failed defer; repeat defers are harmless) and test it; in prune, stop the per-thread loop on `psycopg.OperationalError`/`PoolTimeout` (database down) instead of one warning per thread; add a test of the conditional DELETE itself (override `thread_is_idle` to insert a run and return True: rows survive, checkpoints do not). Ctrl+C / SIGINT on `ranvier worker` and `serve` makes ProcessPoolExecutor children print KeyboardInterrupt tracebacks and leak semaphores (forkserver): fix in the shutdown path. The next SQL migration must be 0003.
