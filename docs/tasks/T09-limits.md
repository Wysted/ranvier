---
id: T09
title: Limits: admission, executors, process pool, loop lag
part: 3
wave: D
depends_on: [T04, S5]
unblocks: [T15, T27, T29, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T09-limits
status: todo
---

## Goal
The platform-owned global limits: run admission semaphore, bounded default thread executor, a process pool with timeout recycling, and the event-loop lag sampler.

## Design references
- design.md §8 table rows "Run admission", "Sync tools and sync nodes", "CPU-heavy tools", "Loop health"
- design.md §6 "HTTP API" admission paragraph (`429 too_many_runs`)
- design.md §14 open check on `ProcessPoolExecutor.kill_workers`

## Scope: files this task may create or edit
- packages/core/src/graph_agent/_internal/limits.py
- packages/core/tests/unit/test_limits.py
Shared files: none.

## Out of scope
- Per-tool and per-MCP semaphores and timeouts (T27, T29)
- Draining on SIGTERM (T26 appends a `stop_admission`)
- Publishing gauges to OTel (T33)
- Creating the objects in the container (T15)

## Acceptance criteria
- [ ] `Limits.admit()` async context manager: waits up to `admission_wait_s`, then raises a typed `TooManyRuns`; releases on exit and on cancellation
- [ ] `install_default_executor(loop, sync_threads)` sets a bounded `ThreadPoolExecutor` via `loop.set_default_executor`
- [ ] Process pool (forkserver context, `cpu_workers` workers) with `run(fn, *args, timeout_s)` and a `recycle()` that follows the S5 result (`kill_workers` on 3.14, the documented fallback on 3.13); a timed-out CPU task never leaves an orphan worker
- [ ] Loop-lag sampler: background task measuring sleep drift every 1 s, exposes current and max lag, logs a warning over `loop_lag_warn_ms`; start/stop methods compatible with an `AsyncExitStack`
- [ ] Unit tests: admission blocks/times out/releases; a blocking `time.sleep` on the loop is observed by the sampler; process timeout recycles the pool
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Read `docs/spikes/S5.md` first.
- Everything is created once in the composition root; no module-level singletons.
- Expose cheap read-only stats (active runs, waiters, executor occupancy) for T33 to publish; do not depend on OTel here.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
