---
id: T25
title: Worker housekeeping: sweep, prune, crons, worker/crons CLI
part: 8
wave: I
depends_on: [T24]
unblocks: [T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T25-worker-housekeeping
status: todo
---

## Goal
Periodic tasks for stalled jobs, orphan runs, checkpoint pruning and manifest crons, plus the `worker` and `crons` commands and the Procrastinate step of `migrate`.

## Design references
- design.md §5 "Serialization, recovery and housekeeping" table (rows Stalled jobs, Orphan runs, Checkpoint growth)
- design.md §4 manifest `[[flows.<f>.crons]]` (UTC, each firing starts a new thread)
- design.md §6 "CLI" rows `worker`, `crons`, `migrate`
- design.md §14 open check on periodic tasks and schema upgrades

## Scope: files this task may create or edit
- packages/core/tests/integration/test_housekeeping.py
- packages/core/tests/unit/test_crons_cli.py
Shared files (append-only, keep edits minimal to avoid merge conflicts):
- packages/core/src/graph_agent/_internal/worker.py (append: sweep, prune, cron tasks; T26 appends shutdown handling in the same wave: keep to separate functions)
- packages/core/src/graph_agent/_internal/cli.py (append `_add_worker`, `_add_crons`; extend `migrate` with the Procrastinate schema step; T26 touches serve/worker options)
- packages/core/src/graph_agent/_internal/queue.py (append schema helpers if needed)
- packages/core/src/graph_agent/_internal/runtime/runs.py (T13, append `idle_threads(before)` if needed)
- packages/core/src/graph_agent/_internal/runtime/checkpointer.py (T11, append `prune(...)`)

## Out of scope
- Drain/requeue on SIGTERM and shutdown timeouts (T26)
- Changing the runner (T16)

## Acceptance criteria
- [ ] Sweep task every minute: stalled jobs via the job manager then `retry_job`; `runs.expired(max_attempts)` + `runs.stale_pending(runs.pending_redefer_s)` then defer each requeued run (idempotent with claims)
- [ ] Prune task: `keep_latest` for threads idle longer than `threads.ttl_days`, `delete_thread` past the second TTL
- [ ] One periodic task per manifest cron, registered from settings with kwargs; firing creates a run on a NEW thread and defers it
- [ ] `crons` lists flow, schedule and next fire time (UTC) using T05's helper; `worker` starts the Procrastinate worker with an opened container; `migrate` also applies the Procrastinate schema (after SQL migrations and checkpointer setup)
- [ ] Integration tests: orphan run is re-deferred (status unchanged) and executes after a re-claim; a cron firing creates a run; stalled job is requeued
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Follow `docs/spikes/S4.md` for the exact Procrastinate API and the schema upgrade path.
- This task, not T18, owns the Procrastinate step in `migrate` because `queue.py` did not exist yet in T18's wave (report).
- Keep tasks in `worker.py` as small functions delegating to repositories.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
- From the T13 review (orchestrator decision, design §5): the sweep never writes `pending`. It (1) marks exhausted expired runs `error`, (2) re-defers expired running runs as they are, (3) re-defers `pending` runs older than `runs.pending_redefer_s`; dedupe via `queueing_lock=run_id`. From T11 review: `aprune`/`adelete_thread` exist on AsyncPostgresSaver (not on InMemorySaver); a long prune holds the saver lock — use a dedicated saver instance for pruning; never call the same saver inside its `alist` iteration.
