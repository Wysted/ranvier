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
status: done
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
- packages/core/src/ranvier/_internal/worker.py (append: sweep, prune, cron tasks; T26 appends shutdown handling in the same wave: keep to separate functions)
- packages/core/src/ranvier/_internal/cli.py (append `_add_worker`, `_add_crons`; extend `migrate` with the Procrastinate schema step; T26 touches serve/worker options)
- packages/core/src/ranvier/_internal/queue.py (append schema helpers if needed)
- packages/core/src/ranvier/_internal/runtime/runs.py (T13, append `idle_threads(before)` if needed)
- packages/core/src/ranvier/_internal/runtime/checkpointer.py (T11, append `prune(...)`)

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
- From S4 and its reviews (orchestrator; read docs/spikes/S4.md 'Impact on dependent tasks' and bench/spikes/S4/sweep_check.py `requeue_stalled`): sweep = `app.periodic(cron='* * * * *', periodic_id='sweep', queueing_lock='sweep')` with NO `lock=`; exclude overlapping sweeps with `pg_try_advisory_xact_lock(SWEEP_LOCK_KEY)` in a transaction on a dedicated connection held for the whole sweep, opened with `options='-c tcp_keepalives_idle=10 -c tcp_keepalives_interval=5 -c tcp_keepalives_count=3'` so a lost host frees it in ~25 s (rule 1d: never a session lock on the pool); not acquired -> return, log at debug and count consecutive skipped sweeps as a metric. Order: stalled jobs (`get_stalled_jobs(seconds_since_heartbeat=30)` + `retry_job`) BEFORE orphan re-defer; on `UniqueViolation` (queueing lock) `finish_job(job, status=Status.FAILED, delete_job=False)`; per-job error handling. Crons: `periodic_id` stable from the cron's content (not list index), fresh kwargs dict per cron (Procrastinate mutates it with `timestamp`), the task's run/thread ids are uuid5(periodic_id, timestamp) with `INSERT ... ON CONFLICT DO NOTHING`. Migrate: Procrastinate step under the SAME `advisory_lock(pool)` helper from T18 (db/migrate.py), record table `ranvier.procrastinate_migrations` (fresh DB: schema.sql + record all shipped names; existing: apply unrecorded files in name order); `get_schema()`/migration `read_text` are sync file I/O -> `asyncio.to_thread` or before `asyncio.run`; execute without params so `%` needs no escaping. The sweep's dedicated connection counts +1 in the connection budget. Integration-test hints: S4 sweep scenario B (stalled job + todo duplicate), kill the worker mid-sweep -> next sweep still runs, two concurrent sweeps -> one skipped.
- From the T24 review (orchestrator): the `worker` command adds +1 to `log_connection_budget` for Procrastinate's standalone LISTEN connection (or pass the process role); pin the Procrastinate tables' schema the same way in `make_app` and in `migrate` (reuse `db.pool._pinned_options`, same reason as the saver's search_path decision); re-defer pending runs ordered by `created_at`; orphan lease re-defers use `RunQueue.defer(..., recovery=True)` if T24's fix pass lands the RECOVERY_PRIORITY change (check runner.py).
