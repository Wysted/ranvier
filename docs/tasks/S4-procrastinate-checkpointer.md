---
id: S4
title: Procrastinate periodic/stalled/schema and AsyncPostgresSaver write lock
part: spike
wave: C
depends_on: [T01, T07]
unblocks: [T24]
owner: sa-debug-py
reviewer: sa-review-py
branch: task/S4-procrastinate-checkpointer
status: todo
---

## Goal
Verify Procrastinate's dynamic periodic tasks, stalled-jobs API and schema upgrade path, `lock=` semantics, and the `AsyncPostgresSaver` write-lock claim, against compose Postgres.

## Design references
- design.md §14 "Open checks" rows on Procrastinate and `AsyncPostgresSaver`
- design.md §5 "Serialization, recovery and housekeeping" table
- design.md §10 "Persistence" table row Procrastinate

## Scope: files this task may create or edit
- docs/spikes/S4.md (findings)
- bench/spikes/S4/ (throwaway scripts only)
Shared files: none.

## Out of scope
- Editing `docs/design.md` (write proposed edits in the findings file; the orchestrator applies them)
- Editing anything under `packages/`, `pyproject.toml` or `uv.lock` (use `uv run --with <pkg>` or a script-local `uv run --script` header)
- Implementing the dependent tasks: T24, T25
- Reading `.env`, calling paid APIs, or touching `~/code/agintis*` beyond reading

## Acceptance criteria
- [ ] Register one periodic task per cron from data at runtime with kwargs (cron expression format, UTC, timestamps passed to the task); give the working snippet
- [ ] Stalled jobs: exact API (`get_stalled_jobs`, `retry_job`), the worker heartbeat setting and its defaults; demonstrate a killed worker's job being requeued
- [ ] Schema: how to apply it and the scripted upgrade path between versions
- [ ] `lock=<thread_id>`: two jobs with the same lock run sequentially, different locks in parallel; behavior when the first job fails/retries
- [ ] `AsyncPostgresSaver`: read the source for per-instance write serialization and measure concurrent `aput` through one instance vs two pooled instances; recommend checkpointer pool sizing
- [ ] `docs/spikes/S4.md` has: Question, Method, Result per check (PASS/FAIL/PARTIAL with exact package versions), Impact on design (list of concrete `design.md` edits as before/after text), Impact on dependent tasks (T24, T25)
- [ ] Every claim is backed by a script output or a source file path; no claim from memory
- [ ] Scripts live only under `bench/spikes/S4/` and run with one documented command

## Notes
- Needs `docker compose up -d` from T07; use a throwaway database.
- Procrastinate API differs between versions; record the installed one.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
