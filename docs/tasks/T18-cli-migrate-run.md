---
id: T18
title: CLI: migrate and run (with --memory)
part: 5
wave: H
depends_on: [T16]
unblocks: [T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T18-cli-migrate-run
status: todo
---

## Goal
`graph-agent migrate` (own SQL + checkpointer setup) and `graph-agent run <flow> --input JSON [--thread ID] [--resume JSON] [--memory]`.

## Design references
- design.md §6 "CLI" table rows `run` and `migrate`
- design.md §10 "Persistence" table
- design.md §5 "Interrupts" (resume map)

## Scope: files this task may create or edit
- packages/core/tests/unit/test_cli_run.py
- packages/core/tests/integration/test_cli_migrate.py
Shared files (append-only, keep edits minimal to avoid merge conflicts):
- packages/core/src/graph_agent/_internal/cli.py (append `_add_migrate`, `_add_run` and handlers; T23 appends `serve` in the same wave)

## Out of scope
- Procrastinate schema step in `migrate` (T25 appends it once `queue.py` exists)
- `serve`, `worker`, `crons`, `usage` (T23, T25, T32)
- Runner internals (T16)

## Acceptance criteria
- [ ] `migrate`: `apply_migrations` then `setup_checkpointer`, in that order, one pool each; prints what was applied; idempotent
- [ ] `run` without `--memory`: creates a run row, drives `execute_run`, prints events as JSON lines; with `--memory`: `open_container(memory=True)` + `stream_graph`, no database touched
- [ ] `--input` and `--resume` mutually exclusive; invalid JSON is an error with exit code 2; `--resume` accepts a map `{interrupt_id: value}`
- [ ] Exit code 0 on success/interrupted, non-zero on error/cancelled
- [ ] Tests: `main([...])` with `--memory` on a toy flow (success and interrupt); integration test for `migrate`
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Keep handlers thin: parse, open container, call runtime, print. `asyncio.run` once per command.
- To run a toy flow in tests build a tmp manifest, same technique as T17's tests.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
- From T11 (orchestrator decision): run `setup_checkpointer` (and later the Procrastinate schema step) under the SAME advisory lock as the own SQL migrations, so concurrent `migrate` invocations cannot interleave the saver's migrations.
