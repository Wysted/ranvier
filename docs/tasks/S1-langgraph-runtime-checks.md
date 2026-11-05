---
id: S1
title: LangGraph runtime checks: RunControl, context, checkpoint metadata
part: spike
wave: B
depends_on: [T01]
unblocks: [T14, T16, T26]
owner: sa-debug-py
reviewer: sa-review-py
branch: task/S1-langgraph-runtime-checks
status: done
---

## Goal
Verify against the installed langgraph 1.2.x how `RunControl` is imported and passed to `astream`, whether runtime context reaches nodes without `context_schema`, and whether checkpoint metadata carries `metadata.run_id`.

## Design references
- design.md §14 "Open checks" rows `RunControl`, "Checkpoint metadata carries `metadata.run_id`", "Runtime context reaches nodes when a graph declares no `context_schema`"
- design.md §5 "Single runner" step 3 and the input-choice paragraph
- design.md §8 row "Graceful shutdown"

## Scope: files this task may create or edit
- docs/spikes/S1.md (findings)
- bench/spikes/S1/ (throwaway scripts only)
Shared files: none.

## Out of scope
- Editing `docs/design.md` (write proposed edits in the findings file; the orchestrator applies them)
- Editing anything under `packages/`, `pyproject.toml` or `uv.lock` (use `uv run --with <pkg>` or a script-local `uv run --script` header)
- Implementing the dependent tasks: T14, T16, T26
- Reading `.env`, calling paid APIs, or touching `~/code/agintis*` beyond reading

## Acceptance criteria
- [ ] Exact import path, constructor and `astream` parameter for `RunControl`; `request_drain()` behavior (when the checkpoint is written, the `GraphDrained` exception, what state the thread is left in, whether a resume continues correctly) demonstrated with a multi-step graph
- [ ] Whether `astream(..., context=RunContext(...))` works and `runtime.context` is visible in nodes with and without `context_schema`; exact failure mode if not
- [ ] Whether `config["metadata"]["run_id"]` ends up in `checkpoint.metadata` of the latest checkpoint (InMemorySaver; Postgres saver too if compose is already up) and how to read it back
- [ ] Bonus: record the exact shape of `version="v2"` stream parts for updates/values/messages/custom and for interrupts (useful to T12/T16)
- [ ] `docs/spikes/S1.md` has: Question, Method, Result per check (PASS/FAIL/PARTIAL with exact package versions), Impact on design (list of concrete `design.md` edits as before/after text), Impact on dependent tasks (T14, T16, T26)
- [ ] Every claim is backed by a script output or a source file path; no claim from memory
- [ ] Scripts live only under `bench/spikes/S1/` and run with one documented command

## Notes
- Use the langgraph version pinned in `uv.lock` (T01); state it in the findings.
- If sources disagree on `RunControl` (as design.md warns), the installed package source is the authority.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
