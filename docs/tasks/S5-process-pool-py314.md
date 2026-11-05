---
id: S5
title: ProcessPoolExecutor.kill_workers and langgraph on Python 3.14
part: spike
wave: B
depends_on: [T01]
unblocks: [T09]
owner: sa-debug-py
reviewer: sa-review-py
branch: task/S5-process-pool-py314
status: todo
---

## Goal
Verify `ProcessPoolExecutor.kill_workers` on 3.14, the fallback on 3.13, and that langgraph imports and runs a basic graph on 3.14.

## Design references
- design.md §14 "Open checks" rows on `kill_workers` and "langgraph on 3.14"
- design.md §8 row "CPU-heavy tools"
- design.md OD-7 row

## Scope: files this task may create or edit
- docs/spikes/S5.md (findings)
- bench/spikes/S5/ (throwaway scripts only)
Shared files: none.

## Out of scope
- Editing `docs/design.md` (write proposed edits in the findings file; the orchestrator applies them)
- Editing anything under `packages/`, `pyproject.toml` or `uv.lock` (use `uv run --with <pkg>` or a script-local `uv run --script` header)
- Implementing the dependent tasks: T09
- Reading `.env`, calling paid APIs, or touching `~/code/agintis*` beyond reading

## Acceptance criteria
- [ ] On CPython 3.14.5+ (install via `uv python install`): `kill_workers()` semantics with forkserver, effect on pending and running tasks, how the pool is recycled after a timeout
- [ ] On 3.13: the smallest correct fallback to kill a stuck worker and recycle the pool (no reliance on undocumented private attributes unless unavoidable; say which)
- [ ] langgraph + langgraph-checkpoint-postgres import and run a tiny graph on 3.14 (InMemorySaver); note warnings/failures
- [ ] Recommendation for `limits.py`: one API that behaves the same on both versions
- [ ] `docs/spikes/S5.md` has: Question, Method, Result per check (PASS/FAIL/PARTIAL with exact package versions), Impact on design (list of concrete `design.md` edits as before/after text), Impact on dependent tasks (T09)
- [ ] Every claim is backed by a script output or a source file path; no claim from memory
- [ ] Scripts live only under `bench/spikes/S5/` and run with one documented command

## Notes
- Use `uv run --python 3.14` / `3.13` with `--with` packages; do not change the project's Python constraints.
- Avoid 3.14.0-3.14.4 (GC regression).
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
