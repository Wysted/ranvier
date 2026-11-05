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
status: todo
---

## Goal
On SIGTERM stop admission, drain running runs with `RunControl.request_drain()`, put them back to `pending`, defer them, and align server shutdown timeouts.

## Design references
- design.md §8 row "Graceful shutdown"
- design.md §5 "Transitions" row `running -> pending` (graceful shutdown)
- design.md §14 open check on `RunControl`

## Scope: files this task may create or edit
- packages/core/tests/integration/test_shutdown.py
Shared files (append-only, keep edits minimal to avoid merge conflicts):
- packages/core/src/graph_agent/_internal/runtime/runner.py (T16, append: registry of active runs + `drain_all(timeout)`)
- packages/core/src/graph_agent/_internal/limits.py (T09, append `stop_admission()`)
- packages/core/src/graph_agent/_internal/api/app.py (T23, lifespan shutdown hook)
- packages/core/src/graph_agent/_internal/worker.py (shared with T25: keep to your own functions)
- packages/core/src/graph_agent/_internal/cli.py (shared with T25: timeouts passed to uvicorn and the worker)

## Out of scope
- Sweep and recovery of crashed owners (T25)
- Changing run semantics or the transition table

## Acceptance criteria
- [ ] SIGTERM (or lifespan/worker shutdown): `stop_admission()` first; new inline runs get 503/429-style refusal with the standard error body
- [ ] Every active run gets `RunControl.request_drain()` (import and usage exactly as S1 documents); when `GraphDrained` arrives the row goes back to `pending` (`attempt` unchanged by the requeue itself) and is deferred again; runs exceeding `limits.shutdown_grace_s` are left to the lease-expiry sweep
- [ ] uvicorn `timeout_graceful_shutdown` and Procrastinate `shutdown_graceful_timeout` are set from `limits.shutdown_grace_s`; a warning is logged at startup if it is smaller than the longest flow `timeout_s` or node timeout
- [ ] Integration test: start a slow multi-step flow, trigger shutdown, assert the run is `pending`, resumable from its checkpoint, and not executed twice
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- If S1 shows `RunControl` is unusable in the installed langgraph, stop and report; do not invent a different mechanism.
- Depends on T23/T24, so the API and worker entry points exist; touch them minimally.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
