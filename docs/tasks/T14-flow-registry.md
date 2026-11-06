---
id: T14
title: Flow resolution, lazy compile cache, version checks
part: 5
wave: F
depends_on: [T05, T12, S1]
unblocks: [T16, T17, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T14-flow-registry
status: done
---

## Goal
Resolve flow names to compiled graphs lazily, cache them per process, and reject incompatible thread versions.

## Design references
- design.md §4 "What `target` may resolve to" table and the paragraph after it
- design.md §4 "Persisted contracts" table
- design.md §5 "Single runner" step 2
- design.md §2 layout row `runtime/flows.py`

## Scope: files this task may create or edit
- packages/core/src/ranvier/_internal/runtime/flows.py
- packages/core/tests/unit/runtime/test_flows.py
Shared files: none.

## Out of scope
- Running the graph (T16)
- `validate` command logic (T17 appends a function here later)
- Building real `FlowContext` collaborators (T15, T30)

## Acceptance criteria
- [ ] `FlowRegistry(flows_settings, checkpointer, context_factory)` with `async get(name) -> CompiledFlow` (graph, version, durability, timeout, max_concurrency)
- [ ] Target kinds: uncompiled `StateGraph` -> compiled with the platform checkpointer; sync or async callable taking `FlowContext` returning a `StateGraph`; `CompiledStateGraph` -> loud error; anything else -> loud error naming the manifest key
- [ ] Lazy and cached per flow; concurrent first calls build once (per-flow lock); a failed build is NOT cached and raises `RunError(flow_build_failed, retryable=True)`
- [ ] `check_version(compiled, recorded_version)` raises `RunError(flow_version_mismatch)`; unknown flow raises a distinct not-found error
- [ ] Unit tests with tiny `StateGraph`s and `InMemorySaver`: each target kind, build failure then success on retry, concurrent get builds once
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Read `docs/spikes/S1.md`: it decides whether flows must declare `context_schema` to receive `runtime.context` and what the compile call needs.
- Pass `name=<flow>` to `compile` for readable traces.
- Error codes and `RunError` come from `runtime/runs.py` (T13).
- Conventions: follow the project's Python conventions and the decision log; never read `.env`; agintis is read-only reference.
