---
id: T12
title: Run events, usage totals, observer protocol, RunContext and FlowContext
part: 5
wave: E
depends_on: [T05]
unblocks: [T14, T21, T30, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T12-events-context
status: done
---

## Goal
The transport-agnostic run event types and the mapping from LangGraph v2 stream parts to them, plus `RunContext`, the `FlowContext` protocol and its implementation.

## Design references
- design.md §3 "Public API" (`RunContext`, `FlowContext`)
- design.md §6 "SSE events" table (event names and DTO fields)
- design.md §5 "Single runner" step 3
- design.md §13 rows "Run context" and "Event DTOs and SSE"

## Scope: files this task may create or edit
- packages/core/src/ranvier/_internal/runtime/events.py
- packages/core/src/ranvier/_internal/runtime/context.py
- packages/core/tests/unit/runtime/test_events.py
- packages/core/tests/unit/runtime/test_context.py
Shared files (append-only, keep edits minimal to avoid merge conflicts):
- packages/core/src/ranvier/__init__.py (append `RunContext`, `FlowContext` to imports and `__all__`; T27 appends `AgentSpec` in the same wave)

## Out of scope
- Wire DTOs and SSE encoding (T21)
- The actual `agent()` and `tools()` behavior (T30 completes it; T12 takes injected callables)
- Event sequencing/`seq` (assigned at the SSE edge, T21)
- The runner (T16)

## Acceptance criteria
- [ ] `events.py`: frozen dataclasses for metadata, updates, values, messages, custom, interrupt, end (status + `UsageTotals`), error (`code`, `message`, `retryable` as plain strings/bool); `RunOutcome`; `RunObserver` Protocol (`callbacks(ctx) -> Sequence[BaseCallbackHandler]`, `async on_finish(ctx, outcome)`) used by T16 and T32
- [ ] `map_stream_part(part) -> RunEvent | None` converting LangGraph `version="v2"` stream parts (updates/values/messages/custom and the interrupt marker); drops nothing silently: unknown types raise
- [ ] `RunContext` exactly as design §3 (frozen, slots); `FlowContext` Protocol exactly as design §3
- [ ] `FlowContextImpl(flow, config, model_fn, tools_fn, agent_fn)`: `model()` and `tools()` delegate to injected callables; `agent()` delegates to `agent_fn` and raises a clear error if none was injected
- [ ] Unit tests for the mapping (one per stream mode, interrupt) using real tiny graphs, and for `FlowContextImpl`
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Port the event mapping from `/home/wysted/code/agintis/src/agintis/agent_kit/runtime/langgraph_runner.py` and `events.py`; drop chat-specific events and `STREAM_TOKENS_TAG` (OD-8: token streaming is `stream_mode=messages`).
- The exact v2 part shape is something S1 also records; if `docs/spikes/S1.md` already exists, use it, otherwise check the installed langgraph.
- Error codes are NOT defined here: T13 owns the `ErrorCode` enum in `runs.py` (T12 and T13 are in the same wave).
- langgraph/langchain imports are allowed in `runtime` (C5).
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
