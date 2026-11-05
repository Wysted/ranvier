---
id: T19
title: Public testing kit: FakeChatModel, flow_context, run_flow
part: 6
wave: H
depends_on: [T16, T30]
unblocks: [T20, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T19-testing-kit
status: done
---

## Goal
`graph_agent.testing` exactly as design §11 so users can unit-test flows without a database or a real model.

## Design references
- design.md §11 "Kit shipped to users (`graph_agent.testing`)" code block and the paragraph after it
- design.md §2 contract C2
- design.md §6 "SSE events" (events in `FlowResult.events`)

## Scope: files this task may create or edit
- packages/core/src/graph_agent/testing.py
- packages/core/tests/unit/test_testing.py
Shared files: none.

## Out of scope
- `examples/` flows (T20)
- pytest plugin/fixtures (not in the design, YAGNI)
- Runner changes (T16)

## Acceptance criteria
- [ ] `FakeChatModel(responses)` re-exports/extends `runtime/offline.py` (T17): scripted `AIMessage`/str replies, tool calls allowed, `bind_tools` no-op, `calls` records received messages
- [ ] `flow_context(*, models, tools, config, mcp=None)` returns a `FlowContext`; the saver lives on the context so a second `run_flow` with the same context and `thread_id` resumes; `agent(spec)` builds through `build_agent` (T30)
- [ ] `run_flow(target, input, *, ctx, thread_id, resume)` accepts a builder or a factory (sync/async), uses `InMemorySaver` and `stream_graph`; returns `FlowResult(values, interrupts, events)`
- [ ] Resume with a bare value works when exactly one interrupt is pending; otherwise requires the map
- [ ] Tests: a plain flow, an interrupt/resume flow, a flow with a scripted tool call
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Beyond tasks.md deps: this needs T30 (`build_agent`, wave F) for `ctx.agent`.
- The optional `mcp=` parameter is a small ADDITION to the §11 signature so T31's research example can be tested without a network; report it so design.md can be updated.
- `graph_agent.testing` may import `_internal`, never the other way around (C2).
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
