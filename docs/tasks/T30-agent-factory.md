---
id: T30
title: build_agent, as_tool and ctx.agent(spec)
part: 9
wave: F
depends_on: [T08, T12, T27, T28, T29]
unblocks: [T19, T31, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T30-agent-factory
status: done
---

## Goal
Build agents from an `AgentSpec` over `create_agent`, expose `as_tool`, and complete `FlowContext.agent/tools/model` wiring.

## Design references
- design.md §7 first paragraph (the `create_agent(...)` call) and "Using a subagent in a flow" table
- design.md §3 `as_tool` and `FlowContext`
- design.md §9 "Usage ledger" (`agent` = `AgentSpec.name`)

## Scope: files this task may create or edit
- packages/core/src/ranvier/_internal/agents/factory.py
- packages/core/tests/unit/agents/test_factory.py
- packages/core/src/ranvier/_internal/runtime/context.py (T12's file, earlier wave: wire `agent`, `tools`, `model` to the real collaborators)
Shared files (append-only, keep edits minimal to avoid merge conflicts):
- packages/core/src/ranvier/__init__.py (append `as_tool` to imports and `__all__`)

## Out of scope
- Container edits (T15 already wires registry and MCP)
- Usage ledger attribution code (T32)
- `[rag]` / `[deepagents]` builders (not implemented in v0.1)

## Acceptance criteria
- [ ] `build_agent(spec, *, models, registry, mcp, ...)` calls `create_agent(model=..., tools=registry.resolve(spec.tools) + MCP tools, system_prompt=..., middleware=[SkillsMiddleware(spec.skills) if skills, *spec.middleware], response_format=..., name=spec.name)`; no checkpointer is passed
- [ ] `as_tool(agent, *, name, description)` returns a `BaseTool` that runs the agent statelessly on a text task and returns the final message content
- [ ] `FlowContext.agent(spec)` awaits `build_agent`; `tools(*names)` and `model(profile)` use the registry and LLM registry
- [ ] Tests with `GenericFakeChatModel`: agent that calls a registry tool, `as_tool` round trip, MCP tools via a fake `McpToolSource`
- [ ] `AgentSpec.name` reaches callback metadata as S3 documents
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Read `docs/spikes/S3.md` (agent name in callbacks) and the langchain 1.4 `create_agent` signature in the installed version.
- Subagent patterns (node vs tool) are plain LangGraph; add no helper beyond `as_tool`.
- Beyond tasks.md deps: T19's `flow_context` will call `build_agent` later.
- Conventions: follow the project's Python conventions and the decision log; never read `.env`; agintis is read-only reference.
- From the T28 review (orchestrator, approved scope extension: you may edit `_internal/agents/skills.py`): (1) extend the system prompt preserving `request.system_message.content_blocks` (pattern of langchain `TodoListMiddleware`) instead of `.text`; (2) add a test through `awrap_model_call` and an end-to-end test through `create_agent` with a fake model that supports `bind_tools`; (3) add `shortcut: ASCII-only skill names, stricter than skills-ref; relax if a real skill needs it` on `_NAME_PATTERN`.
- From the T29 review (orchestrator decision, design §7 "Tool name collisions"): `build_agent` rejects duplicate tool names across `registry(spec.tools) + mcp(spec.mcp_servers)` with an error naming both sources; MCP `tools()` raises `McpSourceError` (no mcp/httpx2 types) when a server is unavailable.
