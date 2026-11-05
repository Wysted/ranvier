---
id: T31
title: examples/: research flow with subagent and MCP
part: 9
wave: J
depends_on: [T20, T30]
unblocks: [T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T31-example-research
status: done
---

## Goal
A `research` example flow that uses a subagent (as a tool) and MCP tools, with a test using fakes and an in-process MCP server.

## Design references
- design.md §2 "Workspace" tree (`research (subagent + MCP)`)
- design.md §7 "Using a subagent in a flow" table
- design.md §7 `[mcp.servers.github]` block
- design.md §11 "Testing a flow"

## Scope: files this task may create or edit
- examples/src/examples/flows/research.py
- examples/tests/test_research.py
- examples/graph-agent.toml (append the `research` flow, a profile and an `[mcp.servers.docs]` entry)
Shared files: none.

## Out of scope
- Real network MCP servers or real LLM calls
- Changes to the agents kit (T30)

## Acceptance criteria
- [ ] `research(ctx)` factory: builds a subagent with `ctx.agent(AgentSpec(...))`, hands it to a parent agent via `as_tool`, parent also has MCP tools
- [ ] Test uses `graph_agent.testing` (`FakeChatModel` scripts for parent and subagent, `mcp=` fake source or an in-process FastMCP server) and asserts on the final state and the tool calls
- [ ] `graph-agent validate --offline` passes on the example manifest
- [ ] Only the public facade is imported (C2)
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Keep it small; it is documentation of the pattern.
- If the `mcp=` test parameter of `flow_context` (T19) is missing, report instead of working around it inside the example.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
