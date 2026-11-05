---
id: S2
title: mcp SDK v2 Client: session reuse, reconnect, failure isolation
part: spike
wave: B
depends_on: [T01]
unblocks: [T29]
owner: sa-debug-py
reviewer: sa-review-py
branch: task/S2-mcp-sdk-session
status: todo
---

## Goal
Verify with the installed `mcp` 2.x how one `mcp.Client` per server behaves for the lifetime of the process.

## Design references
- design.md §14 "Open checks" first row (MCP)
- design.md §7 table rows "MCP port", "MCP results"
- design.md "Decisions at a glance" row MCP
- .claude/agents/_shared/decisions.md last MCP line

## Scope: files this task may create or edit
- docs/spikes/S2.md (findings)
- bench/spikes/S2/ (throwaway scripts only)
Shared files: none.

## Out of scope
- Editing `docs/design.md` (write proposed edits in the findings file; the orchestrator applies them)
- Editing anything under `packages/`, `pyproject.toml` or `uv.lock` (use `uv run --with <pkg>` or a script-local `uv run --script` header)
- Implementing the dependent tasks: T29
- Reading `.env`, calling paid APIs, or touching `~/code/agintis*` beyond reading

## Acceptance criteria
- [ ] One `mcp.Client` entered once reuses its session across many sequential and concurrent calls (show with a counter on the in-process FastMCP server)
- [ ] Reconnect after the server restarts: what error surfaces, whether the client recovers by itself or needs re-entry, and the minimal code that recovers
- [ ] One failing request (tool raising / timeout) does not kill concurrent requests (python-sdk #2604) in the pinned 2.x
- [ ] How to host an in-process FastMCP server over Streamable HTTP for tests (ASGI transport vs a local port), and how `httpx2.AsyncClient` with headers is passed to `mcp.Client`
- [ ] Optional: do image/resource content blocks survive to a LangChain `ToolMessage` content-block shape
- [ ] `docs/spikes/S2.md` has: Question, Method, Result per check (PASS/FAIL/PARTIAL with exact package versions), Impact on design (list of concrete `design.md` edits as before/after text), Impact on dependent tasks (T29)
- [ ] Every claim is backed by a script output or a source file path; no claim from memory
- [ ] Scripts live only under `bench/spikes/S2/` and run with one documented command

## Notes
- Record the real `mcp.Client` API names; T29 will rely on them.
- No external servers; everything local.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
