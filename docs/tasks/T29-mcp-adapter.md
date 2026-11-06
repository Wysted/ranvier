---
id: T29
title: McpToolSource port and own adapter over the mcp SDK v2
part: 9
wave: E
depends_on: [T05, T09, S2]
unblocks: [T15, T30, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T29-mcp-adapter
status: done
---

## Goal
The `McpToolSource` port and an adapter that turns MCP server tools into LangChain `StructuredTool`s with timeouts and semaphores, over `mcp.Client` (Streamable HTTP).

## Design references
- design.md "Decisions at a glance" row MCP
- design.md §7 table rows "MCP port", "MCP results", "MCP transport" and the `[mcp.servers.github]` block
- design.md §8 row "MCP server"
- design.md §14 open check on MCP and `.claude/agents/_shared/decisions.md` (last MCP line)

## Scope: files this task may create or edit
- packages/core/src/ranvier/_internal/agents/mcp.py
- packages/core/tests/unit/agents/test_mcp.py (result mapping, schema conversion)
- packages/core/tests/integration/agents/test_mcp_server.py (in-process FastMCP over Streamable HTTP, no external network)
Shared files: none.

## Out of scope
- Stdio transport, resources, prompts, `InputRequiredResult` -> `interrupt()` (out of scope v0.1)
- Container wiring (T15 calls your `open_mcp_sources`)
- Agent factory (T30)

## Acceptance criteria
- [ ] `McpToolSource` Protocol: `async tools(server) -> Sequence[BaseTool]`
- [ ] `open_mcp_sources(servers_settings, stack) -> McpToolSource`: one `mcp.Client` per server with its own `httpx2.AsyncClient` (headers from env var NAMES via `headers_env`, timeouts), entered in the passed `AsyncExitStack`; tools listed once (paginated) and cached; reconnects on session loss as S2 prescribes
- [ ] Each tool: `StructuredTool(name, description, args_schema=inputSchema, coroutine=..., response_format="content_and_artifact")` wrapped by the server semaphore and `asyncio.timeout`
- [ ] Result mapping per design §7: text blocks -> content; image/resource blocks -> content blocks plus artifact; `structuredContent` -> artifact; `isError` -> error `ToolMessage`; `InputRequiredResult` -> clear error
- [ ] Tool name collisions across servers fail loudly
- [ ] Tests against an in-process FastMCP server from the same SDK: list, call, error result, concurrent calls where one fails, reconnect after server restart (if S2 says feasible)
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Read `docs/spikes/S2.md` first; it records the real `mcp.Client` API and the session-reuse findings.
- `mcp` and `httpx2` may be imported only in this file (ruff banned-api); tests are excepted.
- Never place secrets in config values; headers come from the environment at connect time.
- tasks.md lists no dependency on T05, but the `mcp` server settings model is T05's; it is in an earlier wave.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
- From S2 (orchestrator, overrides any "entered in the passed AsyncExitStack" wording above): each `mcp.Client` is entered and held by its own owner task per server; the composition root only registers the owner's stop. Use the 502-converting httpx2 transport, reconnect on the next call under a per-server lock (owner task dead or `INVALID_REQUEST` stale session), never retry the failed call. Port from `bench/spikes/S2/s2.py` (`OwnedClient`) and add the missing lock. Tests: in-process `MCPServer` via `session_manager.run()` + base URL with explicit port; restart test needs uvicorn on a local port.
