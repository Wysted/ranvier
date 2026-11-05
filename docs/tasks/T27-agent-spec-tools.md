---
id: T27
title: AgentSpec and tool registry with limit wrappers
part: 9
wave: E
depends_on: [T05, T09]
unblocks: [T15, T30, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T27-agent-spec-tools
status: todo
---

## Goal
`AgentSpec` and a tool registry that imports `[tools.*]` lazily and wraps each tool once with timeout, semaphore and isolation.

## Design references
- design.md §3 `AgentSpec`
- design.md §7 table row "Tool registry" and the closing paragraph of §8 (sync tools must declare `isolation = "thread"`)
- design.md §4 manifest `[tools.web_search]`
- design.md §8 rows "Tool", "Sync tools", "CPU-heavy tools", "Error isolation"

## Scope: files this task may create or edit
- packages/core/src/graph_agent/_internal/agents/spec.py
- packages/core/src/graph_agent/_internal/agents/tools.py
- packages/core/tests/unit/agents/test_tools.py
Shared files (append-only, keep edits minimal to avoid merge conflicts):
- packages/core/src/graph_agent/__init__.py (append `AgentSpec`; T12 appends `RunContext`/`FlowContext` in the same wave)

## Out of scope
- Skills (T28), MCP (T29), agent factory and `as_tool` (T30)
- Creating the registry in the container (T15)

## Acceptance criteria
- [ ] `AgentSpec` frozen/slots dataclass with the fields of design §3
- [ ] `ToolRegistry(tools_settings, limits)`: `get(name)` imports lazily and caches; `resolve(Sequence[str | BaseTool]) -> list[BaseTool]` (objects get default limits)
- [ ] Each tool is wrapped once: semaphore from `max_concurrency`, `asyncio.timeout(timeout_s or limits.tool_timeout_s)`, isolation `async` (coroutine required), `thread` (default bounded executor), `process` (T09 pool; arguments must be picklable)
- [ ] A sync-only tool declared `isolation = "async"` is rejected with an error naming `tools.<name>.isolation`
- [ ] A raising or timing-out tool becomes an error `ToolMessage` for the model (via `ToolException`/`handle_tool_error`) and does not touch other runs
- [ ] Unit tests: timeout, concurrency cap, sync rejection, thread isolation, error to message
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- langchain-core tools are allowed in `agents` (C5).
- Preserve the original `name`, `description` and `args_schema` when wrapping.
- Tool name collisions with MCP tools are resolved in T30, not here.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
