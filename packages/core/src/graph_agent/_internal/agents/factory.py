"""`build_agent` (AgentSpec -> `create_agent`) and `as_tool`.

langchain imports are deferred into the functions: the public facade re-exports `as_tool` and
must not load langchain/langgraph eagerly.
"""

import asyncio
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from langchain_core.language_models import BaseChatModel
    from langchain_core.tools import BaseTool
    from langgraph.graph.state import CompiledStateGraph

    from graph_agent._internal.agents.mcp import McpToolSource
    from graph_agent._internal.agents.spec import AgentSpec
    from graph_agent._internal.agents.tools import ToolRegistry


class AgentBuildError(ValueError):
    """The spec cannot be turned into an agent (tool name collision, MCP not configured)."""


async def build_agent(
    spec: "AgentSpec",
    *,
    models: Callable[[str], "BaseChatModel"],
    registry: "ToolRegistry",
    mcp: "McpToolSource | None" = None,
) -> "CompiledStateGraph[Any, Any, Any, Any]":
    from langchain.agents import create_agent

    from graph_agent._internal.agents.skills import SkillsMiddleware
    from graph_agent._internal.usage import AGENT_METADATA_KEY  # loads langchain: keep it lazy

    # SKILL.md files are read synchronously in the constructor.
    middleware = list(spec.middleware)
    if spec.skills:
        middleware.insert(0, await asyncio.to_thread(SkillsMiddleware, spec.skills))

    # The first resolution of a `[tools.*]` name imports user modules (blocking I/O).
    resolved = await asyncio.to_thread(registry.resolve, spec.tools)
    sources: list[tuple[str, Sequence[BaseTool]]] = [
        (f"tools.{item}" if isinstance(item, str) else "tool object", [tool])
        for item, tool in zip(spec.tools, resolved, strict=True)
    ]
    if spec.mcp_servers:
        if mcp is None:
            raise AgentBuildError(f"agent {spec.name!r}: mcp_servers set but no MCP source wired")
        for server in spec.mcp_servers:
            sources.append((f"MCP server {server!r}", await mcp.tools(server)))
    # create_agent adds each middleware's tools (load_skill, write_todos, ...) to the same set.
    sources.extend((f"middleware {type(m).__name__}", m.tools) for m in middleware)

    tools: list[BaseTool] = []
    origin: dict[str, str] = {}
    for source, found in sources:
        for t in found:
            if t.name in origin:
                raise AgentBuildError(
                    f"agent {spec.name!r}: duplicate tool name {t.name!r} "
                    f"(from {origin[t.name]} and {source})"
                )
            origin[t.name] = source
            if not source.startswith("middleware "):
                tools.append(t)

    model = models(spec.profile)
    # Local callback metadata of the model instance: the innermost agent wins (S3).
    model = model.model_copy(
        update={"metadata": {**(model.metadata or {}), AGENT_METADATA_KEY: spec.name}}
    )
    return create_agent(
        model=model,
        tools=tools,
        system_prompt=spec.system_prompt,
        middleware=middleware,
        response_format=spec.response_format,
        name=spec.name,
    )


def as_tool(
    agent: "CompiledStateGraph[Any, Any, Any, Any]",
    *,
    name: str,
    description: str,
    timeout_s: float | None = None,
) -> "BaseTool":
    """Expose an agent as a tool: a run on a text task, returning the final text.

    The subagent runs as a subgraph of the caller: with a parent checkpointer it checkpoints under
    a per-call namespace, so an `interrupt()` inside it is resumable. `timeout_s=None` applies no
    per-tool timeout (the run timeout bounds it, unlike the registry's default for other tools).
    """
    from langchain_core.runnables import RunnableConfig
    from langchain_core.tools import StructuredTool

    from graph_agent._internal.agents.tools import TOOL_TIMEOUT_METADATA_KEY

    async def run(task: str, config: RunnableConfig) -> str:
        # The tool's config propagates callbacks and limits; attribution survives it (S3).
        result = await agent.ainvoke({"messages": [{"role": "user", "content": task}]}, config)
        return str(result["messages"][-1].text)

    return StructuredTool.from_function(
        coroutine=run,
        name=name,
        description=description,
        metadata={TOOL_TIMEOUT_METADATA_KEY: timeout_s},
    )
