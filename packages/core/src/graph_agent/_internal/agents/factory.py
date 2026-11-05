"""`build_agent` (AgentSpec -> `create_agent`) and `as_tool`.

langchain imports are deferred into the functions: the public facade re-exports `as_tool` and
must not load langchain/langgraph eagerly.
"""

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from langchain_core.language_models import BaseChatModel
    from langchain_core.tools import BaseTool
    from langgraph.graph.state import CompiledStateGraph

    from graph_agent._internal.agents.mcp import McpToolSource
    from graph_agent._internal.agents.spec import AgentSpec
    from graph_agent._internal.agents.tools import ToolRegistry

# Callback metadata key carrying AgentSpec.name; the usage ledger (T32) reads it (S3).
AGENT_METADATA_KEY = "graph_agent_agent"


class AgentBuildError(ValueError):
    """The spec cannot be turned into an agent (tool name collision, MCP not configured)."""


class ModelSource(Protocol):
    def model(self, profile: str) -> "BaseChatModel": ...


async def build_agent(
    spec: "AgentSpec",
    *,
    models: ModelSource,
    registry: "ToolRegistry",
    mcp: "McpToolSource | None" = None,
) -> "CompiledStateGraph[Any, Any, Any, Any]":
    from langchain.agents import create_agent

    from graph_agent._internal.agents.skills import SkillsMiddleware

    sources: list[tuple[str, Sequence[BaseTool]]] = [
        ("tool registry", registry.resolve(spec.tools))
    ]
    if spec.mcp_servers:
        if mcp is None:
            raise AgentBuildError(f"agent {spec.name!r}: mcp_servers set but no MCP source wired")
        for server in spec.mcp_servers:
            sources.append((f"MCP server {server!r}", await mcp.tools(server)))

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
            tools.append(t)

    middleware = list(spec.middleware)
    if spec.skills:
        # SKILL.md files are read synchronously in the constructor.
        middleware.insert(0, await asyncio.to_thread(SkillsMiddleware, spec.skills))

    model = models.model(spec.profile)
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
    agent: "CompiledStateGraph[Any, Any, Any, Any]", *, name: str, description: str
) -> "BaseTool":
    """Expose an agent as a tool: a stateless run on a text task, returning the final text."""
    from langchain_core.runnables import RunnableConfig
    from langchain_core.tools import StructuredTool

    async def run(task: str, config: RunnableConfig) -> str:
        # The tool's config propagates callbacks and limits; attribution survives it (S3).
        result = await agent.ainvoke({"messages": [{"role": "user", "content": task}]}, config)
        return str(result["messages"][-1].text)

    return StructuredTool.from_function(coroutine=run, name=name, description=description)
