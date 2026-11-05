import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from langchain_core.callbacks import AsyncCallbackHandler
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.tools import BaseTool, tool

from fake_models import fake_model
from graph_agent import as_tool
from graph_agent._internal.agents.factory import AGENT_METADATA_KEY, AgentBuildError, build_agent
from graph_agent._internal.agents.mcp import McpSourceError
from graph_agent._internal.agents.spec import AgentSpec
from graph_agent._internal.agents.tools import ToolRegistry
from graph_agent._internal.config.settings import LimitsSettings

SKILLS = Path(__file__).parent / "skills_fixtures"


@tool
async def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b


def _call(name: str, args: dict[str, Any], id_: str = "c1") -> AIMessage:
    return AIMessage(content="", tool_calls=[{"name": name, "args": args, "id": id_}])


class _Models:
    def __init__(self, model: BaseChatModel) -> None:
        self._model = model

    def model(self, profile: str) -> BaseChatModel:
        return self._model


class _Mcp:
    def __init__(self, servers: dict[str, list[BaseTool]]) -> None:
        self._servers = servers

    async def tools(self, server: str) -> Sequence[BaseTool]:
        if server not in self._servers:
            raise McpSourceError(f"unknown MCP server {server!r}")
        return self._servers[server]


def _registry() -> ToolRegistry:
    return ToolRegistry({}, LimitsSettings())


class _Names(AsyncCallbackHandler):
    def __init__(self) -> None:
        self.agents: list[Any] = []

    async def on_chat_model_start(
        self,
        serialized: dict[str, Any],
        messages: list[list[BaseMessage]],
        *,
        run_id: UUID,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.agents.append((metadata or {}).get(AGENT_METADATA_KEY))


def _texts(result: dict[str, Any]) -> list[str]:
    return [m.text for m in result["messages"]]


async def test_agent_calls_registry_tool_and_tags_name() -> None:
    model = fake_model(_call("add", {"a": 1, "b": 2}), AIMessage(content="3 it is"))
    agent = await build_agent(
        AgentSpec(name="calc", profile="p", tools=[add], system_prompt="Be brief."),
        models=_Models(model),
        registry=_registry(),
    )
    names = _Names()
    result = await agent.ainvoke({"messages": [("user", "1+2?")]}, {"callbacks": [names]})
    assert _texts(result)[-2:] == ["3", "3 it is"]  # tool output, then the final answer
    assert names.agents == ["calc", "calc"]


async def test_mcp_tools_are_available() -> None:
    @tool
    async def search(q: str) -> str:
        """Search."""
        return f"hit:{q}"

    model = fake_model(_call("search", {"q": "x"}), AIMessage(content="done"))
    agent = await build_agent(
        AgentSpec(name="a", profile="p", mcp_servers=["docs"]),
        models=_Models(model),
        registry=_registry(),
        mcp=_Mcp({"docs": [search]}),
    )
    result = await agent.ainvoke({"messages": [("user", "go")]})
    assert "hit:x" in _texts(result)


async def test_duplicate_tool_names_name_both_sources() -> None:
    @tool("add")
    async def mcp_add(a: int, b: int) -> int:
        """Add via MCP."""
        return a + b

    spec = AgentSpec(name="a", profile="p", tools=[add], mcp_servers=["docs"])
    with pytest.raises(AgentBuildError, match=r"'add'.*tool registry.*MCP server 'docs'"):
        await build_agent(
            spec,
            models=_Models(fake_model()),
            registry=_registry(),
            mcp=_Mcp({"docs": [mcp_add]}),
        )


async def test_mcp_servers_without_source_and_unavailable_server() -> None:
    spec = AgentSpec(name="a", profile="p", mcp_servers=["docs"])
    models = _Models(fake_model())
    with pytest.raises(AgentBuildError, match="no MCP source"):
        await build_agent(spec, models=models, registry=_registry())
    with pytest.raises(McpSourceError):
        await build_agent(spec, models=models, registry=_registry(), mcp=_Mcp({}))


async def test_skills_middleware_end_to_end() -> None:
    model = fake_model(_call("load_skill", {"name": "code-review"}), AIMessage(content="ok"))
    agent = await build_agent(
        AgentSpec(name="a", profile="p", skills=[SKILLS], system_prompt="Be brief."),
        models=_Models(model),
        registry=_registry(),
    )
    result = await agent.ainvoke({"messages": [("user", "review")]})
    assert "Check tests." in _texts(result)  # load_skill output reached the loop


async def test_as_tool_round_trip() -> None:
    sub = await build_agent(
        AgentSpec(name="sub", profile="p"),
        models=_Models(fake_model(AIMessage(content="sub answer"))),
        registry=_registry(),
    )
    sub_tool = as_tool(sub, name="ask_sub", description="Ask the sub agent.")
    boss = await build_agent(
        AgentSpec(name="boss", profile="p", tools=[sub_tool]),
        models=_Models(
            fake_model(_call("ask_sub", {"task": "hello"}), AIMessage(content="boss done"))
        ),
        registry=_registry(),
    )
    names = _Names()
    result = await boss.ainvoke({"messages": [("user", "go")]}, {"callbacks": [names]})
    assert _texts(result)[-2:] == ["sub answer", "boss done"]
    assert names.agents == ["boss", "sub", "boss"]  # the innermost agent wins


def test_facade_does_not_import_langchain() -> None:
    code = (
        "import sys, graph_agent; "
        "sys.exit(any(m.startswith(('langchain', 'langgraph')) for m in sys.modules))"
    )
    assert subprocess.run([sys.executable, "-c", code], check=False).returncode == 0
