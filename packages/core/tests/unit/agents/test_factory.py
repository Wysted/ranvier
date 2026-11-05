import asyncio
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from langchain_core.callbacks import AsyncCallbackHandler
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.tools import BaseTool, tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import START, MessagesState, StateGraph
from langgraph.types import Command, interrupt

from fake_models import fake_model
from graph_agent import as_tool
from graph_agent._internal.agents.factory import AgentBuildError, build_agent
from graph_agent._internal.agents.mcp import McpSourceError
from graph_agent._internal.agents.spec import AgentSpec
from graph_agent._internal.agents.tools import ToolRegistry
from graph_agent._internal.config.manifest import LlmSettings, ToolSpec
from graph_agent._internal.config.settings import LimitsSettings
from graph_agent._internal.llm import LlmRegistry
from graph_agent._internal.observability.tracing import AGENT_METADATA_KEY

LIMIT_S = 0.05
LONGER_THAN_LIMIT_S = 0.3
SKILLS = Path(__file__).parent / "skills_fixtures"


@tool
async def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b


def _call(name: str, args: dict[str, Any], id_: str = "c1") -> AIMessage:
    return AIMessage(content="", tool_calls=[{"name": name, "args": args, "id": id_}])


def _models(model: BaseChatModel) -> Callable[[str], BaseChatModel]:
    return lambda _profile: model


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
        models=_models(model),
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
        models=_models(model),
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
    with pytest.raises(AgentBuildError, match=r"'add'.*tool object.*MCP server 'docs'"):
        await build_agent(
            spec,
            models=_models(fake_model()),
            registry=_registry(),
            mcp=_Mcp({"docs": [mcp_add]}),
        )


async def test_mcp_servers_without_source_and_unavailable_server() -> None:
    spec = AgentSpec(name="a", profile="p", mcp_servers=["docs"])
    models = _models(fake_model())
    with pytest.raises(AgentBuildError, match="no MCP source"):
        await build_agent(spec, models=models, registry=_registry())
    with pytest.raises(McpSourceError):
        await build_agent(spec, models=models, registry=_registry(), mcp=_Mcp({}))


async def test_skills_middleware_end_to_end() -> None:
    model = fake_model(_call("load_skill", {"name": "code-review"}), AIMessage(content="ok"))
    agent = await build_agent(
        AgentSpec(name="a", profile="p", skills=[SKILLS], system_prompt="Be brief."),
        models=_models(model),
        registry=_registry(),
    )
    result = await agent.ainvoke({"messages": [("user", "review")]})
    assert "Check tests." in _texts(result)  # load_skill output reached the loop


async def test_as_tool_round_trip() -> None:
    sub = await build_agent(
        AgentSpec(name="sub", profile="p"),
        models=_models(fake_model(AIMessage(content="sub answer"))),
        registry=_registry(),
    )
    sub_tool = as_tool(sub, name="ask_sub", description="Ask the sub agent.")
    boss = await build_agent(
        AgentSpec(name="boss", profile="p", tools=[sub_tool]),
        models=_models(
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


async def test_duplicate_with_middleware_tool_names_both_sources() -> None:
    @tool("load_skill")
    async def mine(name: str) -> str:
        """Clashes with the skills middleware."""
        return name

    spec = AgentSpec(name="a", profile="p", tools=[mine], skills=[SKILLS])
    with pytest.raises(
        AgentBuildError, match=r"'load_skill'.*tool object.*middleware SkillsMiddleware"
    ):
        await build_agent(spec, models=_models(fake_model()), registry=_registry())


async def test_duplicate_names_label_registry_tools() -> None:
    reg = ToolRegistry({"dup": ToolSpec(target="xml.dom.minidom:parseString")}, LimitsSettings())
    reg.get = lambda name: add  # type: ignore[method-assign]  # shortcut: avoid a real target
    spec = AgentSpec(name="a", profile="p", tools=["dup", add])
    with pytest.raises(AgentBuildError, match=r"'add'.*tools\.dup.*tool object"):
        await build_agent(spec, models=_models(fake_model()), registry=reg)


@pytest.fixture
def tools_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "ga_factory_tools.py").write_text(
        "from langchain_core.tools import tool\n\n@tool\nasync def mul(a: int, b: int) -> int:\n"
        '    """Multiply."""\n    return a * b\n'
    )
    monkeypatch.syspath_prepend(tmp_path)


@pytest.mark.usefixtures("tools_module")
async def test_manifest_tool_resolves_off_loop_under_blockbuster() -> None:
    # The first resolution imports the target module from disk: blocking I/O if done on the loop.
    reg = ToolRegistry({"mul": ToolSpec(target="ga_factory_tools:mul")}, LimitsSettings())
    model = fake_model(_call("mul", {"a": 2, "b": 3}), AIMessage(content="6"))
    agent = await build_agent(
        AgentSpec(name="a", profile="p", tools=["mul"]), models=_models(model), registry=reg
    )
    result = await agent.ainvoke({"messages": [("user", "go")]})
    assert _texts(result)[-2:] == ["6", "6"]


async def test_concurrent_first_resolution_wraps_once() -> None:
    reg = ToolRegistry({"t": ToolSpec(target=f"{__name__}:add")}, LimitsSettings())
    first, second = await asyncio.gather(
        asyncio.to_thread(reg.resolve, ["t"]), asyncio.to_thread(reg.resolve, ["t"])
    )
    assert first[0] is second[0]


@tool
async def approve(question: str) -> str:
    """Ask a human."""
    return "human said " + str(interrupt({"q": question}))


async def test_subagent_node_interrupt_surfaces_and_resumes() -> None:
    sub = await build_agent(
        AgentSpec(name="sub", profile="p", tools=[approve]),
        models=_models(
            fake_model(_call("approve", {"question": "ok?"}), AIMessage(content="sub final"))
        ),
        registry=_registry(),
    )
    graph = StateGraph(MessagesState)
    graph.add_node("sub", sub)
    graph.add_edge(START, "sub")
    app: Any = graph.compile(checkpointer=InMemorySaver())
    cfg = {"configurable": {"thread_id": "T"}}
    result = await app.ainvoke({"messages": [("user", "go")]}, cfg)
    assert result["__interrupt__"][0].value == {"q": "ok?"}
    state = await app.aget_state(cfg, subgraphs=True)
    assert [t.name for t in state.tasks] == ["sub"]
    assert state.tasks[0].state is not None  # the subagent is inspectable
    result = await app.ainvoke(Command(resume="yes"), cfg)
    assert _texts(result)[-2:] == ["human said yes", "sub final"]


async def test_as_tool_is_a_checkpointed_subgraph_with_isolated_parallel_calls() -> None:
    sub = await build_agent(
        AgentSpec(name="sub", profile="p"),
        models=_models(fake_model(AIMessage(content="ans1"), AIMessage(content="ans2"))),
        registry=_registry(),
    )
    sub_tool = as_tool(sub, name="ask_sub", description="Ask.")
    parallel = AIMessage(
        content="",
        tool_calls=[
            {"name": "ask_sub", "args": {"task": "a"}, "id": "c1"},
            {"name": "ask_sub", "args": {"task": "b"}, "id": "c2"},
        ],
    )
    boss = await build_agent(
        AgentSpec(name="boss", profile="p", tools=[sub_tool]),
        models=_models(fake_model(parallel, AIMessage(content="boss done"))),
        registry=_registry(),
    )
    graph = StateGraph(MessagesState)
    graph.add_node("boss", boss)
    graph.add_edge(START, "boss")
    saver = InMemorySaver()
    app: Any = graph.compile(checkpointer=saver)
    result = await app.ainvoke(
        {"messages": [("user", "go")]},
        {"configurable": {"thread_id": "T"}},
    )
    answers = {m.tool_call_id: m.text for m in result["messages"] if isinstance(m, ToolMessage)}
    assert sorted(answers.values()) == ["ans1", "ans2"]
    namespaces = {c.config["configurable"]["checkpoint_ns"] async for c in saver.alist(None)}
    # the subagent checkpoints under the parent's tools-node namespace, one per call
    assert len({ns for ns in namespaces if "|tools:" in ns}) == 2


async def test_agent_tool_is_not_cut_by_the_default_tool_timeout() -> None:
    @tool
    async def slow_lookup(q: str) -> str:
        """Takes longer than the default tool timeout."""
        await asyncio.sleep(LONGER_THAN_LIMIT_S)
        return "found"

    sub = await build_agent(
        AgentSpec(name="sub", profile="p", tools=[slow_lookup]),
        models=_models(fake_model(_call("slow_lookup", {"q": "x"}), AIMessage(content="slow ok"))),
        registry=_registry(),
    )
    reg = ToolRegistry({}, LimitsSettings(tool_timeout_s=LIMIT_S))
    (unbounded,) = reg.resolve([as_tool(sub, name="ask_sub", description="Ask.")])
    call = {"type": "tool_call", "name": "ask_sub", "args": {"task": "t"}, "id": "1"}
    out = await unbounded.ainvoke(call)
    assert out.status == "success"
    assert out.content == "slow ok"
    (bounded,) = reg.resolve([as_tool(sub, name="ask_sub", description="Ask.", timeout_s=LIMIT_S)])
    assert bounded.timeout_s == LIMIT_S  # type: ignore[attr-defined]


def test_model_copy_keeps_the_shared_http_client(monkeypatch: pytest.MonkeyPatch) -> None:
    # build_agent tags the model through model_copy: the provider's pooled client must survive it.
    monkeypatch.setenv("FAKE_KEY", "sk-fake")
    keyed = {"api_key_env": "FAKE_KEY"}
    reg = LlmRegistry(
        LlmSettings.model_validate(
            {
                "profiles": {
                    "oa": {"provider": "openai", "model": "gpt-x"} | keyed,
                    "an": {"provider": "anthropic", "model": "claude-x"} | keyed,
                    "ol": {"provider": "ollama", "model": "llama"},
                }
            }
        )
    )

    def client(profile: str, m: Any) -> Any:
        if profile == "oa":
            return m.root_async_client._client
        return m._async_client._client if profile == "an" else m._async_client._client._transport

    for profile in ("oa", "an", "ol"):
        original = reg.model(profile)
        tagged = original.model_copy(update={"metadata": {AGENT_METADATA_KEY: "x"}})
        assert client(profile, tagged) is client(profile, original)
        assert AGENT_METADATA_KEY not in (original.metadata or {})  # the shared model is untouched
