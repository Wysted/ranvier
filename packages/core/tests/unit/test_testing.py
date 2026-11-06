"""The public test kit, exercised through the real runner mapping."""

import threading
from collections.abc import Sequence
from typing import Any, TypedDict

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import BaseTool, tool
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.types import interrupt

from ranvier import AgentSpec, FlowContext
from ranvier.testing import EndEvent, FakeChatModel, InterruptEvent, flow_context, run_flow


class _State(TypedDict, total=False):
    n: int
    answer: str
    a: str
    b: str


def _plain() -> StateGraph[Any, Any, Any, Any]:
    b = StateGraph(_State)
    b.add_node("double", lambda s: {"n": s["n"] * 2})
    b.add_edge(START, "double")
    b.add_edge("double", END)
    return b


def _asking() -> StateGraph[Any, Any, Any, Any]:
    b = StateGraph(_State)
    b.add_node("ask", lambda s: {"answer": interrupt("name?")})
    b.add_edge(START, "ask")
    b.add_edge("ask", END)
    return b


async def test_plain_flow_and_factories() -> None:
    result = await run_flow(_plain(), {"n": 2})
    assert result.values == {"n": 4}
    assert result.interrupts == ()
    assert isinstance(result.events[-1], EndEvent)

    async def afactory(ctx: FlowContext) -> StateGraph[Any, Any, Any, Any]:
        return _plain()

    assert (await run_flow(afactory, {"n": 3})).values == {"n": 6}
    assert (await run_flow(lambda ctx: _plain(), {"n": 4})).values == {"n": 8}


async def test_interrupt_resume_shares_the_thread() -> None:
    ctx = flow_context()
    first = await run_flow(_asking(), {}, ctx=ctx, thread_id="t1")
    assert [i.value for i in first.interrupts] == ["name?"]
    assert any(isinstance(e, InterruptEvent) for e in first.events)

    second = await run_flow(_asking(), ctx=ctx, thread_id="t1", resume="ada")
    assert second.values == {"answer": "ada"}
    assert second.interrupts == ()


async def test_input_on_an_interrupted_thread_is_rejected() -> None:
    ctx = flow_context()
    await run_flow(_asking(), {}, ctx=ctx, thread_id="t6")
    with pytest.raises(ValueError, match="pending interrupt"):
        await run_flow(_asking(), {"n": 1}, ctx=ctx, thread_id="t6")


async def test_resume_by_id_map_and_errors() -> None:
    ctx = flow_context()
    with pytest.raises(ValueError, match="exactly one pending interrupt"):
        await run_flow(_asking(), ctx=ctx, thread_id="none", resume="x")

    first = await run_flow(_asking(), {}, ctx=ctx, thread_id="t2")
    result = await run_flow(
        _asking(), ctx=ctx, thread_id="t2", resume={first.interrupts[0].id: "bob"}
    )
    assert result.values == {"answer": "bob"}


async def test_resume_map_with_empty_or_foreign_ids_is_rejected() -> None:
    ctx = flow_context()
    await run_flow(_asking(), {}, ctx=ctx, thread_id="t5")
    for bad in ({}, {"d" * 32: "x"}):
        with pytest.raises(ValueError, match="pending interrupt ids"):
            await run_flow(_asking(), ctx=ctx, thread_id="t5", resume=bad)


async def test_ambiguous_bare_resume_needs_the_map() -> None:
    def ask(label: str) -> Any:
        return lambda s: {label: interrupt(label)}

    b = StateGraph(_State)  # parallel branches: two interrupts pending at once
    b.add_node("a", ask("a"))
    b.add_node("b", ask("b"))
    b.add_edge(START, "a")
    b.add_edge(START, "b")
    ctx = flow_context()
    first = await run_flow(b, {}, ctx=ctx, thread_id="t3")
    assert len(first.interrupts) == 2
    with pytest.raises(ValueError, match="exactly one pending interrupt"):
        await run_flow(b, ctx=ctx, thread_id="t3", resume="x")
    done = await run_flow(
        b, ctx=ctx, thread_id="t3", resume={i.id: i.value.upper() for i in first.interrupts}
    )
    assert done.interrupts == () and done.values == {"a": "A", "b": "B"}


async def test_agent_with_scripted_tool_call() -> None:
    @tool
    async def add(a: int, b: int) -> int:
        """Add two numbers."""
        return a + b

    call = AIMessage(content="", tool_calls=[{"name": "add", "args": {"a": 2, "b": 3}, "id": "c1"}])
    model = FakeChatModel([call, "the sum is 5"])
    ctx = flow_context(models={"fast": model}, tools={"add": add})

    async def factory(c: FlowContext) -> StateGraph[Any, Any, Any, Any]:
        agent = await c.agent(AgentSpec(name="calc", profile="fast", tools=["add"]))
        b = StateGraph(MessagesState)
        b.add_node("calc", agent)
        b.add_edge(START, "calc")
        return b

    result = await run_flow(factory, {"messages": [HumanMessage("2+3?")]}, ctx=ctx)
    messages = result.values["messages"]
    assert messages[-1].content == "the sum is 5"
    assert [m.content for m in messages if m.type == "tool"] == ["5"]
    assert len(model.calls) == 2  # the tool result went back to the model


def test_missing_model_and_tool_fail_loudly() -> None:
    ctx = flow_context()
    with pytest.raises(KeyError, match="profile 'x'"):
        ctx.model("x")
    with pytest.raises(KeyError, match="no tool named"):
        ctx.tools("y")


async def test_sync_only_tool_runs_in_a_thread() -> None:
    loop_thread = threading.get_ident()

    @tool
    def where() -> int:
        """Report the thread id."""
        return threading.get_ident()

    call = AIMessage(content="", tool_calls=[{"name": "where", "args": {}, "id": "c1"}])
    ctx = flow_context(models={"fast": FakeChatModel([call, "done"])}, tools={"where": where})

    async def factory(c: FlowContext) -> StateGraph[Any, Any, Any, Any]:
        b = StateGraph(MessagesState)
        b.add_node("a", await c.agent(AgentSpec(name="a", profile="fast", tools=["where"])))
        b.add_edge(START, "a")
        return b

    result = await run_flow(factory, {"messages": [HumanMessage("go")]}, ctx=ctx)
    [ran_in] = [m.content for m in result.values["messages"] if m.type == "tool"]
    assert ran_in != str(loop_thread)


async def test_same_named_async_tool_in_spec_is_not_forced_to_a_thread() -> None:
    @tool("x")
    def sync_x() -> str:
        """Sync x."""
        return "sync"

    @tool("x")
    async def async_x() -> str:
        """Async x."""
        return "async"

    call = AIMessage(content="", tool_calls=[{"name": "x", "args": {}, "id": "c1"}])
    ctx = flow_context(models={"fast": FakeChatModel([call, "done"])}, tools={"x": sync_x})

    async def factory(c: FlowContext) -> StateGraph[Any, Any, Any, Any]:
        b = StateGraph(MessagesState)
        b.add_node("a", await c.agent(AgentSpec(name="a", profile="fast", tools=[async_x])))
        b.add_edge(START, "a")
        return b

    result = await run_flow(factory, {"messages": [HumanMessage("go")]}, ctx=ctx)
    assert [m.content for m in result.values["messages"] if m.type == "tool"] == ["async"]


async def test_agent_with_fake_mcp_source() -> None:
    @tool
    async def ping() -> str:
        """Ping."""
        return "pong"

    class FakeMcp:
        async def tools(self, server: str) -> Sequence[BaseTool]:
            assert server == "x"
            return [ping]

    call = AIMessage(content="", tool_calls=[{"name": "ping", "args": {}, "id": "c1"}])
    ctx = flow_context(models={"fast": FakeChatModel([call, "done"])}, mcp=FakeMcp())

    async def factory(c: FlowContext) -> StateGraph[Any, Any, Any, Any]:
        spec = AgentSpec(name="a", profile="fast", mcp_servers=["x"])
        b = StateGraph(MessagesState)
        b.add_node("a", await c.agent(spec))
        b.add_edge(START, "a")
        return b

    result = await run_flow(factory, {"messages": [HumanMessage("hi")]}, ctx=ctx)
    assert [m.content for m in result.values["messages"] if m.type == "tool"] == ["pong"]


async def test_resume_map_rules_and_input_xor_resume() -> None:
    ctx = flow_context()
    await run_flow(_asking(), {}, ctx=ctx, thread_id="t4")
    with pytest.raises(ValueError, match="pending interrupt ids"):
        await run_flow(_asking(), ctx=ctx, thread_id="t4", resume={})
    with pytest.raises(ValueError, match="pending interrupt ids"):
        await run_flow(_asking(), ctx=ctx, thread_id="t4", resume={"a" * 32: "x"})
    with pytest.raises(ValueError, match="input or resume"):
        await run_flow(_asking(), {}, ctx=ctx, thread_id="t4", resume="x")
    # a dict that is not an id map is a direct value for the single pending interrupt
    done = await run_flow(_asking(), ctx=ctx, thread_id="t4", resume={"first": "ada"})
    assert done.values == {"answer": {"first": "ada"}}


async def test_compiled_graph_target_gets_a_clear_error() -> None:
    compiled = _plain().compile()
    with pytest.raises(TypeError, match="compiled graph; export the uncompiled StateGraph"):
        await run_flow(compiled, {"n": 1})  # type: ignore[arg-type]
