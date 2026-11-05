"""The public test kit, exercised through the real runner mapping."""

from typing import Any, TypedDict

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import tool
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.types import interrupt

from graph_agent import AgentSpec, FlowContext
from graph_agent._internal.runtime.events import EndEvent, InterruptEvent
from graph_agent.testing import FakeChatModel, flow_context, run_flow


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


async def test_resume_by_id_map_and_errors() -> None:
    ctx = flow_context()
    with pytest.raises(ValueError, match="no pending interrupt"):
        await run_flow(_asking(), ctx=ctx, thread_id="none", resume="x")

    first = await run_flow(_asking(), {}, ctx=ctx, thread_id="t2")
    result = await run_flow(
        _asking(), ctx=ctx, thread_id="t2", resume={first.interrupts[0].id: "bob"}
    )
    assert result.values == {"answer": "bob"}


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
    with pytest.raises(ValueError, match="map of interrupt id"):
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
