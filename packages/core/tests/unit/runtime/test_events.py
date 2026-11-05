import operator
from typing import Annotated, Any, TypedDict

import pytest
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.config import get_stream_writer
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from graph_agent._internal.runtime.events import (
    CustomEvent,
    InterruptEvent,
    InterruptItem,
    MessagesEvent,
    RunEvent,
    UpdatesEvent,
    ValuesEvent,
    map_stream_part,
)


class _State(TypedDict, total=False):
    x: Any
    msgs: list[Any]
    log: Annotated[list[str], operator.add]


def _echo(state: _State) -> _State:
    get_stream_writer()({"progress": 1})
    return {"x": 1}


def _ask(state: _State) -> _State:
    return {"x": interrupt("name?")}


async def _llm(state: _State) -> _State:
    model = GenericFakeChatModel(messages=iter([AIMessage(content="hi there")]))
    return {"msgs": [await model.ainvoke([HumanMessage("yo")])]}


async def _collect(graph: Any, input: Any, config: Any = None) -> list[RunEvent | None]:
    modes = ["updates", "values", "messages", "custom"]
    return [
        map_stream_part(p)
        async for p in graph.astream(input, config, stream_mode=modes, subgraphs=True, version="v2")
    ]


async def test_updates_values_custom() -> None:
    graph = (
        StateGraph(_State)
        .add_node("echo", _echo)
        .add_edge(START, "echo")
        .add_edge("echo", END)
        .compile()
    )
    events = await _collect(graph, {"x": 0})
    assert UpdatesEvent((), "echo", {"x": 1}) in events
    assert ValuesEvent((), {"x": 1, "log": []}) in events
    assert CustomEvent((), None, {"progress": 1}) in events


async def test_messages_are_llm_chunks() -> None:
    graph = (
        StateGraph(_State)
        .add_node("llm", _llm)
        .add_edge(START, "llm")
        .add_edge("llm", END)
        .compile()
    )
    events = [e for e in await _collect(graph, {"x": 0}) if isinstance(e, MessagesEvent)]
    assert events
    assert all(e.node == "llm" and e.type == "AIMessageChunk" for e in events)
    assert "".join(str(e.content) for e in events) == "hi there"


async def test_interrupt_maps_once_from_updates() -> None:
    graph = (
        StateGraph(_State)
        .add_node("ask", _ask)
        .add_edge(START, "ask")
        .add_edge("ask", END)
        .compile(checkpointer=InMemorySaver())
    )
    config = {"configurable": {"thread_id": "t"}}
    events = await _collect(graph, {"x": 0}, config)
    interrupts = [e for e in events if isinstance(e, InterruptEvent)]
    assert len(interrupts) == 1
    (item,) = interrupts[0].interrupts
    assert item == InterruptItem(item.id, "name?")
    assert item.id
    resumed = await _collect(graph, Command(resume="bob"), config)
    assert UpdatesEvent((), "ask", {"x": "bob"}) in resumed


def test_unknown_type_raises() -> None:
    with pytest.raises(ValueError, match="unknown stream part"):
        map_stream_part({"type": "tasks", "ns": (), "data": {}})


async def test_parallel_resume_strips_cache_metadata() -> None:
    graph = (
        StateGraph(_State)
        .add_node("ask", _ask)
        .add_node("b", lambda s: {"log": ["b"]})
        .add_edge(START, "ask")
        .add_edge(START, "b")
        .add_edge("ask", END)
        .add_edge("b", END)
        .compile(checkpointer=InMemorySaver())
    )
    config = {"configurable": {"thread_id": "p"}}
    first = await _collect(graph, {"log": []}, config)
    assert UpdatesEvent((), "b", {"log": ["b"]}) in first
    resumed = await _collect(graph, Command(resume="bob"), config)
    assert UpdatesEvent((), "ask", {"x": "bob"}) in resumed


async def test_parallel_interrupts_one_event_each() -> None:
    graph = (
        StateGraph(_State)
        .add_node("ask", _ask)
        .add_node("ask2", _ask)
        .add_edge(START, "ask")
        .add_edge(START, "ask2")
        .add_edge("ask", END)
        .add_edge("ask2", END)
        .compile(checkpointer=InMemorySaver())
    )
    events = await _collect(graph, {"x": 0}, {"configurable": {"thread_id": "q"}})
    interrupts = [e for e in events if isinstance(e, InterruptEvent)]
    assert sum(len(e.interrupts) for e in interrupts) == 2


def _subgraph(node: Any) -> Any:
    sub = StateGraph(_State).add_node("inner", node).add_edge(START, "inner").add_edge("inner", END)
    return (
        StateGraph(_State)
        .add_node("sub", sub.compile())
        .add_edge(START, "sub")
        .add_edge("sub", END)
        .compile(checkpointer=InMemorySaver())
    )


async def test_subgraph_interrupt_mapped_only_at_root() -> None:
    graph = _subgraph(_ask)
    config = {"configurable": {"thread_id": "s"}}
    events = await _collect(graph, {"x": 0}, config)
    assert len([e for e in events if isinstance(e, InterruptEvent)]) == 1
    resumed = await _collect(graph, Command(resume="bob"), config)
    assert not any(isinstance(e, InterruptEvent) for e in resumed)


async def test_subgraph_ns_propagates() -> None:
    events = await _collect(_subgraph(_echo), {"x": 0}, {"configurable": {"thread_id": "n"}})
    nested = [e for e in events if isinstance(e, UpdatesEvent) and e.node == "inner"]
    (update,) = nested
    assert len(update.ns) == 1
    assert update.ns[0].startswith("sub:")
    (custom,) = [e for e in events if isinstance(e, CustomEvent)]
    assert custom.ns == update.ns
    assert custom.node == "sub"


async def test_non_streamed_llm_message_mapped_node_messages_dropped() -> None:
    async def node(state: _State) -> _State:
        model = GenericFakeChatModel(
            messages=iter([AIMessage(content="whole")]), disable_streaming=True
        )
        reply = await model.ainvoke([HumanMessage("yo")])
        return {"msgs": [reply, ToolMessage("tool out", tool_call_id="1")]}

    graph = StateGraph(_State).add_node("llm", node).add_edge(START, "llm").add_edge("llm", END)
    events = await _collect(graph.compile(), {"x": 0})
    (msg,) = [e for e in events if isinstance(e, MessagesEvent)]
    assert (msg.node, msg.content, msg.tool_call_chunks) == ("llm", "whole", ())
