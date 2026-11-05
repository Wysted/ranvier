from typing import TypedDict, cast
from unittest.mock import MagicMock

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.errors import GraphDrained
from langgraph.graph import END, START, StateGraph
from langgraph.runtime import RunControl

from graph_agent._internal.runtime.checkpointer import (
    CheckpointerPool,
    create_checkpointer,
    create_memory_checkpointer,
)


class State(TypedDict):
    trace: list[str]


def test_create_memory_checkpointer() -> None:
    assert isinstance(create_memory_checkpointer(), InMemorySaver)


async def test_create_checkpointer_binds_pool() -> None:
    # Upstream binds the running loop in __init__, so it must be built inside a coroutine.
    pool = cast(CheckpointerPool, MagicMock())
    saver = create_checkpointer(pool)
    assert isinstance(saver, AsyncPostgresSaver)
    assert saver.conn is pool


async def test_run_id_metadata_and_drain_resume_in_memory() -> None:
    """Same S1 contract the Postgres integration test checks, against the in-memory saver."""
    control = RunControl()
    calls: list[str] = []

    def first(state: State) -> State:
        calls.append("first")
        control.request_drain()
        return {"trace": ["first"]}

    def second(state: State) -> State:
        calls.append("second")
        return {"trace": [*state["trace"], "second"]}

    builder = StateGraph(State)
    builder.add_node("first", first)
    builder.add_node("second", second)
    builder.add_edge(START, "first")
    builder.add_edge("first", "second")
    builder.add_edge("second", END)
    saver = create_memory_checkpointer()
    graph = builder.compile(checkpointer=saver)
    config: RunnableConfig = {"configurable": {"thread_id": "t1"}, "metadata": {"run_id": "r1"}}

    with pytest.raises(GraphDrained):
        await graph.ainvoke({"trace": []}, config, control=control)
    assert calls == ["first"]
    tup = await saver.aget_tuple({"configurable": {"thread_id": "t1"}})
    assert tup is not None
    assert tup.metadata.get("run_id") == "r1"

    result = await graph.ainvoke(None, config, control=RunControl())
    assert result["trace"] == ["first", "second"]
    assert calls == ["first", "second"]
