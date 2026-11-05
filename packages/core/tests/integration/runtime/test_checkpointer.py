import uuid
from collections.abc import AsyncIterator
from typing import TypedDict

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.errors import GraphDrained
from langgraph.graph import END, START, StateGraph
from langgraph.runtime import RunControl

from graph_agent._internal.db.pool import open_checkpointer_pool
from graph_agent._internal.runtime.checkpointer import (
    CheckpointerPool,
    create_checkpointer,
    setup_checkpointer,
)

POOL_MAX_SIZE = 3


class State(TypedDict):
    trace: list[str]


@pytest.fixture
async def pool(clean_database: str) -> AsyncIterator[CheckpointerPool]:
    pool = await open_checkpointer_pool(clean_database, max_size=POOL_MAX_SIZE)
    try:
        yield pool
    finally:
        await pool.close()


async def test_setup_twice_then_put_get(pool: CheckpointerPool) -> None:
    await setup_checkpointer(pool)
    await setup_checkpointer(pool)
    saver = create_checkpointer(pool)
    checkpoint = empty_checkpoint()
    config: RunnableConfig = {"configurable": {"thread_id": str(uuid.uuid4()), "checkpoint_ns": ""}}
    saved = await saver.aput(config, checkpoint, {"source": "input", "step": 0}, {})
    tup = await saver.aget_tuple(saved)
    assert tup is not None
    assert tup.checkpoint["id"] == checkpoint["id"]


async def test_run_id_round_trip_and_drain_resume(pool: CheckpointerPool) -> None:
    """S1: `metadata.run_id` survives on AsyncPostgresSaver and a drained run resumes with None."""
    await setup_checkpointer(pool)
    saver = create_checkpointer(pool)
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
    graph = builder.compile(checkpointer=saver)
    thread, run_id = str(uuid.uuid4()), str(uuid.uuid4())
    config: RunnableConfig = {"configurable": {"thread_id": thread}, "metadata": {"run_id": run_id}}

    with pytest.raises(GraphDrained):
        await graph.ainvoke({"trace": []}, config, control=control)
    tup = await saver.aget_tuple({"configurable": {"thread_id": thread}})
    assert tup is not None
    assert tup.metadata.get("run_id") == run_id

    result = await graph.ainvoke(None, config, control=RunControl())
    assert result["trace"] == ["first", "second"]
    assert calls == ["first", "second"]
