import uuid
from collections.abc import AsyncIterator

import psycopg
import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.errors import GraphDrained
from langgraph.runtime import RunControl
from langgraph.types import Durability

from drain_graph import build_drain_graph
from graph_agent._internal.db.pool import CheckpointerPool, open_checkpointer_pool
from graph_agent._internal.runtime.checkpointer import create_checkpointer, setup_checkpointer

POOL_MAX_SIZE = 3


@pytest.fixture
async def pool(clean_database: str) -> AsyncIterator[CheckpointerPool]:
    with psycopg.connect(clean_database, autocommit=True) as conn:
        conn.execute("CREATE SCHEMA graph_agent")  # `migrate` creates it before the saver setup
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


@pytest.mark.parametrize("durability", ["sync", "async", "exit"])
async def test_run_id_round_trip_and_drain_resume(
    pool: CheckpointerPool, durability: Durability
) -> None:
    """S1: `metadata.run_id` survives on AsyncPostgresSaver and a drained run resumes with None."""
    await setup_checkpointer(pool)
    saver = create_checkpointer(pool)
    control = RunControl()
    calls: list[str] = []
    graph = build_drain_graph(saver, control, calls)
    thread, run_id = str(uuid.uuid4()), str(uuid.uuid4())
    config: RunnableConfig = {"configurable": {"thread_id": thread}, "metadata": {"run_id": run_id}}

    with pytest.raises(GraphDrained):
        await graph.ainvoke({"trace": []}, config, control=control, durability=durability)
    assert calls == ["first"]
    tup = await saver.aget_tuple({"configurable": {"thread_id": thread}})
    assert tup is not None
    assert tup.metadata.get("run_id") == run_id

    result = await graph.ainvoke(None, config, control=RunControl(), durability=durability)
    assert result["trace"] == ["first", "second"]
    assert calls == ["first", "second"]


async def test_pool_merges_url_options_with_the_pinned_search_path(clean_database: str) -> None:
    sep = "&" if "?" in clean_database else "?"
    url = f"{clean_database}{sep}options=-c%20statement_timeout%3D12345"
    pool = await open_checkpointer_pool(url, max_size=1)
    try:
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT current_setting('search_path') AS sp, "
                "current_setting('statement_timeout') AS st"
            )
            row = await cur.fetchone()
    finally:
        await pool.close()
    assert row == {"sp": "graph_agent", "st": "12345ms"}
