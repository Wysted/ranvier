import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.postgres import _ainternal
from langgraph.errors import GraphDrained
from langgraph.runtime import RunControl
from psycopg_pool import AsyncConnectionPool

from drain_graph import build_drain_graph
from graph_agent._internal.db.pool import CheckpointerPool
from graph_agent._internal.runtime.checkpointer import (
    create_checkpointer,
    create_memory_checkpointer,
)

POOL_MAX = 3
CALLS = 10


async def test_saver_calls_overlap_up_to_pool_max(monkeypatch: pytest.MonkeyPatch) -> None:
    # Unopened real pool: only `isinstance` and `max_size` are used; connections are faked below.
    pool = cast(
        CheckpointerPool,
        AsyncConnectionPool("dbname=unused", min_size=1, max_size=POOL_MAX, open=False),
    )
    running = peak = 0

    @asynccontextmanager
    async def fake_connection(_conn: object) -> AsyncIterator[MagicMock]:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        try:
            await asyncio.sleep(0.01)
            conn = MagicMock()
            cur = MagicMock()
            cur.execute = MagicMock(side_effect=lambda *a, **k: asyncio.sleep(0))
            cur.fetchone = MagicMock(side_effect=lambda: asyncio.sleep(0, None))

            @asynccontextmanager
            async def cursor(*_a: Any, **_k: Any) -> AsyncIterator[MagicMock]:
                yield cur

            conn.cursor = cursor
            yield conn
        finally:
            running -= 1

    monkeypatch.setattr(_ainternal, "get_connection", fake_connection)
    saver = create_checkpointer(pool)
    config: RunnableConfig = {"configurable": {"thread_id": "t"}}
    await asyncio.gather(*(saver.aget_tuple(config) for _ in range(CALLS)))
    assert 1 < peak <= POOL_MAX


async def test_run_id_metadata_and_drain_resume_in_memory() -> None:
    """Same S1 contract the Postgres integration test checks, against the in-memory saver."""
    control = RunControl()
    calls: list[str] = []
    saver = create_memory_checkpointer()
    graph = build_drain_graph(saver, control, calls)
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
