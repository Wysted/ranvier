import asyncio
import sys
import types
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from typing import Any, TypedDict

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from psycopg_pool import AsyncConnectionPool

from graph_agent._internal.config.manifest import FlowSpec
from graph_agent._internal.config.settings import RunsSettings, Settings
from graph_agent._internal.db.migrate import apply_migrations
from graph_agent._internal.db.pool import open_app_pool
from graph_agent._internal.runtime.checkpointer import Checkpointer
from graph_agent._internal.runtime.events import EndEvent, ErrorEvent, RunEvent, RunObserver
from graph_agent._internal.runtime.flows import FlowRegistry
from graph_agent._internal.runtime.runner import RunQueue, execute_run
from graph_agent._internal.runtime.runs import ErrorCode, RunError, RunsRepository, RunStatus

MODULE = "t16_integration_flows"
FLOW_VERSION = 1
SETTINGS = Settings(_env_file=None, runs=RunsSettings(heartbeat_s=0.05, lease_s=1.0))
POLL_S = 0.01
WAIT_S = 5.0


class State(TypedDict, total=False):
    x: int


async def _quick(state: State) -> State:
    return {"x": 1}


async def _slow(state: State) -> State:
    await asyncio.sleep(30)
    return {}


def _builder(fn: Any) -> StateGraph[State, Any, State, State]:
    return StateGraph(State).add_node("n", fn).add_edge(START, "n").add_edge("n", END)


@pytest.fixture
def user_module() -> Iterator[None]:
    mod = types.ModuleType(MODULE)
    mod.quick = _builder(_quick)  # type: ignore[attr-defined]
    mod.slow = _builder(_slow)  # type: ignore[attr-defined]
    sys.modules[MODULE] = mod
    yield
    del sys.modules[MODULE]


@pytest.fixture
async def pool(clean_database: str) -> AsyncIterator[AsyncConnectionPool]:
    pool = await open_app_pool(clean_database, max_size=4)
    try:
        await apply_migrations(pool)
        yield pool
    finally:
        await pool.close()


@dataclass
class Resources:
    settings: Settings
    runs: RunsRepository | None
    flows: FlowRegistry
    checkpointer: Checkpointer
    observers: list[RunObserver] = field(default_factory=list)
    queue: RunQueue | None = None


@pytest.fixture
def res(pool: AsyncConnectionPool, user_module: None) -> Resources:
    saver = InMemorySaver()
    flows = {n: FlowSpec(target=f"{MODULE}:{n}", version=FLOW_VERSION) for n in ("quick", "slow")}
    ctx_factory: Any = lambda name: object()  # noqa: E731
    return Resources(SETTINGS, RunsRepository(pool), FlowRegistry(flows, saver, ctx_factory), saver)


async def _events(res: Resources, run_id: uuid.UUID) -> list[RunEvent]:
    return [e async for e in execute_run(res, run_id)]


async def _new(res: Resources, flow: str, thread: uuid.UUID | None = None) -> uuid.UUID:
    assert res.runs is not None
    run = await res.runs.create(
        thread_id=thread or uuid.uuid4(), flow=flow, flow_version=FLOW_VERSION, input={}
    )
    return run.run_id


async def _status(pool: AsyncConnectionPool, run_id: uuid.UUID) -> str | None:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT status FROM graph_agent.runs WHERE run_id = %s", (run_id,))
        row = await cur.fetchone()
        return None if row is None else str(row[0])


async def _until_running(pool: AsyncConnectionPool, run_id: uuid.UUID) -> None:
    async with asyncio.timeout(WAIT_S):
        while await _status(pool, run_id) != RunStatus.RUNNING:  # noqa: ASYNC110 (polls a DB row)
            await asyncio.sleep(POLL_S)


async def test_success_and_second_execution_is_a_noop(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    run_id = await _new(res, "quick")
    events = await _events(res, run_id)
    assert isinstance(events[-1], EndEvent) and events[-1].status == "success"
    assert await _status(pool, run_id) == RunStatus.SUCCESS
    assert await _events(res, run_id) == []  # claim lost: terminal runs are not claimable


async def test_cancel_flag_is_seen_by_the_heartbeat(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    assert res.runs is not None
    thread = uuid.uuid4()
    run_id = await _new(res, "slow", thread)
    task = asyncio.create_task(_events(res, run_id))
    await _until_running(pool, run_id)
    assert await res.runs.request_cancel(run_id, thread) is RunStatus.RUNNING
    events = await asyncio.wait_for(task, timeout=WAIT_S)
    assert isinstance(events[-1], EndEvent) and events[-1].status == "cancelled"
    assert await _status(pool, run_id) == RunStatus.CANCELLED


async def test_lost_lease_writes_no_terminal_state(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    assert res.runs is not None
    run_id = await _new(res, "slow")
    task = asyncio.create_task(_events(res, run_id))
    await _until_running(pool, run_id)
    # Another executor takes over: expire the lease and claim it.
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE graph_agent.runs SET lease_expires_at = now() - interval '1 second' "
            "WHERE run_id = %s",
            (run_id,),
        )
    taken = await res.runs.claim(run_id, "other", 60.0, 3)
    assert taken.token is not None
    events = await asyncio.wait_for(task, timeout=WAIT_S)
    assert isinstance(events[-1], ErrorEvent) and events[-1].retryable
    assert await _status(pool, run_id) == RunStatus.RUNNING  # still owned by the new executor
    assert await res.runs.finish(run_id, taken.token, RunStatus.SUCCESS)


async def test_second_run_on_a_busy_thread_raises(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    thread = uuid.uuid4()
    first, second = await _new(res, "slow", thread), await _new(res, "quick", thread)
    task = asyncio.create_task(_events(res, first))
    await _until_running(pool, first)
    with pytest.raises(RunError) as ei:
        await _events(res, second)
    assert ei.value.code is ErrorCode.THREAD_BUSY
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
