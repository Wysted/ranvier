"""T24: deferral by name with the thread lock, and the `execute_run` job (S4 hints)."""

import asyncio
import sys
import types
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, TypedDict

import psycopg
import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from procrastinate.schema import SchemaManager
from psycopg_pool import AsyncConnectionPool
from pydantic import SecretStr

from graph_agent._internal.config.manifest import FlowSpec
from graph_agent._internal.config.settings import DatabaseSettings, RunsSettings, Settings
from graph_agent._internal.db.migrate import apply_migrations
from graph_agent._internal.db.pool import open_app_pool
from graph_agent._internal.queue import ProcrastinateRunQueue, make_app
from graph_agent._internal.runtime.checkpointer import Checkpointer
from graph_agent._internal.runtime.events import RunObserver
from graph_agent._internal.runtime.flows import FlowRegistry
from graph_agent._internal.runtime.runner import ActiveRuns, RunQueue
from graph_agent._internal.runtime.runs import RunsRepository, RunStatus
from graph_agent._internal.worker import THREAD_BUSY_WAIT_S, run_worker

MODULE = "t24_integration_flows"
FLOW_VERSION = 1
NODE_S = 0.4
POLL_S = 0.02
WAIT_S = 10.0
timeline: list[str] = []


class State(TypedDict, total=False):
    x: int


async def _timed(state: State) -> State:
    timeline.append("start")
    await asyncio.sleep(NODE_S)
    timeline.append("end")
    return {"x": 1}


@pytest.fixture
def user_module() -> Iterator[None]:
    mod = types.ModuleType(MODULE)
    mod.timed = StateGraph(State).add_node("n", _timed).add_edge(START, "n").add_edge("n", END)  # type: ignore[attr-defined]
    sys.modules[MODULE] = mod
    timeline.clear()
    yield
    del sys.modules[MODULE]


@dataclass
class Budget:
    deadline: float = float("inf")


@dataclass
class Resources:
    settings: Settings
    runs: RunsRepository | None
    flows: FlowRegistry
    checkpointer: Checkpointer
    queue: RunQueue | None
    observers: list[RunObserver] = field(default_factory=list)
    active_runs: ActiveRuns = field(default_factory=ActiveRuns)
    shutdown_budget: Budget = field(default_factory=Budget)


@pytest.fixture
async def pool(clean_database: str) -> AsyncIterator[AsyncConnectionPool]:
    pool = await open_app_pool(clean_database, max_size=4)
    try:
        await apply_migrations(pool)
        yield pool
    finally:
        await pool.close()


@pytest.fixture
def settings(clean_database: str) -> Settings:  # sync: reading the TOML source is blocking I/O
    return Settings(
        _env_file=None,
        database=DatabaseSettings(url=SecretStr(clean_database)),
        runs=RunsSettings(heartbeat_s=0.1, lease_s=2.0),
    )


@pytest.fixture
def procrastinate_schema(clean_database: str) -> None:
    with psycopg.connect(clean_database, autocommit=True) as conn:
        conn.execute(SchemaManager.get_schema())


@pytest.fixture
async def res(
    settings: Settings, procrastinate_schema: None, pool: AsyncConnectionPool, user_module: None
) -> AsyncIterator[Resources]:
    app = make_app(settings)
    async with app.open_async():
        saver = InMemorySaver()
        flows = FlowRegistry(
            {"timed": FlowSpec(target=f"{MODULE}:timed", version=FLOW_VERSION)},
            saver,
            lambda name: object(),  # type: ignore[arg-type,return-value]
        )
        yield Resources(settings, RunsRepository(pool), flows, saver, ProcrastinateRunQueue(app))


@pytest.fixture
async def worker(res: Resources) -> AsyncIterator[None]:
    task = asyncio.create_task(run_worker(res))
    yield
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def _new(res: Resources, thread: uuid.UUID | None = None) -> tuple[uuid.UUID, uuid.UUID]:
    assert res.runs is not None
    thread = thread or uuid.uuid4()
    run = await res.runs.create(thread_id=thread, flow="timed", flow_version=FLOW_VERSION, input={})
    return run.run_id, thread


async def _defer(res: Resources, run_id: uuid.UUID, thread: uuid.UUID) -> None:
    assert res.queue is not None
    await res.queue.defer(run_id, thread)


async def _status(pool: AsyncConnectionPool, run_id: uuid.UUID) -> str:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT status FROM graph_agent.runs WHERE run_id = %s", (run_id,))
        row = await cur.fetchone()
        assert row is not None
        return str(row[0])


async def _until(pool: AsyncConnectionPool, run_id: uuid.UUID, want: RunStatus) -> None:
    async with asyncio.timeout(WAIT_S):
        while await _status(pool, run_id) != want:  # noqa: ASYNC110 (polls a DB row)
            await asyncio.sleep(POLL_S)


async def _jobs(pool: AsyncConnectionPool) -> list[tuple[Any, ...]]:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT status::text, attempts FROM procrastinate_jobs ORDER BY id"
        )
        return await cur.fetchall()


async def _until_jobs_done(pool: AsyncConnectionPool, count: int) -> list[tuple[Any, ...]]:
    async with asyncio.timeout(WAIT_S):
        while len(jobs := [j for j in await _jobs(pool) if j[0] == "succeeded"]) < count:  # noqa: ASYNC110
            await asyncio.sleep(POLL_S)
    return jobs


async def test_normal_run_reaches_success(
    res: Resources, pool: AsyncConnectionPool, worker: None
) -> None:
    run_id, thread = await _new(res)
    await _defer(res, run_id, thread)
    await _until(pool, run_id, RunStatus.SUCCESS)


async def test_run_cancelled_before_claim_is_a_noop(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    assert res.runs is not None
    run_id, thread = await _new(res)
    assert await res.runs.request_cancel(run_id, thread) is RunStatus.CANCELLED
    await _defer(res, run_id, thread)
    task = asyncio.create_task(run_worker(res))
    try:
        await _until_jobs_done(pool, 1)
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    assert await _status(pool, run_id) == RunStatus.CANCELLED
    assert timeline == []  # the flow never ran


async def test_same_thread_jobs_run_one_after_the_other(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    first, thread = await _new(res)
    second, _ = await _new(res, thread)
    await _defer(res, first, thread)
    await _defer(res, second, thread)
    await _defer(res, second, thread)  # queueing_lock: ignored while the job is `todo`
    assert len(await _jobs(pool)) == 2
    task = asyncio.create_task(run_worker(res))
    try:
        jobs = await _until_jobs_done(pool, 2)
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    assert timeline == ["start", "end", "start", "end"]
    assert [attempts for _, attempts in jobs] == [1, 1]  # finish counts one: no thread_busy retries
    assert NODE_S < THREAD_BUSY_WAIT_S
