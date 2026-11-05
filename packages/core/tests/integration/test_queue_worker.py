"""T24: deferral by name with the thread lock, and the `execute_run` job (S4 hints)."""

import asyncio
import sys
import types
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, suppress
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
from graph_agent._internal.observability.tracing import LoggingMetrics, Observability
from graph_agent._internal.queue import ProcrastinateRunQueue, make_app
from graph_agent._internal.runtime.checkpointer import Checkpointer
from graph_agent._internal.runtime.events import RunObserver
from graph_agent._internal.runtime.flows import FlowRegistry
from graph_agent._internal.runtime.runner import ActiveRuns, RunQueue
from graph_agent._internal.runtime.runs import ClaimOutcome, RunsRepository, RunStatus
from graph_agent._internal.worker import run_worker

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
    observability: Observability = field(
        default_factory=lambda: Observability(metrics=LoggingMetrics())
    )


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


@asynccontextmanager
async def _running_worker(res: Resources) -> AsyncIterator[None]:
    task = asyncio.create_task(run_worker(res))
    try:
        yield
    finally:
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


async def test_normal_run_reaches_success(res: Resources, pool: AsyncConnectionPool) -> None:
    run_id, thread = await _new(res)
    await _defer(res, run_id, thread)
    async with _running_worker(res):
        await _until(pool, run_id, RunStatus.SUCCESS)


async def test_run_cancelled_before_claim_is_a_noop(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    assert res.runs is not None
    run_id, thread = await _new(res)
    assert await res.runs.request_cancel(run_id, thread) is RunStatus.CANCELLED
    await _defer(res, run_id, thread)
    async with _running_worker(res):
        await _until_jobs_done(pool, 1)
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
    async with _running_worker(res):
        jobs = await _until_jobs_done(pool, 2)
    assert timeline == ["start", "end", "start", "end"]
    assert [attempts for _, attempts in jobs] == [1, 1]  # finish counts one: no thread_busy retries


async def _until_retrying(pool: AsyncConnectionPool, job_index: int) -> None:
    """Wait until the job is back to `todo` after at least one thread_busy retry."""
    async with asyncio.timeout(WAIT_S):
        while True:
            status, attempts = (await _jobs(pool))[job_index]
            if status == "todo" and attempts >= 1:
                return
            await asyncio.sleep(POLL_S)


async def test_thread_busy_job_retries_until_the_thread_is_free(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    assert res.runs is not None
    blocker, thread = await _new(res)
    claim = await res.runs.claim(blocker, "test", res.settings.runs.lease_s, 3)
    assert claim.outcome is ClaimOutcome.CLAIMED and claim.token is not None
    run_id, _ = await _new(res, thread)
    await _defer(res, run_id, thread)
    async with _running_worker(res):
        await _until_retrying(pool, 0)
        assert await _status(pool, run_id) == RunStatus.PENDING
        assert await res.runs.finish(blocker, claim.token, RunStatus.SUCCESS)
        await _until(pool, run_id, RunStatus.SUCCESS)


@pytest.mark.parametrize("recovery", [False, True])
async def test_recovery_defer_is_not_blocked_by_a_retrying_thread_busy_job(
    res: Resources, pool: AsyncConnectionPool, recovery: bool
) -> None:
    """R1 handed over (running, lease released); R2's job retries thread_busy ahead of R1's defer.

    Without priority, R2's `todo` job (even waiting for its retry) blocks R1's job until it
    exhausts its attempts (~minute): R1 would miss WAIT_S.
    """
    assert res.runs is not None
    r1, thread = await _new(res)
    r2, _ = await _new(res, thread)
    claim = await res.runs.claim(r1, "test", res.settings.runs.lease_s, 3)
    assert claim.token is not None
    assert await res.runs.release_for_drain(r1, claim.token)
    await _defer(res, r2, thread)  # J2: lower id than the hand-over defer
    async with _running_worker(res):
        await _until_retrying(pool, 0)
        assert res.queue is not None
        await res.queue.defer(r1, thread, recovery=recovery)
        if recovery:
            await _until(pool, r1, RunStatus.SUCCESS)
        else:
            with pytest.raises(TimeoutError):  # documents the blocking this priority avoids
                await _until(pool, r1, RunStatus.SUCCESS)
