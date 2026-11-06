"""T26: graceful shutdown. A slow multi-step flow is stopped mid-run through each entrypoint's path
(worker cancel, `Container.begin_shutdown`, the uvicorn server); the run must end with its lease
released for re-claim (never `pending`), resume from its checkpoint and execute no step twice."""

import asyncio
import dataclasses
import itertools
import json
import signal
import sys
import types
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import suppress
from http import HTTPStatus
from typing import Any, TypedDict

import httpx
import pytest
import uvicorn
from fastapi import FastAPI
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from psycopg_pool import AsyncConnectionPool
from pydantic import SecretStr

from integration._resources import GRACE_S, Resources
from ranvier._internal.api.app import DrainingServer, create_app
from ranvier._internal.bootstrap import Container, ShutdownBudget
from ranvier._internal.config.manifest import FlowSpec
from ranvier._internal.config.settings import (
    DatabaseSettings,
    LimitsSettings,
    RunsSettings,
    Settings,
)
from ranvier._internal.db.migrate import advisory_lock, apply_migrations
from ranvier._internal.db.pool import open_app_pool, open_checkpointer_pool
from ranvier._internal.queue import (
    RECOVERY_PRIORITY,
    ProcrastinateRunQueue,
    apply_procrastinate_schema,
    make_app,
)
from ranvier._internal.runtime.checkpointer import setup_checkpointer
from ranvier._internal.runtime.flows import FlowRegistry
from ranvier._internal.runtime.runner import ActiveRuns, execute_run
from ranvier._internal.runtime.runs import RunsRepository, RunStatus
from ranvier._internal.worker import run_worker

MODULE = "t26_integration_flows"
FLOW = "steps"
STEPS = ("a", "b", "c", "d")
NODE_S = 0.3
POLL_S = 0.02
WAIT_S = 10.0
timeline: list[str] = []  # "start:<node>" / "end:<node>", in order


class State(TypedDict, total=False):
    done: list[str]


def _node(name: str) -> Any:
    async def run(state: State) -> State:
        timeline.append(f"start:{name}")
        await asyncio.sleep(NODE_S)
        timeline.append(f"end:{name}")
        return {"done": [*state.get("done", []), name]}

    return run


@pytest.fixture
def user_module() -> Iterator[None]:
    graph = StateGraph(State)
    for name in STEPS:
        graph.add_node(name, _node(name))
    graph.add_edge(START, STEPS[0])
    for before, after in itertools.pairwise(STEPS):
        graph.add_edge(before, after)
    graph.add_edge(STEPS[-1], END)
    mod = types.ModuleType(MODULE)
    mod.steps = graph  # type: ignore[attr-defined]
    sys.modules[MODULE] = mod
    timeline.clear()
    yield
    del sys.modules[MODULE]


@pytest.fixture
def settings(clean_database: str) -> Settings:  # sync: reading the TOML source is blocking I/O
    return Settings(
        _env_file=None,
        database=DatabaseSettings(url=SecretStr(clean_database)),
        limits=LimitsSettings(cpu_workers=1, max_active_runs=2, admission_wait_s=0.1),
        runs=RunsSettings(heartbeat_s=0.1, lease_s=2.0),
        flows={FLOW: FlowSpec(target=f"{MODULE}:{FLOW}")},
    )


async def _until_timeline(entry: str) -> None:
    async with asyncio.timeout(WAIT_S):
        while entry not in timeline:  # noqa: ASYNC110 (polls a test list)
            await asyncio.sleep(POLL_S)


async def _rows(pool: AsyncConnectionPool, query: str, *params: Any) -> list[tuple[Any, ...]]:
    async with pool.connection() as conn:
        cur = await conn.execute(query, params)
        return await cur.fetchall()


async def _lease(pool: AsyncConnectionPool, run_id: uuid.UUID) -> tuple[str, int, str, bool]:
    """(status, attempt, lease token, lease already expired)."""
    rows = await _rows(
        pool,
        "SELECT status, attempt, lease_owner, lease_expires_at <= now() "
        "FROM ranvier.runs WHERE run_id = %s",
        run_id,
    )
    return rows[0]


async def _until_status(pool: AsyncConnectionPool, run_id: uuid.UUID, want: RunStatus) -> None:
    async with asyncio.timeout(WAIT_S):
        while (await _lease(pool, run_id))[0] != want:  # noqa: ASYNC110 (polls a DB row)
            await asyncio.sleep(POLL_S)


def _assert_each_step_ran_once() -> None:
    assert timeline == [f"{edge}:{n}" for n in STEPS for edge in ("start", "end")]


# --- worker: Procrastinate stops fetching, runs drain, the job is deferred again ---------------


@pytest.fixture
async def pool(clean_database: str) -> AsyncIterator[AsyncConnectionPool]:
    pool = await open_app_pool(clean_database, max_size=4)
    try:
        await apply_migrations(pool)
        async with advisory_lock(pool) as conn:
            await apply_procrastinate_schema(conn)
        yield pool
    finally:
        await pool.close()


@pytest.fixture
async def res(
    settings: Settings, pool: AsyncConnectionPool, user_module: None
) -> AsyncIterator[Resources]:
    app = make_app(settings)
    async with app.open_async():
        saver = InMemorySaver()  # outlives the worker "process", like the Postgres checkpoints
        flows = FlowRegistry(
            {FLOW: settings.flows[FLOW]},
            saver,
            lambda name: object(),  # type: ignore[arg-type,return-value]
        )
        yield Resources(settings, RunsRepository(pool), flows, saver, ProcrastinateRunQueue(app))


async def test_worker_stop_drains_releases_the_lease_and_the_requeued_run_resumes_once(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    assert res.runs is not None and res.queue is not None
    thread = uuid.uuid4()
    run = await res.runs.create(thread_id=thread, flow=FLOW, flow_version=1, input={})
    await res.queue.defer(run.run_id, thread)
    worker = asyncio.create_task(run_worker(res))
    await _until_timeline("start:b")
    _, attempt, token, _ = await _lease(pool, run.run_id)
    assert attempt == 1

    worker.cancel()  # what the entrypoint does on SIGTERM
    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(worker, WAIT_S)

    # The superstep in flight finished and checkpointed; the next one never started.
    assert timeline == ["start:a", "end:a", "start:b", "end:b"]
    assert res.active_runs.draining
    status, attempt, new_token, expired = await _lease(pool, run.run_id)
    assert (status, attempt, expired) == ("running", 0, True)  # never `pending`; attempt given back
    assert new_token != token and new_token.startswith("drained:")  # the old owner is fenced out
    jobs = await _rows(
        pool,
        "SELECT status::text, priority FROM ranvier.procrastinate_jobs "
        "WHERE task_name = 'ranvier.execute_run' AND status = 'todo'",
    )
    assert jobs == [("todo", RECOVERY_PRIORITY)]

    # A new worker (fresh process state, its own app) claims it, resumes from the checkpoint and
    # finishes.
    second_app = make_app(res.settings)
    async with second_app.open_async():
        fresh = dataclasses.replace(
            res,
            queue=ProcrastinateRunQueue(second_app),
            active_runs=ActiveRuns(),
            shutdown_budget=ShutdownBudget(GRACE_S),
        )
        second = asyncio.create_task(run_worker(fresh))
        try:
            await _until_status(pool, run.run_id, RunStatus.SUCCESS)
        finally:
            second.cancel()
            with suppress(asyncio.CancelledError):
                await second
    _assert_each_step_ran_once()
    assert (await _lease(pool, run.run_id))[:2] == ("success", 1)


# --- API: begin_shutdown refuses new inline runs and hands the ones in flight over -------------


class RecordingQueue:
    def __init__(self) -> None:
        self.deferred: list[tuple[uuid.UUID, uuid.UUID, bool]] = []

    async def defer(
        self, run_id: uuid.UUID, thread_id: uuid.UUID, *, recovery: bool = False
    ) -> None:
        self.deferred.append((run_id, thread_id, recovery))


async def _prepare_database(database_url: str) -> None:
    pool = await open_app_pool(database_url, max_size=2)
    try:
        await apply_migrations(pool)
    finally:
        await pool.close()
    cp_pool = await open_checkpointer_pool(database_url, max_size=2)
    try:
        await setup_checkpointer(cp_pool)
    finally:
        await cp_pool.close()


@pytest.fixture
async def app(settings: Settings, clean_database: str, user_module: None) -> AsyncIterator[FastAPI]:
    await _prepare_database(clean_database)
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        yield app


@pytest.fixture
def container(app: FastAPI) -> Container:
    c: Container = app.state.container
    return c


def _sse(text: str) -> list[tuple[str, dict[str, Any]]]:
    frames = []
    for block in text.strip().split("\n\n"):
        fields = dict(line.split(": ", 1) for line in block.splitlines())
        frames.append((fields["event"], json.loads(fields["data"])))
    return frames


RUN_BODY = {"flow": FLOW, "input": {}}


async def test_begin_shutdown_hands_over_inline_runs_and_refuses_new_ones(
    app: FastAPI, container: Container
) -> None:
    queue = RecordingQueue()
    container.queue = queue
    assert container.runs is not None and container.app_pool is not None
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        inline = asyncio.create_task(c.post("/runs/stream", json=RUN_BODY))
        await _until_timeline("start:b")

        container.begin_shutdown()  # what the server does on SIGTERM, before waiting for streams

        refused = await c.post("/runs/stream", json=RUN_BODY)
        assert refused.status_code == HTTPStatus.SERVICE_UNAVAILABLE
        assert refused.json() == {
            "code": "shutting_down",
            "message": "shutting down",
            "retryable": True,
        }
        async with asyncio.timeout(WAIT_S):
            frames = _sse((await inline).text)
    # The client is told the run lives on elsewhere, and the superstep in flight completed.
    name, data = frames[-1]
    assert (name, data["code"], data["retryable"]) == ("error", "run_handed_over", True)
    run_id, thread_id = uuid.UUID(data["run_id"]), uuid.UUID(frames[0][1]["thread_id"])
    assert timeline == ["start:a", "end:a", "start:b", "end:b"]
    status, attempt, token, expired = await _lease(container.app_pool, run_id)
    assert (status, attempt, expired) == ("running", 0, True) and token.startswith("drained:")
    assert queue.deferred == [(run_id, thread_id, True)]
    state = await container.checkpointer.aget_tuple({"configurable": {"thread_id": str(thread_id)}})
    assert state is not None and state.metadata["run_id"] == str(run_id)  # resumable from here

    # The next claim (another process: here a fresh ActiveRuns) finishes it; nothing runs twice.
    container.active_runs = ActiveRuns()
    async with asyncio.timeout(WAIT_S):
        events = [event async for event in execute_run(container, run_id)]
    assert events[-1].status == "success"  # type: ignore[union-attr]
    _assert_each_step_ran_once()


async def test_server_drains_before_waiting_for_streams_and_exits_without_reraising(
    settings: Settings, clean_database: str, user_module: None
) -> None:
    """The real uvicorn path: SIGTERM -> `handle_exit`. Upstream would re-raise SIGTERM at the
    end and kill this very test process (and, in `serve`, skip the interpreter cleanup)."""
    await _prepare_database(clean_database)
    app = create_app(settings)
    config = uvicorn.Config(
        app, host="127.0.0.1", port=0, log_level="warning", timeout_graceful_shutdown=int(GRACE_S)
    )
    server = DrainingServer(config, app)
    serving = asyncio.create_task(
        server.serve()
    )  # runs the lifespan: opens and closes the container
    async with asyncio.timeout(WAIT_S):
        while not server.started:  # noqa: ASYNC110
            await asyncio.sleep(POLL_S)
    port = server.servers[0].sockets[0].getsockname()[1]
    container: Container = app.state.container
    container.queue = RecordingQueue()
    async with httpx.AsyncClient() as http:
        async with http.stream(
            "POST", f"http://127.0.0.1:{port}/runs/stream", json=RUN_BODY
        ) as response:
            await _until_timeline("start:b")
            server.handle_exit(signal.SIGTERM, None)
            async with asyncio.timeout(WAIT_S):
                body = (await response.aread()).decode()
        async with asyncio.timeout(WAIT_S):
            await serving
    assert "run_handed_over" in body
    assert timeline == ["start:a", "end:a", "start:b", "end:b"]
    assert container.active_runs.draining and not container.limits.active_runs
    assert container.shutdown_budget.deadline != float("inf")
