"""The HTTP API over ASGI against compose Postgres, with toy flows (no network, no model)."""

import asyncio
import json
import sys
import types
import uuid
from collections.abc import AsyncIterator, Iterator
from http import HTTPStatus
from typing import Any, TypedDict

import httpx
import pytest
from fastapi import FastAPI
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt
from pydantic import SecretStr

from graph_agent._internal.api.app import create_app
from graph_agent._internal.bootstrap import Container
from graph_agent._internal.config.manifest import FlowSpec
from graph_agent._internal.config.settings import (
    DatabaseSettings,
    LimitsSettings,
    RunsSettings,
    Settings,
)
from graph_agent._internal.db.migrate import apply_migrations
from graph_agent._internal.db.pool import open_app_pool, open_checkpointer_pool
from graph_agent._internal.runtime.checkpointer import setup_checkpointer
from graph_agent._internal.runtime.runner import execute_run

MODULE = "t23_integration_flows"
WAIT_S = 5.0
POLL_S = 0.02


class State(TypedDict, total=False):
    x: int
    answer: str


async def _echo(state: State) -> State:
    return {"x": 1}


async def _ask(state: State) -> State:
    return {"answer": interrupt("name?")}


async def _slow(state: State) -> State:
    await asyncio.sleep(30)
    return {}


def _graph(fn: Any) -> StateGraph[State, Any, State, State]:
    return StateGraph(State).add_node("n", fn).add_edge(START, "n").add_edge("n", END)


@pytest.fixture
def user_module() -> Iterator[None]:
    mod = types.ModuleType(MODULE)
    mod.echo = _graph(_echo)  # type: ignore[attr-defined]
    mod.ask = _graph(_ask)  # type: ignore[attr-defined]
    mod.slow = _graph(_slow)  # type: ignore[attr-defined]
    sys.modules[MODULE] = mod
    yield
    del sys.modules[MODULE]


class InlineQueue:
    """Stands in for T24's queue: runs `execute_run` as a task in this process."""

    def __init__(self, container: Container) -> None:
        self._container = container
        self.tasks: list[asyncio.Task[None]] = []

    async def defer(self, run_id: uuid.UUID, thread_id: uuid.UUID) -> None:
        async def drain() -> None:
            async for _ in execute_run(self._container, run_id):
                pass

        self.tasks.append(asyncio.create_task(drain()))


@pytest.fixture
def settings(clean_database: str) -> Settings:
    return Settings(
        _env_file=None,
        database=DatabaseSettings(url=SecretStr(clean_database)),
        limits=LimitsSettings(cpu_workers=1, max_active_runs=1, admission_wait_s=0.1),
        runs=RunsSettings(heartbeat_s=0.05, lease_s=1.0),
        flows={n: FlowSpec(target=f"{MODULE}:{n}") for n in ("echo", "ask", "slow")},
    )


@pytest.fixture
async def app(settings: Settings, clean_database: str, user_module: None) -> AsyncIterator[FastAPI]:
    pool = await open_app_pool(clean_database, max_size=2)
    try:
        await apply_migrations(pool)
    finally:
        await pool.close()
    cp_pool = await open_checkpointer_pool(clean_database, max_size=2)
    try:
        await setup_checkpointer(cp_pool)
    finally:
        await cp_pool.close()
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        yield app


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as http:
        yield http


@pytest.fixture
def container(app: FastAPI) -> Container:
    c: Container = app.state.container
    return c


def _sse(response: httpx.Response) -> list[tuple[str, dict[str, Any]]]:
    frames = []
    for block in response.text.strip().split("\n\n"):
        fields = dict(line.split(": ", 1) for line in block.splitlines())
        frames.append((fields["event"], json.loads(fields["data"])))
    return frames


async def _stream(
    client: httpx.AsyncClient, body: dict[str, Any], thread: str | None = None
) -> httpx.Response:
    path = f"/threads/{thread}/runs/stream" if thread else "/runs/stream"
    return await client.post(path, json=body)


async def _until(client: httpx.AsyncClient, thread: str, run: str, status: str) -> dict[str, Any]:
    async with asyncio.timeout(WAIT_S):
        while True:
            got: dict[str, Any] = (await client.get(f"/threads/{thread}/runs/{run}")).json()
            if got["status"] == status:
                return got
            await asyncio.sleep(POLL_S)


async def test_inline_run_state_and_delete(client: httpx.AsyncClient) -> None:
    response = await _stream(client, {"flow": "echo", "input": {}})
    assert response.status_code == HTTPStatus.OK
    frames = _sse(response)
    assert [name for name, _ in frames] == ["metadata", "updates", "end"]
    assert frames[-1][1]["status"] == "success"
    thread = frames[0][1]["thread_id"]

    runs = (await client.get(f"/threads/{thread}/runs")).json()
    assert [r["status"] for r in runs] == ["success"]
    state = (await client.get(f"/threads/{thread}/state")).json()
    assert state["values"] == {"x": 1} and state["next"] == [] and state["checkpoint_id"]

    assert (await client.delete(f"/threads/{thread}")).status_code == HTTPStatus.NO_CONTENT
    assert (await client.get(f"/threads/{thread}/state")).status_code == HTTPStatus.NOT_FOUND


async def test_interrupt_then_resume(client: httpx.AsyncClient) -> None:
    first = _sse(await _stream(client, {"flow": "ask", "input": {}}))
    assert [name for name, _ in first][-2:] == ["interrupt", "end"]
    assert first[-1][1]["status"] == "interrupted"
    thread = first[0][1]["thread_id"]

    state = (await client.get(f"/threads/{thread}/state")).json()
    assert [i["value"] for i in state["interrupts"]] == ["name?"]

    rejected = await _stream(client, {"flow": "ask", "input": {}}, thread)
    assert rejected.status_code == HTTPStatus.CONFLICT
    assert rejected.json()["code"] == "thread_interrupted"

    resumed = _sse(await _stream(client, {"flow": "ask", "command": {"resume": "Ada"}}, thread))
    assert resumed[-1][1]["status"] == "success"
    state = (await client.get(f"/threads/{thread}/state")).json()
    assert state["values"]["answer"] == "Ada" and state["interrupts"] == []


async def test_resume_without_a_pending_interrupt_is_rejected(client: httpx.AsyncClient) -> None:
    response = await _stream(client, {"flow": "echo", "command": {"resume": "x"}})
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json()["code"] == "invalid_request"


async def test_background_without_a_queue_is_unavailable(client: httpx.AsyncClient) -> None:
    response = await client.post("/runs", json={"flow": "echo", "input": {}})
    assert response.status_code == HTTPStatus.SERVICE_UNAVAILABLE
    assert response.json()["code"] == "queue_unavailable"


async def test_background_run_cancel_and_busy_thread(
    client: httpx.AsyncClient, container: Container
) -> None:
    container.queue = InlineQueue(container)
    accepted = await client.post("/runs", json={"flow": "slow", "input": {}})
    assert accepted.status_code == HTTPStatus.ACCEPTED
    thread, run = accepted.json()["thread_id"], accepted.json()["run_id"]
    await _until(client, thread, run, "running")

    busy = await _stream(client, {"flow": "slow", "input": {}}, thread)
    assert busy.status_code == HTTPStatus.CONFLICT and busy.json()["code"] == "thread_busy"
    assert (await client.delete(f"/threads/{thread}")).status_code == HTTPStatus.CONFLICT
    statuses = [r["status"] for r in (await client.get(f"/threads/{thread}/runs")).json()]
    assert sorted(statuses) == ["cancelled", "running"]  # the refused inline row is not left

    cancel = await client.post(f"/threads/{thread}/runs/{run}/cancel")
    assert cancel.status_code == HTTPStatus.ACCEPTED
    await _until(client, thread, run, "cancelled")
    assert (await client.delete(f"/threads/{thread}")).status_code == HTTPStatus.NO_CONTENT


async def test_inline_run_waits_for_admission_then_429(
    client: httpx.AsyncClient, container: Container
) -> None:
    async with container.limits.admit():  # max_active_runs = 1
        response = await _stream(client, {"flow": "echo", "input": {}})
    assert response.status_code == HTTPStatus.TOO_MANY_REQUESTS
    assert response.json() == {
        "code": "too_many_runs",
        "message": response.json()["message"],
        "retryable": True,
    }
    assert (await _stream(client, {"flow": "echo", "input": {}})).status_code == HTTPStatus.OK


async def test_flow_version_mismatch_and_unknown_flow(
    client: httpx.AsyncClient, container: Container
) -> None:
    assert container.runs is not None
    thread = uuid.uuid4()
    await container.runs.create(thread_id=thread, flow="echo", flow_version=2, input={})
    response = await _stream(client, {"flow": "echo", "input": {}}, str(thread))
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json()["code"] == "flow_version_mismatch"

    unknown = await _stream(client, {"flow": "nope", "input": {}})
    assert unknown.status_code == HTTPStatus.NOT_FOUND
    assert unknown.json()["code"] == "flow_not_found"


async def test_flows_health_and_ready(client: httpx.AsyncClient) -> None:
    assert (await client.get("/healthz")).status_code == HTTPStatus.OK
    assert (await client.get("/readyz")).status_code == HTTPStatus.OK
    flows = (await client.get("/flows")).json()
    assert {f["name"] for f in flows} == {"echo", "ask", "slow"}


async def test_client_disconnect_cancels_an_inline_run(app: FastAPI, container: Container) -> None:
    body = json.dumps({"flow": "slow", "input": {}}).encode()
    sent_body = False
    first_chunk = asyncio.Event()

    async def receive() -> dict[str, Any]:
        nonlocal sent_body
        if not sent_body:
            sent_body = True
            return {"type": "http.request", "body": body, "more_body": False}
        await first_chunk.wait()  # disconnect once the stream started
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.body" and message.get("body"):
            first_chunk.set()

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/runs/stream",
        "raw_path": b"/runs/stream",
        "query_string": b"",
        "headers": [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(body)).encode()),
        ],
        "server": ("t", 80),
        "client": ("c", 1),
        "app": app,
    }
    async with asyncio.timeout(WAIT_S):
        await app(scope, receive, send)  # type: ignore[arg-type]
        await container.active_runs.settled()
    assert container.app_pool is not None
    async with container.app_pool.connection() as conn:
        rows = await (await conn.execute("SELECT status FROM graph_agent.runs")).fetchall()
    assert rows == [("cancelled",)]
    assert container.limits.active_runs == 0  # the admission slot came back
