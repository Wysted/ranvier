"""`_frames` and `_SseResponse` with in-memory generators: no run, no socket."""

import asyncio
import json
import time
import uuid
from collections.abc import AsyncGenerator, MutableMapping
from contextlib import AsyncExitStack
from typing import Any

import pytest

from ranvier._internal.api import routes
from ranvier._internal.api.routes import _frames, _SseResponse
from ranvier._internal.runtime.events import EndEvent, UpdatesEvent, UsageTotals
from ranvier._internal.runtime.runner import ActiveRuns


class Tracked:
    """An inner event stream that records whether it was closed."""

    def __init__(self, events: list[Any]) -> None:
        self.closed = False
        self._events = events

    async def gen(self) -> AsyncGenerator[Any]:
        try:
            for event in self._events:
                yield event
        finally:
            self.closed = True


async def _collect(frames: AsyncGenerator[bytes]) -> list[tuple[str, dict[str, Any], str]]:
    out = []
    async for chunk in frames:
        fields = dict(line.split(": ", 1) for line in chunk.decode().strip().splitlines())
        out.append((fields["event"], json.loads(fields["data"]), fields["id"]))
    return out


async def _start(events: list[Any], run_id: uuid.UUID) -> tuple[Tracked, AsyncGenerator[bytes]]:
    inner = Tracked(events)
    agen = inner.gen()
    first = await anext(agen, None)
    return inner, _frames(agen, first, run_id)


async def test_stream_ending_without_a_terminal_event_is_handed_over() -> None:
    run_id = uuid.uuid4()
    inner, frames = await _start([UpdatesEvent((), "n", {"x": 1})], run_id)
    got = await _collect(frames)
    assert [name for name, _, _ in got] == ["updates", "error"]
    _, body, frame_id = got[-1]
    assert body == {
        "code": "run_handed_over",
        "message": body["message"],
        "retryable": True,
        "run_id": str(run_id),
    }
    assert frame_id == f"{run_id}:2"
    assert inner.closed


async def test_terminal_event_ends_the_stream_without_a_hand_over() -> None:
    inner, frames = await _start([EndEvent("success", UsageTotals(0, 0, 0))], uuid.uuid4())
    assert [name for name, _, _ in await _collect(frames)] == ["end"]
    assert inner.closed


async def test_unserializable_event_ends_with_an_error_frame_and_closes_the_stream() -> None:
    inner, frames = await _start(
        [UpdatesEvent((), "n", {"bad": object()}), UpdatesEvent((), "n", {})], uuid.uuid4()
    )
    got = await _collect(frames)
    assert [name for name, _, _ in got] == ["error"]
    assert got[0][1]["code"] == "internal_error"
    assert inner.closed  # the run is cancelled, not left running behind a dead stream


async def test_a_send_that_never_returns_drops_the_stream_and_closes_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(routes, "SSE_SEND_TIMEOUT_S", 0.05)
    events = [UpdatesEvent((), "n", {}), UpdatesEvent((), "n", {})]
    inner, frames = await _start(events, uuid.uuid4())
    response = _SseResponse(frames, AsyncExitStack(), ActiveRuns())

    async def receive() -> dict[str, Any]:
        await asyncio.Event().wait()  # the client never disconnects
        raise AssertionError

    async def send(message: MutableMapping[str, Any]) -> None:
        if message["type"] == "http.response.body":
            await asyncio.Event().wait()  # a stuck socket

    async with asyncio.timeout(2):
        await response({"type": "http", "asgi": {"spec_version": "2.3"}}, receive, send)
    assert inner.closed


async def test_ping_keeps_a_silent_stream_alive_and_the_close_cancels_the_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(routes, "SSE_PING_INTERVAL_S", 0.02)
    closed = False
    release = asyncio.Event()

    async def slow() -> AsyncGenerator[Any]:
        nonlocal closed
        try:
            yield UpdatesEvent((), "n", {})
            await release.wait()  # a node that stays silent
            yield EndEvent("success", UsageTotals(0, 0, 0))
        finally:
            closed = True

    agen = slow()
    frames = _frames(agen, await anext(agen), uuid.uuid4())
    chunks = [await anext(frames)]  # the first event
    chunks += [await anext(frames), await anext(frames)]  # two pings while the node is silent
    assert chunks[1:] == [routes._SSE_PING] * 2
    release.set()
    assert b"event: end" in await anext(frames)
    await frames.aclose()
    assert closed

    # Leaving while the next event is pending: the wait is cancelled before the stream is closed.
    closed = False
    release.clear()
    agen = slow()
    frames = _frames(agen, await anext(agen), uuid.uuid4())
    await anext(frames)
    assert await anext(frames) == routes._SSE_PING
    await frames.aclose()  # would raise "asynchronous generator is already running" otherwise
    assert closed


async def test_disconnect_while_the_node_is_silent_closes_the_stream_without_spinning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(routes, "SSE_PING_INTERVAL_S", 30.0)
    closed = False
    cleanup_s = 0.3

    async def silent() -> AsyncGenerator[Any]:
        nonlocal closed
        try:
            yield UpdatesEvent((), "n", {})
            await asyncio.sleep(100)  # a node that stays silent
            yield EndEvent("success", UsageTotals(0, 0, 0))
        except asyncio.CancelledError:
            # Like execute_run: a shielded cleanup (DB writes) that outlives the cancel.
            await asyncio.shield(asyncio.create_task(asyncio.sleep(cleanup_s)))
            raise
        finally:
            closed = True

    agen = silent()
    response = _SseResponse(
        _frames(agen, await anext(agen), uuid.uuid4()), AsyncExitStack(), ActiveRuns()
    )

    async def receive() -> dict[str, Any]:
        await asyncio.sleep(0.1)
        return {"type": "http.disconnect"}

    async def send(message: MutableMapping[str, Any]) -> None:
        pass

    cpu = time.process_time()
    async with asyncio.timeout(5):
        # spec 2.3: Starlette runs the body in a task group that it cancels on disconnect.
        await response({"type": "http", "asgi": {"spec_version": "2.3"}}, receive, send)
    assert closed
    # A busy-wait on the cancelled scope burns CPU for the whole cleanup (measured: ~1x wall).
    assert time.process_time() - cpu < cleanup_s / 2
