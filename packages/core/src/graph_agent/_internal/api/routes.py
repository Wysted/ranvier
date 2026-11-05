"""HTTP endpoints (design section 6): runs, SSE streams, thread state, admission."""

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, aclosing
from http import HTTPStatus
from typing import Annotated, Any, Final

import anyio
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.sse import format_sse_event
from starlette.responses import StreamingResponse
from starlette.types import Message, Receive, Scope, Send

from graph_agent._internal.api.auth import Principal, require_principal
from graph_agent._internal.api.dto import (
    FlowItem,
    RunAccepted,
    RunOut,
    RunRequest,
    SseEncoder,
    ThreadState,
)
from graph_agent._internal.bootstrap import Container
from graph_agent._internal.limits import TooManyRuns
from graph_agent._internal.runtime.events import EndEvent, ErrorEvent
from graph_agent._internal.runtime.flows import check_version
from graph_agent._internal.runtime.runner import (
    ActiveRuns,
    check_thread_command,
    execute_run,
    pending_interrupts,
)
from graph_agent._internal.runtime.runs import (
    ErrorCode,
    Run,
    RunError,
    RunsRepository,
    ThreadPin,
)

logger = logging.getLogger(__name__)

# A client that does not read for this long is dropped (its run is cancelled): the runner's
# bounded queue would otherwise hold the thread and keep the lease alive behind a stuck socket.
SSE_SEND_TIMEOUT_S: Final = 30.0
RUN_HANDED_OVER: Final = "run_handed_over"  # shortcut: not an ErrorCode; add it there with T24/T26
QUEUE_UNAVAILABLE: Final = "queue_unavailable"
READY_DB_TIMEOUT_S: Final = 2.0
_SSE_HEADERS: Final = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}

router = APIRouter(dependencies=[Depends(require_principal)])
health = APIRouter()


def _container(request: Request) -> Container:
    container: Container = request.app.state.container
    return container


Ctr = Annotated[Container, Depends(_container)]
Caller = Annotated[Principal, Depends(require_principal)]


def _runs(container: Container) -> RunsRepository:
    if container.runs is None:
        raise RuntimeError("the HTTP API needs the runs repository (memory mode has none)")
    return container.runs


# -- helpers --------------------------------------------------------------------------------------


def _own(pin: ThreadPin, principal: Principal) -> None:
    """A tenant sees only its own threads; a foreign one looks absent."""
    if principal.tenant_id is not None and pin.tenant_id != principal.tenant_id:
        raise RunError(ErrorCode.NOT_FOUND, "thread not found")


async def _pin(container: Container, principal: Principal, thread_id: uuid.UUID) -> ThreadPin:
    pin = await _runs(container).thread_pin(thread_id)
    if pin is None:
        raise RunError(ErrorCode.NOT_FOUND, "thread not found")
    _own(pin, principal)
    return pin


async def _check_request(
    container: Container, principal: Principal, thread_id: uuid.UUID, req: RunRequest
) -> tuple[int, str | None]:
    """Validate a run request against the thread; returns the (flow_version, tenant_id) to pin."""
    compiled = await container.flows.get(req.flow)  # flow_not_found / flow_build_failed
    pin = await _runs(container).thread_pin(thread_id)
    if pin is None:
        pending: tuple[str, ...] = ()
        version, tenant = compiled.version, principal.tenant_id
    else:
        _own(pin, principal)
        if pin.flow != req.flow:
            raise RunError(ErrorCode.INVALID_REQUEST, f"thread belongs to flow {pin.flow!r}")
        check_version(compiled, pin.flow_version)  # flow_version_mismatch
        pending = await pending_interrupts(container.checkpointer, thread_id)
        version, tenant = pin.flow_version, pin.tenant_id
    resume = None if req.command is None else req.command.resume
    check_thread_command(pending, input=req.input, resume=resume)
    return version, tenant


async def _insert(
    container: Container,
    thread_id: uuid.UUID,
    req: RunRequest,
    pinned: tuple[int, str | None],
) -> Run:
    version, tenant = pinned
    return await _runs(container).create(
        thread_id=thread_id,
        flow=req.flow,
        flow_version=version,
        tenant_id=tenant,
        input=req.input,
        command=None if req.command is None else req.command.model_dump(),
        metadata=req.metadata,
    )


# -- SSE ------------------------------------------------------------------------------------------


async def _frames(
    agen: AsyncGenerator[Any], first: Any, run_id: uuid.UUID
) -> AsyncGenerator[bytes]:
    """SSE bytes of one inline run; ends after the terminal frame."""
    encoder = SseEncoder(run_id)
    seq = 0
    terminal = False
    event = first
    async with aclosing(agen):  # leaving early (encoder failure, disconnect) cancels the run
        while event is not None:
            frame = encoder.encode(event)
            seq += 1
            yield format_sse_event(data_str=frame.raw_data, event=frame.event, id=frame.id)
            if frame.event == "error" or isinstance(event, EndEvent | ErrorEvent):
                terminal = True  # an encoder `error` frame also ends the stream
                break
            event = await anext(agen, None)
    if not terminal:
        # Drained for shutdown (or the claim was lost): the run goes on elsewhere; the client polls.
        body = {
            "code": RUN_HANDED_OVER,
            "message": "run handed over to another executor; poll the run for its result",
            "retryable": True,
            "run_id": str(run_id),
        }
        yield format_sse_event(data_str=json.dumps(body), event="error", id=f"{run_id}:{seq + 1}")


class _SseResponse(StreamingResponse):
    """`text/event-stream` with a per-send timeout and a guaranteed close of the run's stream."""

    media_type = "text/event-stream"

    def __init__(
        self,
        frames: AsyncGenerator[bytes],
        stack: AsyncExitStack,
        active: ActiveRuns,
    ) -> None:
        super().__init__(frames, headers=_SSE_HEADERS)
        self._frames = frames
        self._stack = stack
        self._active = active

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        async def bounded(message: Message) -> None:
            with anyio.fail_after(SSE_SEND_TIMEOUT_S):
                await send(message)

        try:
            await super().__call__(scope, receive, bounded)
        except TimeoutError:
            logger.warning("SSE client stalled for %ss; dropping the stream", SSE_SEND_TIMEOUT_S)
        finally:
            # Own task + strong reference (1q): the scope that got cancelled cancels every await.
            await asyncio.shield(self._active.spawn(self._close()))

    async def _close(self) -> None:
        try:
            await self._frames.aclose()  # cancels the run if it is still going
        finally:
            await self._stack.aclose()  # releases the admission slot


async def _stream(
    container: Container, principal: Principal, thread_id: uuid.UUID, req: RunRequest
) -> _SseResponse:
    pinned = await _check_request(container, principal, thread_id, req)
    async with AsyncExitStack() as stack:
        try:
            await stack.enter_async_context(container.limits.admit())
        except TooManyRuns as exc:
            raise RunError(ErrorCode.TOO_MANY_RUNS, str(exc), retryable=True) from None
        run = await _insert(container, thread_id, req, pinned)
        agen = execute_run(container, run.run_id, stream_modes=req.stream_mode)
        try:
            # The claim (thread_busy) happens before the first event: still a plain HTTP error.
            first = await anext(agen, None)
        except RunError as exc:
            if exc.code is ErrorCode.THREAD_BUSY:  # the pending row would stay claimable
                await _runs(container).request_cancel(run.run_id, thread_id)
            raise
        except BaseException:
            await agen.aclose()
            raise
        frames = _frames(agen, first, run.run_id)
        return _SseResponse(frames, stack.pop_all(), container.active_runs)


async def _background(
    container: Container, principal: Principal, thread_id: uuid.UUID, req: RunRequest
) -> JSONResponse:
    queue = container.queue
    if queue is None:
        raise HTTPException(HTTPStatus.SERVICE_UNAVAILABLE, QUEUE_UNAVAILABLE)
    run = await _insert(
        container, thread_id, req, await _check_request(container, principal, thread_id, req)
    )
    # A failed defer leaves the row pending: the orphan sweep re-defers it (design section 5).
    await queue.defer(run.run_id, run.thread_id)
    body = RunAccepted(run_id=run.run_id, thread_id=run.thread_id)
    return JSONResponse(body.model_dump(mode="json"), status_code=HTTPStatus.ACCEPTED)


# -- endpoints ------------------------------------------------------------------------------------


@health.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@health.get("/readyz")
async def readyz(request: Request) -> JSONResponse:
    c = _container(request)
    problems: list[str] = []
    lag, limit = c.lag_sampler.current_ms, c.settings.limits.loop_lag_ready_ms
    if lag > limit:
        problems.append(f"event loop lag {lag:.0f} ms over {limit:.0f} ms")
    for name, pool in (("app", c.app_pool), ("checkpointer", c.checkpointer_pool)):
        if pool is None:
            continue
        try:
            async with pool.connection(timeout=READY_DB_TIMEOUT_S) as conn:
                await conn.execute("SELECT 1")
        except Exception as exc:  # any failure means not ready; the type is the reason
            problems.append(f"{name} pool: {type(exc).__name__}")
    if problems:
        body = {"code": "not_ready", "message": "; ".join(problems), "retryable": True}
        return JSONResponse(body, status_code=HTTPStatus.SERVICE_UNAVAILABLE)
    return JSONResponse({"status": "ready"})


@router.get("/flows")
async def list_flows(c: Ctr) -> list[FlowItem]:
    # Names, versions and descriptions come from the manifest: nothing is compiled to list them.
    return [
        FlowItem(name=name, version=spec.version, description=spec.description)
        for name, spec in c.settings.flows.items()
    ]


@router.post("/runs", status_code=HTTPStatus.ACCEPTED)
async def create_run(req: RunRequest, c: Ctr, who: Caller) -> JSONResponse:
    return await _background(c, who, uuid.uuid4(), req)


@router.post("/runs/stream")
async def stream_new_run(req: RunRequest, c: Ctr, who: Caller) -> _SseResponse:
    return await _stream(c, who, uuid.uuid4(), req)


@router.post("/threads/{thread_id}/runs", status_code=HTTPStatus.ACCEPTED)
async def create_thread_run(
    thread_id: uuid.UUID, req: RunRequest, c: Ctr, who: Caller
) -> JSONResponse:
    return await _background(c, who, thread_id, req)


@router.post("/threads/{thread_id}/runs/stream")
async def stream_thread_run(
    thread_id: uuid.UUID, req: RunRequest, c: Ctr, who: Caller
) -> _SseResponse:
    return await _stream(c, who, thread_id, req)


@router.get("/threads/{thread_id}/runs")
async def list_runs(thread_id: uuid.UUID, c: Ctr, who: Caller) -> list[RunOut]:
    await _pin(c, who, thread_id)
    return [RunOut.from_run(r) for r in await _runs(c).list_for_thread(thread_id)]


@router.get("/threads/{thread_id}/runs/{run_id}")
async def get_run(thread_id: uuid.UUID, run_id: uuid.UUID, c: Ctr, who: Caller) -> RunOut:
    await _pin(c, who, thread_id)
    return RunOut.from_run(await _existing_run(c, run_id, thread_id))


async def _existing_run(c: Container, run_id: uuid.UUID, thread_id: uuid.UUID) -> Run:
    run = await _runs(c).get(run_id, thread_id)
    if run is None:
        raise RunError(ErrorCode.NOT_FOUND, "run not found")
    return run


@router.post("/threads/{thread_id}/runs/{run_id}/cancel", status_code=HTTPStatus.ACCEPTED)
async def cancel_run(thread_id: uuid.UUID, run_id: uuid.UUID, c: Ctr, who: Caller) -> RunOut:
    await _pin(c, who, thread_id)
    await _existing_run(c, run_id, thread_id)
    # None: already terminal, nothing to cancel; the body shows the final state.
    await _runs(c).request_cancel(run_id, thread_id)
    return RunOut.from_run(await _existing_run(c, run_id, thread_id))


@router.get("/threads/{thread_id}/state")
async def thread_state(thread_id: uuid.UUID, c: Ctr, who: Caller) -> ThreadState:
    pin = await _pin(c, who, thread_id)
    compiled = await c.flows.get(pin.flow)
    snapshot = await compiled.graph.aget_state({"configurable": {"thread_id": str(thread_id)}})
    return ThreadState.model_validate(
        {
            "values": snapshot.values,
            "next": list(snapshot.next),
            "interrupts": [{"id": i.id, "value": i.value} for i in snapshot.interrupts],
            "checkpoint_id": snapshot.config.get("configurable", {}).get("checkpoint_id"),
        }
    )


@router.delete("/threads/{thread_id}", status_code=HTTPStatus.NO_CONTENT)
async def delete_thread(thread_id: uuid.UUID, c: Ctr, who: Caller) -> None:
    await _pin(c, who, thread_id)
    await _runs(c).delete_thread(thread_id)  # thread_busy (409) while a run is running
    await c.checkpointer.adelete_thread(str(thread_id))
