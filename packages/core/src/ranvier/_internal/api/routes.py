"""HTTP endpoints (design section 6): runs, SSE streams, thread state, admission."""

import asyncio
import logging
import uuid
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack
from http import HTTPStatus
from typing import Annotated, Any, Final

import anyio
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from fastapi.sse import format_sse_event
from starlette.responses import StreamingResponse
from starlette.types import Message, Receive, Scope, Send

from ranvier._internal.api.auth import Principal, require_principal
from ranvier._internal.api.dto import (
    FlowItem,
    RunAccepted,
    RunOut,
    RunRequest,
    SseEncoder,
    ThreadState,
    error_response,
)
from ranvier._internal.bootstrap import Container
from ranvier._internal.limits import ShuttingDown, TooManyRuns
from ranvier._internal.runtime.events import EndEvent, ErrorEvent
from ranvier._internal.runtime.runner import (
    ActiveRuns,
    cancel_pending_run,
    check_tenant,
    execute_run,
    prepare_thread_run,
)
from ranvier._internal.runtime.runs import (
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
READY_DB_TIMEOUT_S: Final = 2.0
# Proxies with an idle timeout cut a stream that stays silent through a long node.
SSE_PING_INTERVAL_S: Final = 15.0
_SSE_PING: Final = b": ping\n\n"  # an SSE comment: clients ignore it
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


async def _prepare(
    container: Container, principal: Principal, thread_id: uuid.UUID, req: RunRequest
) -> ThreadPin:
    return await prepare_thread_run(
        container,
        _runs(container),
        flow=req.flow,
        thread_id=thread_id,
        tenant_id=principal.tenant_id,
        input=req.input,
        resume=None if req.command is None else req.command.resume,
    )


async def _pin(container: Container, principal: Principal, thread_id: uuid.UUID) -> ThreadPin:
    pin = await _runs(container).thread_pin(thread_id)
    if pin is None:
        raise RunError(ErrorCode.NOT_FOUND, "thread not found")
    check_tenant(pin, principal.tenant_id)
    return pin


async def _insert(
    container: Container,
    thread_id: uuid.UUID,
    req: RunRequest,
    pin: ThreadPin,
) -> Run:
    return await _runs(container).create(
        thread_id=thread_id,
        flow=req.flow,
        flow_version=pin.flow_version,
        tenant_id=pin.tenant_id,
        input=req.input,
        command=None if req.command is None else req.command.model_dump(),
        metadata=req.metadata,
    )


# -- SSE ------------------------------------------------------------------------------------------


async def _reap(task: "asyncio.Task[Any]") -> None:
    """Cancel `task` and wait for it to finish, shielded from a cancelled anyio scope.

    Such a scope re-cancels the waiting task on every loop iteration (1q); the shield makes this
    one wait. A native cancel still propagates, leaving `task` unfinished.
    """
    task.cancel()
    with anyio.CancelScope(shield=True):
        await asyncio.wait({task})


async def _frames(
    agen: AsyncGenerator[Any], first: Any, run_id: uuid.UUID
) -> AsyncGenerator[bytes]:
    """SSE bytes of one inline run; ends after the terminal frame."""
    encoder = SseEncoder(run_id)
    terminal = False
    event = first
    # The next event is awaited in a task so the wait can time out into a keepalive.
    pending: asyncio.Task[Any] | None = None
    try:  # leaving early (encoder failure, disconnect) closes `agen`, which cancels the run
        try:
            while event is not None:
                frame = encoder.encode(event)
                yield format_sse_event(data_str=frame.raw_data, event=frame.event, id=frame.id)
                if frame.event == "error" or isinstance(event, EndEvent | ErrorEvent):
                    terminal = True  # an encoder `error` frame also ends the stream
                    break
                pending = asyncio.ensure_future(anext(agen, None))
                while not (await asyncio.wait({pending}, timeout=SSE_PING_INTERVAL_S))[0]:
                    yield _SSE_PING
                ready, pending = pending, None  # a failure propagates; the finally has nothing
                event = ready.result()
        finally:
            if pending is not None:
                await _reap(pending)  # before `aclose`: "generator already running" otherwise
                if not pending.cancelled() and (exc := pending.exception()) is not None:
                    logger.error("run %s: stream failed while closing", run_id, exc_info=exc)
    finally:
        # A native cancel during the reap leaves the task unwinding: it closes `agen` itself.
        if pending is None or pending.done():
            await agen.aclose()
    if not terminal:
        # Drained for shutdown (or the claim was lost): the run goes on elsewhere; the client polls.
        frame = encoder.handed_over()
        yield format_sse_event(data_str=frame.raw_data, event=frame.event, id=frame.id)


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
        send_timed_out = False

        async def bounded(message: Message) -> None:
            nonlocal send_timed_out
            try:
                with anyio.fail_after(SSE_SEND_TIMEOUT_S):
                    await send(message)
            except TimeoutError:
                send_timed_out = True
                raise

        try:
            await super().__call__(scope, receive, bounded)
        except TimeoutError:
            if not send_timed_out:  # a TimeoutError from the stream itself is a real failure
                raise
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
    pin = await _prepare(container, principal, thread_id, req)
    async with AsyncExitStack() as stack:
        try:
            await stack.enter_async_context(container.limits.admit())
        except TooManyRuns as exc:
            raise RunError(ErrorCode.TOO_MANY_RUNS, str(exc), retryable=True) from None
        except ShuttingDown as exc:
            raise RunError(ErrorCode.SHUTTING_DOWN, str(exc), retryable=True) from None
        run = await _insert(container, thread_id, req, pin)
        agen = execute_run(container, run.run_id, stream_modes=req.stream_mode)
        try:
            # The claim (thread_busy) happens before the first event: still a plain HTTP error.
            first = await anext(agen, None)
        except BaseException:
            # Claim refused, DB error, handler cancelled: the client gets an error, so the row
            # must not stay claimable.
            try:
                await cancel_pending_run(container, _runs(container), run)
            finally:
                await agen.aclose()
            raise
        frames = _frames(agen, first, run.run_id)
        return _SseResponse(frames, stack.pop_all(), container.active_runs)


async def _background(
    container: Container, principal: Principal, thread_id: uuid.UUID, req: RunRequest
) -> RunAccepted:
    queue = container.queue
    if queue is None:
        raise RunError(ErrorCode.QUEUE_UNAVAILABLE, "no job queue in this process", retryable=True)
    run = await _insert(
        container, thread_id, req, await _prepare(container, principal, thread_id, req)
    )
    try:
        await queue.defer(run.run_id, run.thread_id)
    except BaseException as exc:
        # The client sees an error, so the row must not run behind its back (cancel_pending_run).
        await cancel_pending_run(container, _runs(container), run)
        if not isinstance(exc, Exception):
            raise
        logger.error("could not defer run %s", run.run_id, exc_info=exc)
        raise RunError(
            ErrorCode.QUEUE_UNAVAILABLE, "could not enqueue the run; retry", retryable=True
        ) from None
    return RunAccepted(run_id=run.run_id, thread_id=run.thread_id)


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
            async with asyncio.timeout(READY_DB_TIMEOUT_S), pool.connection() as conn:
                await conn.execute("SELECT 1")
        except Exception as exc:  # any failure means not ready; the type is the reason
            problems.append(f"{name} pool: {type(exc).__name__}")
    if problems:
        return error_response(
            HTTPStatus.SERVICE_UNAVAILABLE, "not_ready", "; ".join(problems), retryable=True
        )
    return JSONResponse({"status": "ready"})


@router.get("/flows")
async def list_flows(c: Ctr) -> list[FlowItem]:
    # Names, versions and descriptions come from the manifest: nothing is compiled to list them.
    return [
        FlowItem(name=name, version=spec.version, description=spec.description)
        for name, spec in c.settings.flows.items()
    ]


@router.post("/runs", status_code=HTTPStatus.ACCEPTED)
async def create_run(req: RunRequest, c: Ctr, who: Caller) -> RunAccepted:
    return await _background(c, who, uuid.uuid4(), req)


@router.post("/runs/stream")
async def stream_new_run(req: RunRequest, c: Ctr, who: Caller) -> _SseResponse:
    return await _stream(c, who, uuid.uuid4(), req)


@router.post("/threads/{thread_id}/runs", status_code=HTTPStatus.ACCEPTED)
async def create_thread_run(
    thread_id: uuid.UUID, req: RunRequest, c: Ctr, who: Caller
) -> RunAccepted:
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
    """Delete the thread's checkpoints and runs; refused (409) while a run is running.

    Queued (pending) runs are discarded with the rows; their Procrastinate jobs exit at the
    claim (LOST) without a trace.
    """
    await _pin(c, who, thread_id)
    runs = _runs(c)
    # Checkpoints go before the rows: the rows are what makes a retry find the thread (and keeps
    # the section 5 rules for a reused thread_id) if the checkpoint delete fails.
    if await runs.has_running(thread_id):
        raise RunError(ErrorCode.THREAD_BUSY, "thread has a running run", retryable=True)
    await c.checkpointer.adelete_thread(str(thread_id))
    # shortcut: a run that starts between the check and here loses its checkpoints (the delete
    # below still answers thread_busy); upgrade to a per-thread lock if live threads get deleted.
    await runs.delete_thread(thread_id)
