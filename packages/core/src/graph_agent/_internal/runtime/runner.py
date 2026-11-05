"""The single runner (design section 5).

`stream_graph` is the database-free core: it drives one `astream` and maps the parts to
`RunEvent`s. `run --memory` and `testing.run_flow` call it directly. `execute_run` wraps it with
the runs table: claim, heartbeat, run timeout, cancellation, drain and the fenced terminal write.
"""

import asyncio
import logging
import os
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Sequence
from contextlib import aclosing
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final, Protocol, cast

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, LLMResult
from langchain_core.runnables import RunnableConfig
from langgraph.errors import GraphDrained
from langgraph.runtime import RunControl
from langgraph.types import Command, StreamMode

from graph_agent._internal.config.settings import Settings
from graph_agent._internal.runtime.checkpointer import Checkpointer
from graph_agent._internal.runtime.context import RunContext
from graph_agent._internal.runtime.events import (
    CustomEvent,
    EndEvent,
    ErrorEvent,
    InterruptEvent,
    MessagesEvent,
    MetadataEvent,
    RunEvent,
    RunObserver,
    RunOutcome,
    UpdatesEvent,
    UsageTotals,
    ValuesEvent,
    map_stream_part,
)
from graph_agent._internal.runtime.flows import CompiledFlow, FlowRegistry, check_version
from graph_agent._internal.runtime.flows import RunError as FlowRunError
from graph_agent._internal.runtime.runs import (
    ClaimOutcome,
    ErrorCode,
    Run,
    RunError,
    RunsRepository,
    RunStatus,
)

logger = logging.getLogger(__name__)

DEFAULT_STREAM_MODES: Final = ("updates", "custom")
# `updates` is always requested from LangGraph (interrupts arrive as `updates.__interrupt__`);
# events are filtered to what the client asked for afterwards.
_MODE_OF_EVENT: Final[dict[type, str]] = {
    UpdatesEvent: "updates",
    ValuesEvent: "values",
    MessagesEvent: "messages",
    CustomEvent: "custom",
}
CLIENT_STREAM_MODES: Final = frozenset(_MODE_OF_EVENT.values())
_OWNER: Final = f"pid{os.getpid()}"


class RunQueue(Protocol):
    """Defers `execute_run` for a run (T24); background endpoints and the drain use it."""

    async def defer(self, run_id: uuid.UUID, thread_id: uuid.UUID) -> None: ...


class RunnerResources(Protocol):
    """The container slice `execute_run` needs (runtime cannot import bootstrap)."""

    settings: Settings
    runs: RunsRepository | None
    flows: FlowRegistry
    checkpointer: Checkpointer
    observers: list[RunObserver]
    queue: RunQueue | None


class UsageCollector(BaseCallbackHandler):
    """Sums token usage over every chat-model call of a run, subagents included."""

    run_inline = True  # sync handler called on the loop, no executor hop and no data race

    def __init__(self) -> None:
        self._input = self._output = self._cached = 0

    def on_llm_end(self, response: LLMResult, **kwargs: Any) -> None:
        for generations in response.generations:
            for gen in generations:
                msg = gen.message if isinstance(gen, ChatGeneration) else None
                if isinstance(msg, AIMessage) and (usage := msg.usage_metadata) is not None:
                    self._input += usage["input_tokens"]
                    self._output += usage["output_tokens"]
                    details = usage.get("input_token_details") or {}
                    self._cached += details.get("cache_read", 0)

    def totals(self) -> UsageTotals:
        return UsageTotals(self._input, self._output, self._cached)


def metadata_event(ctx: RunContext) -> MetadataEvent:
    return MetadataEvent(ctx.run_id, ctx.thread_id, ctx.flow, ctx.flow_version, ctx.attempt)


async def stream_graph(
    compiled: CompiledFlow,
    input_or_command: Any,
    config: RunnableConfig,
    ctx: RunContext,
    stream_modes: Sequence[str],
    observers: Sequence[RunObserver] = (),
    *,
    control: RunControl | None = None,
    usage: UsageCollector | None = None,
) -> AsyncGenerator[RunEvent]:
    """Run the graph once and yield `metadata`, the mapped events and a final `end`.

    No database. Errors (including `GraphDrained`) propagate to the caller. `config` must not
    carry callbacks: the usage collector and the observers' callbacks are installed here.
    `on_finish` is the caller's job (`execute_run` calls it after the terminal write).
    """
    if unknown := set(stream_modes) - CLIENT_STREAM_MODES:
        raise ValueError(f"unknown stream modes {sorted(unknown)}")
    usage = usage or UsageCollector()
    run_config: RunnableConfig = {
        **config,
        "callbacks": [usage, *(cb for obs in observers for cb in obs.callbacks(ctx))],
    }
    modes = cast("list[StreamMode]", list(dict.fromkeys(["updates", *stream_modes])))
    parts = compiled.graph.astream(
        input_or_command,
        run_config,
        context=ctx,
        control=control,
        stream_mode=modes,
        subgraphs=True,
        version="v2",
        durability=compiled.durability,
    )
    yield metadata_event(ctx)
    interrupted = False
    # astream is an async generator at runtime; its typed return hides `aclose`.
    async with aclosing(cast("AsyncGenerator[Any]", parts)):
        async for part in parts:
            event = map_stream_part(part)
            if event is None:
                continue
            if isinstance(event, InterruptEvent):
                interrupted = True
            elif _MODE_OF_EVENT[type(event)] not in stream_modes:
                continue
            yield event
    status = RunStatus.INTERRUPTED if interrupted else RunStatus.SUCCESS
    yield EndEvent(status.value, usage.totals())


class _StopReason(StrEnum):
    CANCEL = "cancel"  # cancel_requested seen by the heartbeat
    LOST = "lost"  # the lease is gone: another executor owns the run


@dataclass(slots=True)
class _Stop:
    reason: _StopReason | None = None
    timed_out: bool = False


class _Done:
    """Queue sentinel: the pump task finished (inspect the task for how)."""


_DONE: Final = _Done()


async def execute_run(
    container: RunnerResources,
    run_id: uuid.UUID,
    *,
    stream_modes: Sequence[str] = DEFAULT_STREAM_MODES,
    owner: str = _OWNER,
) -> AsyncGenerator[RunEvent]:
    """Claim and run one run, yielding its events; the only way a flow runs against Postgres.

    Yields nothing if the claim is lost (another executor owns it) or if the lease is lost and
    the drain ends the stream silently. Raises `RunError(thread_busy, retryable)` when another run
    of the thread is running (HTTP 409; the worker retries with backoff). Closing the generator
    (client disconnect) cancels the run. Memory mode has no runs repository: raises.
    """
    runs = container.runs
    if runs is None:
        raise RuntimeError("execute_run needs the runs repository; memory mode uses stream_graph")
    cfg = container.settings.runs
    claim = await runs.claim(run_id, owner, cfg.lease_s, cfg.max_attempts)
    if claim.outcome is ClaimOutcome.LOST:
        return
    if claim.outcome is ClaimOutcome.THREAD_BUSY:
        raise RunError(ErrorCode.THREAD_BUSY, "thread has a running run", retryable=True)
    if claim.outcome is ClaimOutcome.CANCELLED:
        yield EndEvent(RunStatus.CANCELLED.value, UsageTotals())
        return
    run, token = claim.run, claim.token
    assert run is not None and token is not None  # CLAIMED always carries both
    ctx = RunContext(
        str(run.run_id), str(run.thread_id), run.flow, run.flow_version, run.attempt, run.tenant_id
    )
    usage = UsageCollector()

    try:
        flow = await container.flows.get(run.flow)
        check_version(flow, run.flow_version)
    except FlowRunError as exc:
        error = RunError(ErrorCode(exc.code), exc.message, exc.retryable)
        if await _finish(container, ctx, token, RunStatus.ERROR, usage, error):
            yield metadata_event(ctx)
            yield _error_event(error)
        return

    config: RunnableConfig = {
        "configurable": {"thread_id": ctx.thread_id},
        # Platform keys win: a client cannot spoof the recovery check's `run_id`.
        "metadata": {**run.metadata, "run_id": ctx.run_id, "thread_id": ctx.thread_id},
    }
    if flow.max_concurrency is not None:
        config["max_concurrency"] = flow.max_concurrency
    control = RunControl()  # one per attempt (S1): a drained control stays drained
    stop = _Stop()
    # shortcut: unbounded queue, a slow SSE client buffers events in memory; bound it (and
    # block the pump) if long runs with big `values` streams show up.
    queue: asyncio.Queue[RunEvent | _Done] = asyncio.Queue()
    stream = stream_graph(
        flow,
        await _initial_input(container.checkpointer, run),
        config,
        ctx,
        stream_modes,
        container.observers,
        control=control,
        usage=usage,
    )
    pump = asyncio.create_task(_pump(stream, queue, flow.timeout_s, stop), name=f"run-{run_id}")
    beat = asyncio.create_task(_heartbeat(runs, run, token, container, pump, stop))
    end: EndEvent | None = None
    try:
        while not isinstance(item := await queue.get(), _Done):
            if isinstance(item, EndEvent):
                end = item  # held back until the terminal row is written
            else:
                yield item
    except (GeneratorExit, asyncio.CancelledError):
        # Client went away (or the executor task was cancelled): cancel the run, best effort.
        await _stop_tasks(pump, beat)
        try:
            await _finish(container, ctx, token, RunStatus.CANCELLED, usage)
        except Exception:
            logger.exception("run %s: could not record cancellation", run_id)
        raise
    await _stop_tasks(beat)  # no lease extension after the stream ended
    await asyncio.wait({pump})

    if pump.cancelled():  # only the heartbeat cancels the pump while we are still listening
        if stop.reason is _StopReason.CANCEL:
            if await _finish(container, ctx, token, RunStatus.CANCELLED, usage):
                yield EndEvent(RunStatus.CANCELLED.value, usage.totals())
            return
        logger.warning("run %s: lease lost, stopping without a terminal write", run_id)
        yield _error_event(RunError(ErrorCode.INTERNAL_ERROR, "run lease lost", retryable=True))
        return
    failure = pump.exception()

    if failure is None:
        assert end is not None  # stream_graph always ends with an `end` event
        if await _finish(container, ctx, token, RunStatus(end.status), usage):
            yield end
        return
    if isinstance(failure, GraphDrained):  # a GraphBubbleUp, not a failure: hand the run over
        await _drain(runs, container.queue, run, token)
        return
    if stop.timed_out:
        error = RunError(ErrorCode.RUN_TIMEOUT, f"run exceeded {flow.timeout_s}s", retryable=False)
    else:
        # Client message: exception type only (user exceptions may carry secrets).
        logger.error("run %s failed", run_id, exc_info=failure)
        error = RunError(ErrorCode.INTERNAL_ERROR, type(failure).__name__, retryable=False)
    if await _finish(container, ctx, token, RunStatus.ERROR, usage, error):
        yield _error_event(error)


async def _initial_input(checkpointer: Checkpointer, run: Run) -> Any:
    """`None` if the latest checkpoint already carries this run_id (recovery or drain re-claim)."""
    latest = await checkpointer.aget_tuple({"configurable": {"thread_id": str(run.thread_id)}})
    if latest is not None and latest.metadata.get("run_id") == str(run.run_id):
        return None
    if run.command is not None:
        assert isinstance(run.command, dict)  # stored as {"resume": ...} (api.dto.ResumeCommand)
        return Command(resume=run.command["resume"])
    return run.input


async def _pump(
    stream: AsyncIterator[RunEvent],
    queue: asyncio.Queue[RunEvent | _Done],
    timeout_s: float,
    stop: _Stop,
) -> None:
    """Drain the stream into the queue under the run timeout; always ends with `_DONE`."""
    timeout = asyncio.timeout(timeout_s)
    try:
        async with timeout:
            async for event in stream:
                queue.put_nowait(event)
    finally:
        stop.timed_out = timeout.expired()
        queue.put_nowait(_DONE)


async def _heartbeat(
    runs: RunsRepository,
    run: Run,
    token: str,
    container: RunnerResources,
    pump: "asyncio.Task[None]",
    stop: _Stop,
) -> None:
    cfg = container.settings.runs
    while True:
        await asyncio.sleep(cfg.heartbeat_s)
        try:
            cancel_requested = await runs.heartbeat(run.run_id, token, cfg.lease_s)
        except Exception:  # a transient DB error must not kill the run; a lost lease shows up next
            logger.warning("run %s: heartbeat failed", run.run_id, exc_info=True)
            continue
        if cancel_requested is None:
            stop.reason = _StopReason.LOST
        elif cancel_requested:
            stop.reason = _StopReason.CANCEL
        else:
            continue
        pump.cancel()
        return


async def _stop_tasks(*tasks: "asyncio.Task[None]") -> None:
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def _finish(
    container: RunnerResources,
    ctx: RunContext,
    token: str,
    status: RunStatus,
    usage: UsageCollector,
    error: RunError | None = None,
) -> bool:
    """Fenced terminal write, then the observers. False: the lease was lost, nothing written."""
    assert container.runs is not None
    if not await container.runs.finish(uuid.UUID(ctx.run_id), token, status, error):
        logger.warning("run %s: lease lost before the terminal write", ctx.run_id)
        return False
    outcome = RunOutcome(
        status.value, usage.totals(), None if error is None else _error_event(error)
    )
    for observer in container.observers:
        try:
            await observer.on_finish(ctx, outcome)
        except Exception:  # an observer (ledger) must never change the run's outcome
            logger.exception("run %s: observer %r failed", ctx.run_id, observer)
    return True


async def _drain(runs: RunsRepository, queue: RunQueue | None, run: Run, token: str) -> None:
    """Graceful shutdown: expire the lease, give the attempt back and defer; no terminal event."""
    if not await runs.release_for_drain(run.run_id, token):
        logger.warning("run %s: lease lost before the drain release", run.run_id)
        return
    if queue is None:
        logger.warning("run %s drained but no queue to defer it; the orphan sweep will", run.run_id)
        return
    try:
        await queue.defer(run.run_id, run.thread_id)
    except Exception:  # the lease is already expired: the orphan sweep re-defers it
        logger.exception("run %s: could not defer after drain", run.run_id)


def _error_event(error: RunError) -> ErrorEvent:
    return ErrorEvent(error.code.value, error.message, error.retryable)
