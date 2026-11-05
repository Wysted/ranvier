"""The single runner (design section 5).

`stream_graph` is the database-free core: it drives one `astream` and maps the parts to
`RunEvent`s. `run --memory` and `testing.run_flow` call it directly. `execute_run` wraps it with
the runs table: claim, heartbeat, run timeout, cancellation, drain and the fenced terminal write.
"""

import asyncio
import contextvars
import logging
import math
import os
import re
import socket
import sys
import uuid
from collections.abc import AsyncGenerator, Coroutine, Iterator, Mapping, Sequence
from contextlib import aclosing, contextmanager
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final, Literal, Protocol, TypeGuard, cast

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import CheckpointTuple
from langgraph.errors import GraphDrained
from langgraph.runtime import RunControl
from langgraph.types import Command, StreamMode

from graph_agent._internal.config.settings import Settings
from graph_agent._internal.observability.tracing import Observability
from graph_agent._internal.runtime.checkpointer import Checkpointer
from graph_agent._internal.runtime.context import RunContext
from graph_agent._internal.runtime.events import (
    INTERRUPT_KEY,
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
from graph_agent._internal.runtime.runs import (
    ClaimOutcome,
    ErrorCode,
    Run,
    RunError,
    RunsRepository,
    RunStatus,
    ThreadPin,
)
from graph_agent._internal.usage import Counts, UsageCallback, sum_counts

logger = logging.getLogger(__name__)

DEFAULT_STREAM_MODES: Final[tuple[Literal["updates", "custom"], ...]] = ("updates", "custom")
# `updates` is always requested from LangGraph (interrupts arrive as `updates.__interrupt__`);
# events are filtered to what the client asked for afterwards.
_MODE_OF_EVENT: Final[dict[type, str]] = {
    UpdatesEvent: "updates",
    ValuesEvent: "values",
    MessagesEvent: "messages",
    CustomEvent: "custom",
}
CLIENT_STREAM_MODES: Final = frozenset(_MODE_OF_EVENT.values())
RUN_EVENT_BUFFER: Final = 256  # events between the stream and a slow consumer (backpressure)
_OWNER: Final = f"{socket.gethostname()}:{os.getpid()}"
DRAIN_REASON: Final = "shutdown"
METRIC_ATTEMPTS: Final = "run_attempts"  # counter, one per attempt, by flow and status
# shortcut: sum counter (mean only), histogram when the Metrics port grows one
METRIC_ATTEMPT_SECONDS: Final = "run_attempt_seconds"
METRIC_CANCELLED_BEFORE_START: Final = (
    "runs_cancelled_before_start"  # claims that found it cancelled
)
STATUS_RELEASED: Final = "released"  # the attempt ended without a terminal write


class RunQueue(Protocol):
    """Defers `execute_run` for a run (T24); background endpoints and the drain use it."""

    async def defer(
        self, run_id: uuid.UUID, thread_id: uuid.UUID, *, recovery: bool = False
    ) -> None:
        """`recovery=True`: a run that was `running` (hand-over, lease re-defer) jumps the queue."""
        ...


class ActiveRuns:
    """Controls of the attempts in flight in this process: the shutdown drain (T26) reaches them.

    Also tracks every run from its claim until its cleanup finished (`settled`), including the
    detached cleanup tasks that outlive a cancelled consumer.
    """

    def __init__(self) -> None:
        self._controls: set[RunControl] = set()
        self._busy: set[object] = set()  # attempts and cleanup tasks (strong refs)
        self._idle = asyncio.Event()
        self._idle.set()
        self.draining = False

    def begin(self, item: object) -> None:
        self._busy.add(item)
        self._idle.clear()

    def end(self, item: object) -> None:
        self._busy.discard(item)
        if not self._busy:
            self._idle.set()

    def spawn(self, coro: Coroutine[Any, Any, Any]) -> "asyncio.Task[Any]":
        """Run `coro` as a tracked task with a strong reference (the loop keeps only weak ones)."""
        task = asyncio.create_task(coro)
        self.begin(task)
        task.add_done_callback(self.end)
        return task

    async def settled(self) -> None:
        """Wait until every tracked run and cleanup finished (T26: before closing the pools)."""
        await self._idle.wait()

    @contextmanager
    def track(self) -> Iterator[RunControl]:
        """A fresh `RunControl` for one attempt (S1: a drained control stays drained)."""
        control = RunControl()
        if self.draining:  # claimed while shutting down: stop after the first superstep
            control.request_drain(DRAIN_REASON)
        self._controls.add(control)
        try:
            yield control
        finally:
            self._controls.discard(control)

    def request_drain(self) -> None:
        """Ask every attempt in flight to stop at its next superstep boundary (and new ones too)."""
        self.draining = True
        for control in list(self._controls):
            control.request_drain(DRAIN_REASON)

    def __len__(self) -> int:  # T33 reads it as the active_attempts gauge
        return len(self._controls)


class _Budget(Protocol):
    """`bootstrap.ShutdownBudget` (runtime cannot import bootstrap)."""

    deadline: float  # loop time; `math.inf` until shutdown starts

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        """Start the shared clock (idempotent: the first call counts)."""
        ...


class RunnerResources(Protocol):
    """The container slice `execute_run` needs (runtime cannot import bootstrap)."""

    settings: Settings
    runs: RunsRepository | None
    flows: FlowRegistry
    checkpointer: Checkpointer
    observers: list[RunObserver]
    observability: Observability
    queue: RunQueue | None
    active_runs: ActiveRuns

    @property
    def shutdown_budget(self) -> _Budget: ...


def start_drain(active: ActiveRuns, budget: _Budget) -> None:
    """Begin the shutdown: start the shared clock, then ask every attempt to drain (idempotent).

    Callers stop their intake first (the worker stops fetching, the API closes admission): a claim
    in flight is not in `active` yet, and one that lands later drains after its first superstep.
    """
    budget.start(asyncio.get_running_loop())
    active.request_drain()


def _until(budget: _Budget) -> float | None:
    """`timeout_at` argument for the shutdown deadline (None while it has not started)."""
    return None if math.isinf(budget.deadline) else budget.deadline


async def settle_runs(active: ActiveRuns, budget: _Budget) -> bool:
    """Wait until every tracked run and cleanup finished, at most until the shutdown deadline.

    False: the deadline passed first; the stragglers keep running and their leases expire for the
    sweep. Close the pools only after this returns.
    """
    try:
        async with asyncio.timeout_at(_until(budget)):
            await active.settled()
    except TimeoutError:
        logger.error("runs still active at the shutdown deadline; lease expiry recovers them")
        return False
    return True


def metadata_event(ctx: RunContext) -> MetadataEvent:
    return MetadataEvent(ctx.run_id, ctx.thread_id, ctx.flow, ctx.flow_version, ctx.attempt)


def _check_modes(stream_modes: Sequence[str]) -> None:
    if unknown := set(stream_modes) - CLIENT_STREAM_MODES:
        raise ValueError(f"unknown stream modes {sorted(unknown)}")


def _totals(counts: Counts) -> UsageTotals:
    return UsageTotals(counts.input_tokens, counts.output_tokens, counts.cached_input_tokens)


async def stream_graph(
    compiled: CompiledFlow,
    input_or_command: Any,
    config: RunnableConfig,
    ctx: RunContext,
    stream_modes: Sequence[str],
    observers: Sequence[RunObserver] = (),
    *,
    control: RunControl | None = None,
    usage: UsageCallback | None = None,
) -> AsyncGenerator[RunEvent]:
    """Run the graph once and yield `metadata`, the mapped events and a final `end`.

    No database and no run timeout (the caller wraps one; `execute_run` does). Errors (including
    `GraphDrained`) propagate to the caller. `config` must not carry callbacks: the usage callback
    (the caller's, to read the totals afterwards, or a fresh one) and the observers' callbacks are
    installed here. `on_finish` is the caller's job (`execute_run` calls it after the terminal
    write).
    """
    _check_modes(stream_modes)
    if usage is None:
        usage = UsageCallback()
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
    yield EndEvent(status.value, _totals(usage.totals()))


class _StopReason(StrEnum):
    CANCEL = "cancel"  # cancel_requested seen by the heartbeat
    LOST = "lost"  # the lease is gone: another executor owns the run


@dataclass(slots=True, eq=False)
class _Attempt:
    """Mutable state shared by the consumer, the pump and the heartbeat of one attempt."""

    pump: "asyncio.Task[None] | None" = None
    reason: _StopReason | None = None
    timed_out: bool = False
    settled: bool = False  # the run row (or its lease) is already dealt with: no cleanup write
    counted: bool = False  # the attempt is already in `run_attempts`

    def stop(self, reason: _StopReason) -> None:
        self.reason = reason
        if self.pump is not None:
            self.pump.cancel()


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
    of the thread is running (HTTP 409; the worker retries with backoff). An inline caller that
    gets it must `request_cancel` the pending row it created, or it stays claimable. Closing the
    generator (client disconnect) or cancelling its task cancels the run: the cleanup runs in
    a separate shielded task bounded by the shutdown budget. Memory mode has no runs repository:
    raises.

    While the container drains, any cancellation (`CancelledError` while waiting on `queue.get`,
    `GeneratorExit` when closed at a `yield`, e.g. the server closing streams at its own timeout)
    hands the run over (release + defer) instead of recording it `cancelled` (T26).
    """
    _check_modes(stream_modes)
    runs = container.runs
    if runs is None:
        raise RuntimeError("execute_run needs the runs repository; memory mode uses stream_graph")
    cfg = container.settings.runs
    loop = asyncio.get_running_loop()
    claimed_at = loop.time()  # the lease runs from the claim: heartbeats are measured against it
    claim = await runs.claim(run_id, owner, cfg.lease_s, cfg.max_attempts)
    if claim.outcome is ClaimOutcome.LOST:
        return
    if claim.outcome is ClaimOutcome.THREAD_BUSY:
        raise RunError(ErrorCode.THREAD_BUSY, "thread has a running run", retryable=True)
    run, token = claim.run, claim.token
    if run is None:
        raise RuntimeError(f"claim {claim.outcome} of run {run_id} carries no run")
    ctx = RunContext(
        str(run.run_id), str(run.thread_id), run.flow, run.flow_version, run.attempt, run.tenant_id
    )
    if claim.outcome is ClaimOutcome.CANCELLED:
        container.observability.metrics.counter(
            METRIC_CANCELLED_BEFORE_START, 1, {"flow": ctx.flow}
        )
        yield metadata_event(ctx)
        yield EndEvent(RunStatus.CANCELLED.value, UsageTotals())
        return
    if token is None:
        raise RuntimeError(f"claimed run {run_id} carries no lease token")

    usage = UsageCallback()  # per attempt: attempts accumulate in the ledger
    attempt = _Attempt()
    # The run span lives in its own context, not the consumer's: the generator can be closed from
    # another task (cancelled SSE handler, GC), where detaching a token set here would fail.
    span_ctx = contextvars.copy_context()
    span = container.observability.run_span(
        flow=ctx.flow, run_id=ctx.run_id, thread_id=ctx.thread_id
    )
    span_ctx.run(span.__enter__)  # before `begin`: a failing span must not strand the attempt
    container.active_runs.begin(attempt)
    beat = asyncio.create_task(
        _heartbeat(runs, run, token, container, attempt, claimed_at), name=f"beat-{run_id}"
    )
    try:
        try:
            flow = await container.flows.get(run.flow)
            check_version(flow, run.flow_version)
            # The request-time check can be stale: runs queued behind an interrupted one.
            initial = await _initial_input(container.checkpointer, run)
        except RunError as run_error:  # flow_*, thread_interrupted, invalid_request
            await _stop_tasks(beat)
            ok = await _settle(
                container,
                attempt,
                _finish(container, attempt, runs, ctx, token, RunStatus.ERROR, usage, run_error),
            )
            if ok:
                yield metadata_event(ctx)
                yield _error_event(run_error)
            return

        config = run_config(ctx, flow, run.metadata)
        queue: asyncio.Queue[RunEvent] = asyncio.Queue(RUN_EVENT_BUFFER)
        with container.active_runs.track() as control:
            stream = stream_graph(
                flow,
                initial,
                config,
                ctx,
                stream_modes,
                container.observers,
                control=control,
                usage=usage,
            )
            pump = asyncio.create_task(
                _pump(stream, queue, flow.timeout_s, attempt),
                name=f"run-{run_id}",
                context=span_ctx,  # LangChain spans parent to the run span
            )
            attempt.pump = pump
            # Also fires if the task is cancelled before it ever starts.
            pump.add_done_callback(lambda _task: queue.shutdown())
            if attempt.reason is not None:  # the heartbeat decided while the flow was loading
                pump.cancel()
            end: EndEvent | None = None
            while True:
                try:
                    item = await queue.get()
                except asyncio.QueueShutDown:
                    break
                if isinstance(item, EndEvent):
                    end = item  # held back until the terminal row is written
                else:
                    yield item
            await _stop_tasks(beat)  # no lease extension after the stream ended
            await asyncio.wait({pump})

        if pump.cancelled():  # the heartbeat, or a node that propagated its own CancelledError
            if attempt.reason is _StopReason.CANCEL:
                ok = await _settle(
                    container,
                    attempt,
                    _finish(container, attempt, runs, ctx, token, RunStatus.CANCELLED, usage),
                )
                if ok:
                    yield EndEvent(RunStatus.CANCELLED.value, _totals(usage.totals()))
                return
            if attempt.reason is _StopReason.LOST:
                logger.warning("run %s: lease lost, stopping without a terminal write", run_id)
                await _settle(
                    container, attempt, _notify(container, attempt, ctx, RunStatus.RUNNING, usage)
                )
                yield _error_event(
                    RunError(ErrorCode.INTERNAL_ERROR, "run lease lost", retryable=True)
                )
                return
            failure: BaseException | None = asyncio.CancelledError()
        else:
            failure = pump.exception()

        if failure is None:
            if end is None:  # stream_graph always ends with an `end` event
                raise RuntimeError(f"run {run_id}: the stream ended without an end event")
            ok = await _settle(
                container,
                attempt,
                _finish(container, attempt, runs, ctx, token, RunStatus(end.status), usage),
            )
            if ok:
                yield end
            return
        if isinstance(failure, GraphDrained):  # a GraphBubbleUp, not a failure: hand the run over
            await _settle(
                container, attempt, _hand_over(container, attempt, runs, run, ctx, token, usage)
            )
            return
        if attempt.timed_out:
            error = RunError(
                ErrorCode.RUN_TIMEOUT, f"run exceeded {flow.timeout_s}s", retryable=False
            )
        else:
            # Client message: exception type only (user exceptions may carry secrets).
            logger.error("run %s failed", run_id, exc_info=failure)
            error = RunError(ErrorCode.INTERNAL_ERROR, type(failure).__name__, retryable=False)
        ok = await _settle(
            container,
            attempt,
            _finish(container, attempt, runs, ctx, token, RunStatus.ERROR, usage, error),
        )
        if ok:
            yield _error_event(error)
    except BaseException as exc:
        # Client gone (GeneratorExit), executor cancelled, or a bug/infrastructure failure.
        if not attempt.settled:
            cancelled = isinstance(exc, (GeneratorExit, asyncio.CancelledError))
            # While draining, any cancellation (the executor's, or the server closing the stream at
            # its own timeout) is the deploy, not a client decision: hand the run over.
            drain = cancelled and container.active_runs.draining
            if cancelled:
                status, error = RunStatus.CANCELLED, None
            else:
                logger.error("run %s: unexpected failure", run_id, exc_info=exc)
                status = RunStatus.ERROR
                error = RunError(ErrorCode.INTERNAL_ERROR, type(exc).__name__, retryable=False)
            tasks = (beat,) if attempt.pump is None else (attempt.pump, beat)
            abort = _abort(
                container, attempt, runs, run, ctx, token, status, usage, error, tasks, drain
            )
            await _shielded(container.active_runs, abort)
        raise
    finally:
        beat.cancel()
        if attempt.pump is not None:
            attempt.pump.cancel()
        container.active_runs.end(attempt)
        container.observability.metrics.counter(
            METRIC_ATTEMPT_SECONDS, loop.time() - claimed_at, {"flow": ctx.flow}
        )
        span_ctx.run(span.__exit__, *sys.exc_info())  # an unexpected error marks the span


def _count_attempt(
    container: RunnerResources, attempt: _Attempt, ctx: RunContext, status: str
) -> None:
    """Count the attempt in `run_attempts` once, whichever path ends it."""
    if not attempt.counted:
        attempt.counted = True
        container.observability.metrics.counter(
            METRIC_ATTEMPTS, 1, {"flow": ctx.flow, "status": status}
        )


# Metadata namespaces the platform and LangChain/LangGraph read: a client key there could spoof the
# usage attribution (`graph_agent_agent`) or the stream mapping (`langgraph_*`).
_RESERVED_METADATA_PREFIXES: Final = ("graph_agent_", "ls_", "langgraph_")


def run_config(ctx: RunContext, flow: CompiledFlow, metadata: dict[str, Any]) -> RunnableConfig:
    """The `astream` config of one run; platform keys win so a client cannot spoof `run_id`."""
    client = {k: v for k, v in metadata.items() if not k.startswith(_RESERVED_METADATA_PREFIXES)}
    config: RunnableConfig = {
        "configurable": {"thread_id": ctx.thread_id},
        "metadata": {**client, "run_id": ctx.run_id, "thread_id": ctx.thread_id},
    }
    if flow.max_concurrency is not None:
        config["max_concurrency"] = flow.max_concurrency
    return config


async def pending_interrupts(checkpointer: Checkpointer, thread_id: uuid.UUID) -> tuple[str, ...]:
    """Ids of the interrupts the thread's latest checkpoint is still waiting on."""
    return _pending_ids(
        await checkpointer.aget_tuple({"configurable": {"thread_id": str(thread_id)}})
    )


def _pending_ids(latest: CheckpointTuple | None) -> tuple[str, ...]:
    if latest is None:
        return ()
    ids: list[str] = []
    for _task, channel, value in latest.pending_writes or ():
        if channel == INTERRUPT_KEY:
            ids.extend(i.id for i in (value if isinstance(value, (list, tuple)) else (value,)))
    return tuple(ids)


# LangGraph's own rule (pregel/_loop.py): a resume dict is an id map iff every key looks like this.
INTERRUPT_ID: Final = re.compile(r"[0-9a-f]{32}")


def is_resume_map(resume: object) -> TypeGuard[Mapping[str, object]]:
    """Whether LangGraph reads `resume` as `{interrupt_id: value}` (`{}` included)."""
    return isinstance(resume, Mapping) and all(
        isinstance(k, str) and INTERRUPT_ID.fullmatch(k) for k in resume
    )


def check_thread_command(pending: Sequence[str], *, input: object, resume: object) -> None:
    """Reject a command LangGraph would drop or fail on obscurely (design section 5).

    A map (`is_resume_map`) must be non-empty with only pending ids; anything else is a bare
    value and needs exactly one pending interrupt. `None` means absent.
    """
    if input is not None:
        if pending:
            raise RunError(
                ErrorCode.THREAD_INTERRUPTED,
                f"thread has {len(pending)} pending interrupt(s); resume instead of sending input",
            )
        return
    if resume is None:
        return
    if isinstance(resume, Mapping) and not all(isinstance(k, str) for k in resume):
        raise RunError(ErrorCode.INVALID_REQUEST, "resume map keys must be strings")
    if is_resume_map(resume):
        if not resume or not set(resume) <= set(pending):
            raise RunError(
                ErrorCode.INVALID_REQUEST,
                f"resume map must be non-empty with only pending interrupt ids: {list(pending)}",
            )
    elif len(pending) != 1:
        raise RunError(
            ErrorCode.INVALID_REQUEST,
            f"a bare resume value needs exactly one pending interrupt, the thread has "
            f"{len(pending)}: send a map {{interrupt_id: value}}",
        )


def check_tenant(pin: ThreadPin, tenant_id: str | None) -> None:
    """A tenant sees only its own threads (a foreign one looks absent); `None` sees all."""
    if tenant_id is not None and pin.tenant_id != tenant_id:
        raise RunError(ErrorCode.NOT_FOUND, "thread not found")


async def prepare_thread_run(
    container: RunnerResources,
    runs: RunsRepository,
    *,
    flow: str,
    thread_id: uuid.UUID,
    tenant_id: str | None,
    input: object,
    resume: object,
) -> ThreadPin:
    """Request-time checks of a run on a thread; returns what the new row pins.

    A fast fail only: `execute_run` re-checks the thread command when the run starts. Used by the
    API and the CLI so both reject the same way (raises `RunError`).
    """
    compiled = await container.flows.get(flow)  # flow_not_found / flow_build_failed
    pin = await runs.thread_pin(thread_id)
    if pin is None:  # first run of the thread: it fixes flow, version and tenant
        pending: tuple[str, ...] = ()
        pin = ThreadPin(flow, compiled.version, tenant_id)
    else:
        check_tenant(pin, tenant_id)
        if pin.flow != flow:
            raise RunError(ErrorCode.INVALID_REQUEST, f"thread belongs to flow {pin.flow!r}")
        check_version(compiled, pin.flow_version)  # flow_version_mismatch
        pending = await pending_interrupts(container.checkpointer, thread_id)
    check_thread_command(pending, input=input, resume=resume)
    return pin


async def cancel_pending_run(container: RunnerResources, runs: RunsRepository, run: Run) -> None:
    """Best-effort `request_cancel` of a row whose request failed; survives cancellation (1q, 1z).

    A failure is logged, never raised: it must not replace the error that got us here.
    """

    async def cancel() -> None:
        try:
            await runs.request_cancel(run.run_id, run.thread_id)
        except Exception:
            # shortcut: if this also fails the row stays pending and the orphan sweep re-defers
            # it, so the run may still execute; upgrade if that surprises clients.
            logger.exception("could not cancel run %s after a failed request", run.run_id)

    await asyncio.shield(container.active_runs.spawn(cancel()))


async def _initial_input(checkpointer: Checkpointer, run: Run) -> Any:
    """`None` if the latest checkpoint already carries this run_id (recovery or drain re-claim).

    Otherwise the thread-command rule runs again (`check_thread_command`, raises `RunError`): the
    request-time check cannot see a sibling run queued earlier that ended up interrupted.
    """
    latest = await checkpointer.aget_tuple({"configurable": {"thread_id": str(run.thread_id)}})
    if latest is not None and latest.metadata.get("run_id") == str(run.run_id):
        return None
    resume = None
    if run.command is not None:
        # Stored as {"resume": ...} (api.dto.ResumeCommand).
        if not isinstance(run.command, dict) or "resume" not in run.command:
            raise RuntimeError(f"run {run.run_id}: stored command has no 'resume'")
        resume = run.command["resume"]
    check_thread_command(_pending_ids(latest), input=run.input, resume=resume)
    return run.input if run.command is None else Command(resume=resume)


async def _pump(
    stream: AsyncGenerator[RunEvent],
    queue: "asyncio.Queue[RunEvent]",
    timeout_s: float,
    attempt: _Attempt,
) -> None:
    """Drain the stream into the bounded queue under the run timeout.

    A full queue blocks the stream (backpressure); that wait counts against the timeout. The
    stream is closed here on every exit, so a cancel while blocked on the queue still stops the
    graph's node tasks before the pump ends.
    """
    timeout = asyncio.timeout(timeout_s)
    try:
        async with timeout, aclosing(stream):
            async for event in stream:
                await queue.put(event)
    finally:
        attempt.timed_out = timeout.expired()


async def _heartbeat(
    runs: RunsRepository,
    run: Run,
    token: str,
    container: RunnerResources,
    attempt: _Attempt,
    claimed_at: float,
) -> None:
    """Extend the lease every `heartbeat_s`; stop the stream on a cancel request or a lost lease.

    The lease counts as lost when a heartbeat answers so, or when none succeeded for `lease_s`
    (the database is unreachable: the lease has expired and another executor may own the run).
    """
    cfg = container.settings.runs
    loop = asyncio.get_running_loop()
    last_ok = claimed_at
    while True:
        remaining = cfg.lease_s - (loop.time() - last_ok)
        delay = min(cfg.heartbeat_s, remaining)
        await asyncio.sleep(max(delay, 0.0))
        if delay >= remaining:  # nothing extended the lease for `lease_s`: it has expired
            logger.warning("run %s: no heartbeat within the lease", run.run_id)
            attempt.stop(_StopReason.LOST)
            return
        started = loop.time()  # the lease is extended from the call, not from its answer
        try:
            async with asyncio.timeout_at(last_ok + cfg.lease_s):  # a hung call must not outlive it
                cancel_requested = await runs.heartbeat(run.run_id, token, cfg.lease_s)
        except Exception as exc:
            if isinstance(exc, TimeoutError):
                logger.warning("run %s: heartbeat hung past the lease", run.run_id, exc_info=True)
                attempt.stop(_StopReason.LOST)
                return
            logger.warning("run %s: heartbeat failed", run.run_id, exc_info=True)
            continue
        last_ok = started
        if cancel_requested is None:
            attempt.stop(_StopReason.LOST)
            return
        if cancel_requested:
            attempt.stop(_StopReason.CANCEL)
            return


async def _stop_tasks(*tasks: "asyncio.Task[None]") -> None:
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def _shielded[T](active: ActiveRuns, coro: Coroutine[Any, Any, T]) -> T:
    """Run `coro` as its own tracked task and wait for it, surviving cancellation of the caller.

    anyio/Starlette cancel every `await` of a cancelled scope again, so the cleanup cannot run in
    the cancelled task itself. A second cancel propagates; the cleanup keeps running.
    """
    return await asyncio.shield(active.spawn(coro))


async def _settle[T](
    container: RunnerResources, attempt: _Attempt, coro: Coroutine[Any, Any, T]
) -> T:
    """The attempt's terminal step (write + observers) in a shielded task.

    `settled` is set first: a cancel from here on must not make the abort notify a second time.

    shortcut: this step is not bounded by the shutdown budget (only `_abort` is), so a hung
    database write can outlast it; `settle_runs` stops waiting at the deadline and lease expiry
    recovers the run.
    """
    attempt.settled = True
    return await _shielded(container.active_runs, coro)


async def _abort(
    container: RunnerResources,
    attempt: _Attempt,
    runs: RunsRepository,
    run: Run,
    ctx: RunContext,
    token: str,
    status: RunStatus,
    usage: UsageCallback,
    error: RunError | None,
    tasks: "tuple[asyncio.Task[None], ...]",
    drain: bool,
) -> None:
    """Stop the attempt's tasks, then record `cancelled`/`error` (or hand the run over)."""
    try:
        async with asyncio.timeout_at(_until(container.shutdown_budget)):
            await _stop_tasks(*tasks)
            if drain:
                await _hand_over(container, attempt, runs, run, ctx, token, usage)
            else:
                await _finish(container, attempt, runs, ctx, token, status, usage, error)
    except TimeoutError:
        logger.error(
            "run %s: cleanup exceeded the shutdown budget; lease expiry recovers", ctx.run_id
        )
    except Exception:
        logger.exception("run %s: could not record %s", ctx.run_id, status)
    finally:  # budget overrun or cancel: nothing was notified, the lease recovers the run
        _count_attempt(container, attempt, ctx, STATUS_RELEASED)


async def _finish(
    container: RunnerResources,
    attempt: _Attempt,
    runs: RunsRepository,
    ctx: RunContext,
    token: str,
    status: RunStatus,
    usage: UsageCallback,
    error: RunError | None = None,
) -> bool:
    """Fenced terminal write, then the observers. False: the lease was lost, nothing written."""
    try:
        written = await runs.finish(uuid.UUID(ctx.run_id), token, status, error)
    except Exception:  # the ledger still records what the attempt spent, once
        await _notify(container, attempt, ctx, RunStatus.RUNNING, usage)
        raise
    if not written:
        logger.warning("run %s: lease lost before the terminal write", ctx.run_id)
        await _notify(container, attempt, ctx, RunStatus.RUNNING, usage)
        return False
    await _notify(container, attempt, ctx, status, usage, error)
    return True


async def _notify(
    container: RunnerResources,
    attempt: _Attempt,
    ctx: RunContext,
    status: RunStatus,
    usage: UsageCallback,
    error: RunError | None = None,
) -> None:
    """Tell the observers how this attempt ended (`running`: it ended without a terminal write)."""
    _count_attempt(
        container, attempt, ctx, STATUS_RELEASED if status is RunStatus.RUNNING else status.value
    )
    by_key = usage.snapshot()  # one snapshot: the totals and the breakdown always agree
    outcome = RunOutcome(
        status.value,
        _totals(sum_counts(by_key.values())),
        None if error is None else _error_event(error),
        by_key,
    )
    for observer in container.observers:
        try:
            await observer.on_finish(ctx, outcome)
        except Exception:  # an observer (ledger) must never change the run's outcome
            logger.exception("run %s: observer %r failed", ctx.run_id, observer)


async def _hand_over(
    container: RunnerResources,
    attempt: _Attempt,
    runs: RunsRepository,
    run: Run,
    ctx: RunContext,
    token: str,
    usage: UsageCallback,
) -> None:
    """Graceful shutdown: expire the lease, give the attempt back and defer; no terminal event.

    The observers are told the attempt ended without a terminal write (status `running`).
    """
    try:
        if not await runs.release_for_drain(run.run_id, token):
            logger.warning("run %s: lease lost before the drain release", run.run_id)
        elif container.queue is None:
            logger.warning(
                "run %s drained but no queue to defer it; the orphan sweep will", run.run_id
            )
        else:
            try:
                await container.queue.defer(run.run_id, run.thread_id, recovery=True)
            except Exception:  # the lease is already expired: the orphan sweep re-defers it
                logger.exception("run %s: could not defer after drain", run.run_id)
    finally:
        await _notify(container, attempt, ctx, RunStatus.RUNNING, usage)


def _error_event(error: RunError) -> ErrorEvent:
    return ErrorEvent(error.code.value, error.message, error.retryable)
