import asyncio
import time
import types
from typing import Any, cast

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import StateGraph
from langgraph.types import Command

from graph_agent._internal.bootstrap import ShutdownBudget
from graph_agent._internal.runtime import runner
from graph_agent._internal.runtime.context import RunContext
from graph_agent._internal.runtime.events import (
    CustomEvent,
    EndEvent,
    ErrorEvent,
    InterruptEvent,
    MetadataEvent,
    RunEvent,
    RunOutcome,
    UpdatesEvent,
    UsageTotals,
    ValuesEvent,
)
from graph_agent._internal.runtime.flows import CompiledFlow
from graph_agent._internal.runtime.runner import (
    ActiveRuns,
    execute_run,
    run_config,
    settle_runs,
    start_drain,
    stream_graph,
)
from graph_agent._internal.runtime.runs import (
    ClaimOutcome,
    ErrorCode,
    RunError,
    RunStatus,
)
from graph_agent._internal.usage import Counts
from unit.runtime._runner_doubles import (
    CHAIN,
    FLOW_VERSION,
    LATE_HEARTBEAT_S,
    LATE_LEASE_S,
    LATE_SETTINGS,
    SHORT_LEASE_SETTINGS,
    TOKEN,
    USAGE,
    FakeQueue,
    GatedObserver,
    Observer,
    State,
    _ask,
    _builder,
    _events,
    _resources,
    _run,
    _work,
    cancel_seen,
    drain_targets,
    reached,
)

# --- stream_graph (no database, real graphs, blockbuster active) ---------------------------


def _compiled(builder: StateGraph[State, Any, State, State], saver: InMemorySaver) -> CompiledFlow:
    return CompiledFlow("f", builder.compile(checkpointer=saver), 1, "sync", 5.0, None)


def _ctx(thread: str = "t1") -> RunContext:
    return RunContext("r1", thread, "f", 1, 1, None)


def _cfg(thread: str = "t1") -> Any:
    return {"configurable": {"thread_id": thread}, "metadata": {"run_id": "r1"}}


async def test_stream_graph_events_usage_and_mode_filter() -> None:
    compiled = _compiled(_builder(_work), InMemorySaver())
    events = [
        e async for e in stream_graph(compiled, {"x": 0}, _cfg(), _ctx(), ["custom", "values"])
    ]
    kinds = [type(e) for e in events]
    assert kinds[0] is MetadataEvent and kinds[-1] is EndEvent
    assert UpdatesEvent not in kinds  # requested internally, filtered out for the client
    assert CustomEvent in kinds and ValuesEvent in kinds
    assert events[-1] == EndEvent("success", USAGE)


async def test_stream_graph_interrupt_then_resume() -> None:
    saver = InMemorySaver()
    compiled = _compiled(_builder(_ask), saver)
    first = [e async for e in stream_graph(compiled, {}, _cfg(), _ctx(), ["updates"])]
    interrupts = [e for e in first if isinstance(e, InterruptEvent)]
    assert len(interrupts) == 1 and interrupts[0].interrupts[0].value == "name?"
    assert first[-1] == EndEvent("interrupted", UsageTotals())
    resume: Command[Any] = Command(resume={interrupts[0].interrupts[0].id: "bob"})
    second = [e async for e in stream_graph(compiled, resume, _cfg(), _ctx(), ["updates"])]
    assert second[-1] == EndEvent("success", UsageTotals())


async def test_stream_graph_rejects_unknown_mode() -> None:
    compiled = _compiled(_builder(_work), InMemorySaver())
    with pytest.raises(ValueError, match="debug"):
        [e async for e in stream_graph(compiled, {}, _cfg(), _ctx(), ["debug"])]


# --- execute_run ----------------------------------------------------------------------------


async def test_success_writes_terminal_row_before_end_and_notifies_observers(
    user_module: types.ModuleType,
) -> None:
    run = _run("work", metadata={"run_id": "spoofed", "tag": "a"})
    res, fake = _resources(run)
    obs = Observer()
    res.observers.append(obs)
    finished_when_end_seen: list[int] = []
    events: list[RunEvent] = []
    async for ev in execute_run(res, run.run_id, stream_modes=["updates"]):
        if isinstance(ev, EndEvent):
            finished_when_end_seen.append(len(fake.finished))
        events.append(ev)
    assert isinstance(events[0], MetadataEvent) and events[0].run_id == str(run.run_id)
    assert events[-1] == EndEvent("success", USAGE)
    assert finished_when_end_seen == [1] and fake.finished == [(RunStatus.SUCCESS, None)]
    (outcome,) = obs.outcomes
    assert (outcome.status, outcome.usage, outcome.error) == ("success", USAGE, None)
    assert list(outcome.usage_by_key.values()) == [Counts(10, 5, 4, 0, 1)]
    # Platform metadata wins over the request's, and lands in the checkpoint.
    latest = await res.checkpointer.aget_tuple({"configurable": {"thread_id": str(run.thread_id)}})
    assert latest is not None
    assert latest.metadata["run_id"] == str(run.run_id) and dict(latest.metadata)["tag"] == "a"


async def test_interrupt_ends_interrupted(user_module: types.ModuleType) -> None:
    run = _run("ask")
    res, fake = _resources(run)
    events = await _events(res, run)
    assert any(isinstance(e, InterruptEvent) for e in events)
    assert events[-1] == EndEvent("interrupted", UsageTotals())
    assert fake.finished == [(RunStatus.INTERRUPTED, None)]


async def test_resume_command_continues_the_thread(user_module: types.ModuleType) -> None:
    saver = InMemorySaver()
    first = _run("ask")
    res, _ = _resources(first, saver=saver)
    (iv,) = [e for e in await _events(res, first) if isinstance(e, InterruptEvent)]
    second = _run(
        "ask",
        thread_id=first.thread_id,
        input=None,
        command={"resume": {iv.interrupts[0].id: "bob"}},
    )
    res2, fake2 = _resources(second, saver=saver)
    events2 = await _events(res2, second, stream_modes=["values"])
    assert events2[-1] == EndEvent("success", UsageTotals())
    assert fake2.finished == [(RunStatus.SUCCESS, None)]


async def test_run_timeout(user_module: types.ModuleType) -> None:
    run = _run("slow")
    res, fake = _resources(run, timeout_s=0.05)
    events = await _events(res, run)
    assert events[-1] == ErrorEvent("run_timeout", "run exceeded 0.05s", False)
    status, error = fake.finished[0]
    assert status is RunStatus.ERROR and error is not None and error.code is ErrorCode.RUN_TIMEOUT


async def test_node_error_is_internal_error_without_leaking_the_message(
    user_module: types.ModuleType, caplog: pytest.LogCaptureFixture
) -> None:
    run = _run("boom")
    res, fake = _resources(run)
    events = await _events(res, run)
    assert events[-1] == ErrorEvent("internal_error", "ValueError", False)
    assert fake.finished[0][0] is RunStatus.ERROR


async def test_heartbeat_cancel_request_cancels_the_stream(user_module: types.ModuleType) -> None:
    run = _run("slow")
    res, fake = _resources(run, heartbeats=[False, True])
    events = await _events(res, run)
    assert events[-1] == EndEvent("cancelled", UsageTotals())
    assert fake.finished == [(RunStatus.CANCELLED, None)]


async def test_lost_lease_stops_without_terminal_write(user_module: types.ModuleType) -> None:
    run = _run("slow")
    res, fake = _resources(run, heartbeats=[None])
    events = await _events(res, run)
    assert isinstance(events[-1], ErrorEvent) and events[-1].retryable
    assert fake.finished == []


async def test_lost_lease_at_finish_yields_no_end(user_module: types.ModuleType) -> None:
    run = _run("work")
    res, fake = _resources(run)
    fake.finish_ok = False
    events = await _events(res, run)
    assert not any(isinstance(e, EndEvent) for e in events)


async def test_client_disconnect_cancels_the_run(user_module: types.ModuleType) -> None:
    run = _run("slow")
    res, fake = _resources(run)
    gen = execute_run(res, run.run_id)
    await anext(gen)  # metadata
    await gen.aclose()
    assert fake.finished == [(RunStatus.CANCELLED, None)]


async def test_drain_releases_lease_defers_and_notifies(user_module: types.ModuleType) -> None:
    run = _run("drain")
    res, fake = _resources(run)
    drain_targets.append(res.active_runs)
    obs = Observer()
    res.observers.append(obs)
    events = await _events(res, run)
    assert not any(isinstance(e, (EndEvent, ErrorEvent)) for e in events)
    assert fake.finished == [] and fake.released == [TOKEN]
    assert [o.status for o in obs.outcomes] == ["running"]
    assert len(res.active_runs) == 0 and res.active_runs.draining
    assert cast(FakeQueue, res.queue).deferred == [(run.run_id, run.thread_id)]
    # The re-claim resumes with None: the checkpoint already carries this run_id.
    assert await runner._initial_input(res.checkpointer, run) is None


async def test_flow_version_mismatch_is_an_error_row(user_module: types.ModuleType) -> None:
    run = _run("work", flow_version=FLOW_VERSION + 1)
    res, fake = _resources(run)
    events = await _events(res, run)
    assert isinstance(events[-1], ErrorEvent) and events[-1].code == "flow_version_mismatch"
    assert fake.finished[0][0] is RunStatus.ERROR


@pytest.mark.parametrize(
    ("outcome", "expect"),
    [(ClaimOutcome.LOST, []), (ClaimOutcome.CANCELLED, [MetadataEvent, EndEvent])],
)
async def test_claim_outcomes(outcome: ClaimOutcome, expect: list[type]) -> None:
    run = _run("work")
    res, fake = _resources(run, outcome)
    events = await _events(res, run)
    assert [type(e) for e in events] == expect
    if expect:
        assert events[-1] == EndEvent("cancelled", UsageTotals())
    assert fake.finished == []


async def test_thread_busy_raises_typed_error() -> None:
    run = _run("work")
    res, _ = _resources(run, ClaimOutcome.THREAD_BUSY)
    with pytest.raises(RunError) as ei:
        await _events(res, run)
    assert ei.value.code is ErrorCode.THREAD_BUSY and ei.value.retryable


async def test_memory_mode_raises_loudly() -> None:
    run = _run("work")
    res, _ = _resources(run)
    res.runs = None
    with pytest.raises(RuntimeError, match="stream_graph"):
        await _events(res, run)


async def test_initial_input_rule() -> None:
    saver = InMemorySaver()
    run = _run("ask", command={"resume": "x"}, input=None)
    with pytest.raises(RunError) as ei:  # nothing to resume: the thread rule applies at execution
        await runner._initial_input(saver, run)
    assert ei.value.code is ErrorCode.INVALID_REQUEST
    assert await runner._initial_input(saver, _run("ask")) == {"x": 0}
    compiled = _compiled(_builder(_work), saver)
    cfg: Any = {
        "configurable": {"thread_id": str(run.thread_id)},
        "metadata": {"run_id": str(run.run_id)},
    }
    [e async for e in stream_graph(compiled, {"x": 0}, cfg, _ctx(str(run.thread_id)), [])]
    assert await runner._initial_input(saver, run) is None  # same run_id: recovery
    other = _run("ask", thread_id=run.thread_id)
    assert await runner._initial_input(saver, other) == {"x": 0}  # new run on the thread
    with pytest.raises(RuntimeError, match="resume"):
        await runner._initial_input(saver, _run("ask", command={"oops": 1}, input=None))


async def test_initial_input_recovery_skips_the_interrupt_rule() -> None:
    saver = InMemorySaver()
    run = _run("ask")  # input run whose own first attempt already left an interrupt
    compiled = _compiled(_builder(_ask), saver)
    cfg: Any = {
        "configurable": {"thread_id": str(run.thread_id)},
        "metadata": {"run_id": str(run.run_id)},
    }
    [e async for e in stream_graph(compiled, {"x": 0}, cfg, _ctx(str(run.thread_id)), [])]
    assert await runner.pending_interrupts(saver, run.thread_id)
    assert await runner._initial_input(saver, run) is None  # recovery: not rejected
    with pytest.raises(RunError) as ei:  # a different run's input still is
        await runner._initial_input(saver, _run("ask", thread_id=run.thread_id))
    assert ei.value.code is ErrorCode.THREAD_INTERRUPTED


def test_run_config_drops_reserved_client_metadata() -> None:
    client = {
        "graph_agent_agent": "spoofed",
        "ls_provider": "x",
        "langgraph_node": "x",
        "team": "billing",
        "run_id": "spoofed",
    }
    flow = _compiled(_builder(_work), InMemorySaver())
    assert run_config(_ctx(), flow, client)["metadata"] == {
        "team": "billing",
        "run_id": "r1",
        "thread_id": "t1",
    }


# --- cancellation-safe cleanup, lease loss, backpressure, usage ------------------------------


async def test_task_cancelled_twice_still_records_cancelled(user_module: types.ModuleType) -> None:
    run = _run("slow")
    res, fake = _resources(run)
    seen: list[RunEvent] = []

    async def consume() -> None:
        async for ev in execute_run(res, run.run_id):
            seen.append(ev)

    task = asyncio.create_task(consume())
    while not seen:  # noqa: ASYNC110 (waits for the metadata event)
        await asyncio.sleep(0.005)
    task.cancel()  # anyio/Starlette re-cancel every await of a cancelled scope
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await res.active_runs.settled()
    assert fake.finished == [(RunStatus.CANCELLED, None)]
    assert len(res.active_runs) == 0


async def test_cancellation_while_the_flow_loads_cancels_the_run(
    user_module: types.ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = _run("work")
    res, fake = _resources(run)
    entered = asyncio.Event()

    async def hang(name: str) -> CompiledFlow:
        entered.set()
        await asyncio.sleep(30)
        raise AssertionError("unreachable")

    monkeypatch.setattr(res.flows, "get", hang)
    task = asyncio.create_task(_events(res, run))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await res.active_runs.settled()
    assert fake.finished == [(RunStatus.CANCELLED, None)]


async def test_unexpected_failure_records_an_error_row(
    user_module: types.ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = _run("work")
    res, fake = _resources(run)

    async def broken(*args: Any) -> Any:
        raise OSError("checkpointer down")

    monkeypatch.setattr(runner, "_initial_input", broken)
    with pytest.raises(OSError, match="checkpointer down"):
        await _events(res, run)
    await res.active_runs.settled()
    ((status, error),) = fake.finished
    assert status is RunStatus.ERROR and error is not None and error.message == "OSError"


async def test_closing_after_the_end_event_writes_nothing_more(
    user_module: types.ModuleType,
) -> None:
    run = _run("work")
    res, fake = _resources(run)
    obs = Observer()
    res.observers.append(obs)
    gen = execute_run(res, run.run_id)
    async for ev in gen:
        if isinstance(ev, EndEvent):
            break
    await gen.aclose()
    await res.active_runs.settled()
    assert fake.finished == [(RunStatus.SUCCESS, None)]
    assert len(obs.outcomes) == 1  # the ledger sees the attempt once


@pytest.mark.parametrize("fault", ["raise", "hang"])
async def test_heartbeat_silence_for_a_lease_means_lost(
    user_module: types.ModuleType, fault: str
) -> None:
    run = _run("slow")
    res, fake = _resources(run, settings=SHORT_LEASE_SETTINGS)
    fake.heartbeat_fault = fault
    obs = Observer()
    res.observers.append(obs)
    events = await _events(res, run)
    assert isinstance(events[-1], ErrorEvent) and events[-1].retryable
    assert fake.finished == []
    assert [o.status for o in obs.outcomes] == ["running"]


async def test_lost_lease_at_finish_still_notifies_the_ledger(
    user_module: types.ModuleType,
) -> None:
    run = _run("work")
    res, fake = _resources(run)
    fake.finish_ok = False
    obs = Observer()
    res.observers.append(obs)
    await _events(res, run)
    (outcome,) = obs.outcomes
    assert outcome.status == "running" and outcome.usage == USAGE


async def test_failed_terminal_write_still_notifies_the_ledger(
    user_module: types.ModuleType,
) -> None:
    run = _run("work")
    res, fake = _resources(run)
    fake.finish_fault = True
    obs = Observer()
    res.observers.append(obs)
    with pytest.raises(ConnectionError):
        await _events(res, run)
    (outcome,) = obs.outcomes
    assert outcome.status == "running" and outcome.usage == USAGE


async def test_slow_consumer_backpressures_the_stream(
    user_module: types.ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(runner, "RUN_EVENT_BUFFER", 1)
    run = _run("chain")
    res, _ = _resources(run)
    gen = execute_run(res, run.run_id)
    await anext(gen)  # metadata, then stop consuming
    await asyncio.sleep(0.2)
    assert len(reached) < CHAIN
    rest = [e async for e in gen]
    assert len(reached) == CHAIN
    assert isinstance(rest[-1], EndEvent)


async def test_unknown_stream_mode_is_rejected_before_the_claim() -> None:
    run = _run("work")
    res, fake = _resources(run)
    with pytest.raises(ValueError, match="debug"):
        await _events(res, run, stream_modes=["debug"])
    assert fake.claims == 0


async def test_cancel_while_the_pump_is_blocked_stops_the_nodes(
    user_module: types.ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(runner, "RUN_EVENT_BUFFER", 1)
    run = _run("fanout")
    res, fake = _resources(run)
    seen_at_finish: list[list[str]] = []

    class Spy(Observer):
        async def on_finish(self, ctx: RunContext, outcome: RunOutcome) -> None:
            seen_at_finish.append(list(cancel_seen))

    res.observers.append(Spy())
    gen = execute_run(res, run.run_id)
    await anext(gen)  # metadata, then a slow consumer: the pump blocks on the full queue
    await asyncio.sleep(0.2)
    await gen.aclose()
    await res.active_runs.settled()
    assert fake.finished == [(RunStatus.CANCELLED, None)]
    assert seen_at_finish == [["b"]]  # the sibling node was cancelled before `finished`


async def test_cancel_inside_an_observer_notifies_once(user_module: types.ModuleType) -> None:
    run = _run("work")
    res, fake = _resources(run)
    obs = GatedObserver()
    res.observers.append(obs)
    seen: list[RunEvent] = []

    async def consume() -> None:
        async for ev in execute_run(res, run.run_id):
            seen.append(ev)

    task = asyncio.create_task(consume())
    while not obs.outcomes:  # noqa: ASYNC110 (waits for the terminal step)
        await asyncio.sleep(0.005)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    obs.gate.set()
    await res.active_runs.settled()
    assert fake.finished == [(RunStatus.SUCCESS, None)] and len(obs.outcomes) == 1


async def test_cancelled_while_draining_hands_the_run_over(user_module: types.ModuleType) -> None:
    run = _run("slow")
    res, fake = _resources(run)
    seen: list[RunEvent] = []

    async def consume() -> None:
        async for ev in execute_run(res, run.run_id):
            seen.append(ev)

    task = asyncio.create_task(consume())
    while not seen:  # noqa: ASYNC110 (waits for the metadata event)
        await asyncio.sleep(0.005)
    res.active_runs.request_drain()  # SIGTERM: the executor is cancelled right after
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await res.active_runs.settled()
    assert fake.finished == [] and fake.released == [TOKEN]
    assert cast(FakeQueue, res.queue).deferred == [(run.run_id, run.thread_id)]


async def test_closed_at_a_yield_while_draining_hands_the_run_over(
    user_module: types.ModuleType,
) -> None:
    run = _run("slow")
    res, fake = _resources(run)
    gen = execute_run(res, run.run_id)
    await anext(gen)  # metadata: the generator is suspended at its yield
    res.active_runs.request_drain()  # the server then closes the stream (GeneratorExit)
    await gen.aclose()
    await res.active_runs.settled()
    assert fake.finished == [] and fake.released == [TOKEN]
    assert cast(FakeQueue, res.queue).deferred == [(run.run_id, run.thread_id)]


async def test_pump_cancelled_without_a_reason_is_an_internal_error(
    user_module: types.ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def cancelled_pump(*args: Any) -> None:
        raise asyncio.CancelledError  # nobody asked: not the heartbeat, not the consumer

    monkeypatch.setattr(runner, "_pump", cancelled_pump)
    run = _run("work")
    res, fake = _resources(run)
    obs = Observer()
    res.observers.append(obs)
    events = await _events(res, run)
    assert events[-1] == ErrorEvent("internal_error", "CancelledError", False)
    ((status, error),) = fake.finished
    assert status is RunStatus.ERROR and error is not None
    assert [o.status for o in obs.outcomes] == ["error"]


async def test_lost_lease_is_detected_at_lease_expiry(user_module: types.ModuleType) -> None:
    run = _run("slow")
    res, fake = _resources(run, settings=LATE_SETTINGS)
    fake.heartbeat_fault = "raise"
    started = time.monotonic()
    events = await _events(res, run)
    elapsed = time.monotonic() - started
    assert isinstance(events[-1], ErrorEvent) and events[-1].retryable
    # Nominal beats at 0.12 and 0.24; a loaded CI box may skip one. Lost at lease expiry, never
    # after an extra full interval.
    assert 1 <= fake.beats <= 2
    assert elapsed < LATE_LEASE_S + LATE_HEARTBEAT_S


async def test_settled_waits_for_the_tracked_run() -> None:
    active = ActiveRuns()
    await active.settled()  # nothing in flight
    gate = asyncio.Event()
    task = active.spawn(gate.wait())
    waiter = asyncio.create_task(active.settled())
    await asyncio.sleep(0.01)
    assert not waiter.done()
    gate.set()
    await waiter
    assert task.done()


def test_active_runs_tracks_and_drains() -> None:
    active = ActiveRuns()
    with active.track() as control:
        assert len(active) == 1 and not active.draining
        active.request_drain()
        assert active.draining
        assert control is not None
    assert len(active) == 0
    with active.track():  # claimed during shutdown: starts already asked to drain
        assert active.draining


async def test_settle_runs_stops_waiting_at_the_shutdown_deadline() -> None:
    active = ActiveRuns()
    budget = ShutdownBudget(0.05)
    gate = asyncio.Event()
    stuck = active.spawn(gate.wait())  # a cleanup that never ends (hung write)
    start_drain(active, budget)
    assert active.draining and budget.deadline != float("inf")
    started = time.monotonic()
    assert await settle_runs(active, budget) is False
    assert time.monotonic() - started < 1
    gate.set()
    await stuck
    assert await settle_runs(active, budget) is True


async def test_settle_runs_without_a_started_clock_waits_for_the_runs() -> None:
    active = ActiveRuns()
    gate = asyncio.Event()
    active.spawn(gate.wait())
    waiter = asyncio.create_task(settle_runs(active, ShutdownBudget(0.01)))  # clock not started
    await asyncio.sleep(0.05)
    assert not waiter.done()
    gate.set()
    assert await waiter is True
