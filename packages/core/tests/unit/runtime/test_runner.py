import asyncio
import datetime as dt
import sys
import types
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any, TypedDict, cast

import pytest
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.config import get_stream_writer
from langgraph.graph import END, START, StateGraph
from langgraph.runtime import RunControl
from langgraph.types import Command, interrupt

from graph_agent._internal.config.manifest import FlowSpec
from graph_agent._internal.config.settings import RunsSettings, Settings
from graph_agent._internal.runtime import runner
from graph_agent._internal.runtime.checkpointer import Checkpointer
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
from graph_agent._internal.runtime.flows import CompiledFlow, FlowRegistry
from graph_agent._internal.runtime.runner import RunQueue, execute_run, stream_graph
from graph_agent._internal.runtime.runs import (
    Claim,
    ClaimOutcome,
    ErrorCode,
    Run,
    RunError,
    RunsRepository,
    RunStatus,
)

MODULE = "t16_user_flows"
FLOW_VERSION = 3
TOKEN = "w:tok"
HEARTBEAT_S = 0.02
LEASE_S = 1.0
USAGE = UsageTotals(10, 5, 4)
# Built at import: the TOML settings source stats files, which blockbuster rejects in the loop.
SETTINGS = Settings(_env_file=None, runs=RunsSettings(heartbeat_s=HEARTBEAT_S, lease_s=LEASE_S))


class State(TypedDict, total=False):
    x: int
    answer: str


class UsageModel(BaseChatModel):
    @property
    def _llm_type(self) -> str:
        return "usage-fake"

    def _generate(self, messages: list[BaseMessage], *args: Any, **kwargs: Any) -> ChatResult:
        msg = AIMessage(
            content="hi",
            usage_metadata={
                "input_tokens": 10,
                "output_tokens": 5,
                "total_tokens": 15,
                "input_token_details": {"cache_read": 4},
            },
        )
        return ChatResult(generations=[ChatGeneration(message=msg)])


async def _work(state: State) -> State:
    get_stream_writer()({"progress": 1})
    await UsageModel().ainvoke("yo")
    return {"x": state.get("x", 0) + 1}


async def _ask(state: State) -> State:
    return {"answer": interrupt("name?")}


async def _slow(state: State) -> State:
    await asyncio.sleep(30)
    return {}


async def _boom(state: State) -> State:
    raise ValueError("secret-token-123")


drain_controls: list[RunControl] = []


async def _drain_first(state: State) -> State:
    drain_controls[-1].request_drain("test")
    return {"x": 1}


async def _after(state: State) -> State:
    return {"x": 2}


def _builder(*nodes: Any) -> StateGraph[State, Any, State, State]:
    g = StateGraph(State)
    prev = START
    for i, fn in enumerate(nodes):
        g.add_node(f"n{i}", fn)
        g.add_edge(prev, f"n{i}")
        prev = f"n{i}"
    g.add_edge(prev, END)
    return g


@pytest.fixture
def user_module() -> Iterator[types.ModuleType]:
    mod = types.ModuleType(MODULE)
    mod.work = _builder(_work)  # type: ignore[attr-defined]
    mod.ask = _builder(_ask)  # type: ignore[attr-defined]
    mod.slow = _builder(_slow)  # type: ignore[attr-defined]
    mod.boom = _builder(_boom)  # type: ignore[attr-defined]
    mod.drain = _builder(_drain_first, _after)  # type: ignore[attr-defined]
    sys.modules[MODULE] = mod
    yield mod
    del sys.modules[MODULE]
    drain_controls.clear()


class FakeRuns:
    def __init__(self, claim: Claim, heartbeats: Sequence[bool | None] = ()) -> None:
        self.claim_result = claim
        self.heartbeats = list(heartbeats)
        self.finished: list[tuple[RunStatus, RunError | None]] = []
        self.released: list[str] = []
        self.finish_ok = True

    async def claim(
        self, run_id: uuid.UUID, owner: str, lease_s: float, max_attempts: int
    ) -> Claim:
        return self.claim_result

    async def heartbeat(self, run_id: uuid.UUID, token: str, lease_s: float) -> bool | None:
        assert token == TOKEN
        return self.heartbeats.pop(0) if self.heartbeats else False

    async def finish(
        self, run_id: uuid.UUID, token: str, status: RunStatus, error: RunError | None = None
    ) -> bool:
        assert token == TOKEN
        if self.finish_ok:
            self.finished.append((status, error))
        return self.finish_ok

    async def release_for_drain(self, run_id: uuid.UUID, token: str) -> bool:
        assert token == TOKEN
        self.released.append(token)
        return True


@dataclass
class FakeQueue:
    deferred: list[tuple[uuid.UUID, uuid.UUID]] = field(default_factory=list)

    async def defer(self, run_id: uuid.UUID, thread_id: uuid.UUID) -> None:
        self.deferred.append((run_id, thread_id))


@dataclass
class Observer:
    outcomes: list[RunOutcome] = field(default_factory=list)

    def callbacks(self, ctx: RunContext) -> Sequence[BaseCallbackHandler]:
        return ()

    async def on_finish(self, ctx: RunContext, outcome: RunOutcome) -> None:
        self.outcomes.append(outcome)


@dataclass
class Resources:
    settings: Settings
    runs: RunsRepository | None
    flows: FlowRegistry
    checkpointer: Checkpointer
    observers: list[Any]
    queue: RunQueue | None


def _run(flow: str, **overrides: Any) -> Run:
    base: dict[str, Any] = {
        "run_id": uuid.uuid4(),
        "thread_id": uuid.uuid4(),
        "flow": flow,
        "flow_version": FLOW_VERSION,
        "tenant_id": None,
        "status": RunStatus.RUNNING,
        "input": {"x": 0},
        "command": None,
        "error": None,
        "attempt": 1,
        "lease_owner": TOKEN,
        "lease_expires_at": None,
        "cancel_requested": False,
        "created_at": dt.datetime.now(dt.UTC),
        "started_at": None,
        "finished_at": None,
        "metadata": {},
    }
    return Run(**{**base, **overrides})


def _resources(
    run: Run,
    outcome: ClaimOutcome = ClaimOutcome.CLAIMED,
    *,
    heartbeats: Sequence[bool | None] = (),
    timeout_s: float = 5.0,
    saver: InMemorySaver | None = None,
) -> tuple[Resources, FakeRuns]:
    claim = Claim(outcome, run, TOKEN if outcome is ClaimOutcome.CLAIMED else None)
    fake = FakeRuns(claim, heartbeats)
    saver = saver or InMemorySaver()
    flows = {
        name: FlowSpec(target=f"{MODULE}:{name}", version=FLOW_VERSION, timeout_s=timeout_s)
        for name in ("work", "ask", "slow", "boom", "drain")
    }
    ctx_factory: Any = lambda name: object()  # noqa: E731
    registry = FlowRegistry(flows, saver, ctx_factory)
    res = Resources(SETTINGS, cast(RunsRepository, fake), registry, saver, [], FakeQueue())
    return res, fake


async def _events(res: Resources, run: Run, **kw: Any) -> list[RunEvent]:
    return [e async for e in execute_run(res, run.run_id, **kw)]


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
    assert obs.outcomes == [RunOutcome("success", USAGE)]
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


async def test_drain_releases_lease_and_defers(
    user_module: types.ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    def make_control() -> RunControl:
        drain_controls.append(RunControl())
        return drain_controls[-1]

    monkeypatch.setattr(runner, "RunControl", make_control)
    run = _run("drain")
    res, fake = _resources(run)
    events = await _events(res, run)
    assert not any(isinstance(e, (EndEvent, ErrorEvent)) for e in events)
    assert fake.finished == [] and fake.released == [TOKEN]
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
    [(ClaimOutcome.LOST, []), (ClaimOutcome.CANCELLED, [EndEvent("cancelled", UsageTotals())])],
)
async def test_claim_outcomes(outcome: ClaimOutcome, expect: list[RunEvent]) -> None:
    run = _run("work")
    res, fake = _resources(run, outcome)
    assert await _events(res, run) == expect
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
    assert isinstance(await runner._initial_input(saver, run), Command)  # no checkpoint yet
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
