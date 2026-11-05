"""Doubles shared by the runner unit tests (not a test module)."""

import asyncio
import datetime as dt
import math
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, TypedDict, cast

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.config import get_stream_writer
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from graph_agent._internal.config.manifest import FlowSpec
from graph_agent._internal.config.settings import RunsSettings, Settings
from graph_agent._internal.observability.tracing import LoggingMetrics, Observability
from graph_agent._internal.runtime.checkpointer import Checkpointer
from graph_agent._internal.runtime.context import RunContext
from graph_agent._internal.runtime.events import (
    RunEvent,
    RunOutcome,
    UsageTotals,
)
from graph_agent._internal.runtime.flows import FlowRegistry
from graph_agent._internal.runtime.runner import (
    ActiveRuns,
    RunQueue,
    execute_run,
)
from graph_agent._internal.runtime.runs import (
    Claim,
    ClaimOutcome,
    Run,
    RunError,
    RunsRepository,
    RunStatus,
)

MODULE = "t16_user_flows"
FLOW_VERSION = 3
TOKEN = "w:tok"
CHAIN = 8
FLOOD = 20
HEARTBEAT_S = 0.02
LEASE_S = 1.0
USAGE = UsageTotals(10, 5, 4)
# Built at import: the TOML settings source stats files, which blockbuster rejects in the loop.
SETTINGS = Settings(_env_file=None, runs=RunsSettings(heartbeat_s=HEARTBEAT_S, lease_s=LEASE_S))
LATE_LEASE_S = 0.3
LATE_HEARTBEAT_S = 0.12
LATE_SETTINGS = Settings(
    _env_file=None, runs=RunsSettings(heartbeat_s=LATE_HEARTBEAT_S, lease_s=LATE_LEASE_S)
)
SHORT_LEASE_S = 0.1
SHORT_LEASE_SETTINGS = Settings(
    _env_file=None, runs=RunsSettings(heartbeat_s=0.01, lease_s=SHORT_LEASE_S)
)


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


cancel_seen: list[str] = []


async def _guarded_sleep(state: State) -> State:
    try:
        await asyncio.sleep(30)
    except asyncio.CancelledError:
        cancel_seen.append("b")
        raise
    return {}


async def _chatty(state: State) -> State:
    writer = get_stream_writer()
    for i in range(FLOOD):
        writer({"i": i})
    return {}


def _fanout() -> StateGraph[State, Any, State, State]:
    g = StateGraph(State)
    g.add_node("a", _chatty)
    g.add_node("b", _guarded_sleep)
    g.add_edge(START, "a")
    g.add_edge(START, "b")
    g.add_edge("a", END)
    g.add_edge("b", END)
    return g


drain_targets: list[ActiveRuns] = []
reached: list[int] = []


async def _drain_first(state: State) -> State:
    drain_targets[-1].request_drain()
    return {"x": 1}


def _step(i: int) -> Any:
    async def step(state: State) -> State:
        reached.append(i)
        return {"x": i}

    return step


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


class FakeRuns:
    def __init__(self, claim: Claim, heartbeats: Sequence[bool | None] = ()) -> None:
        self.claim_result = claim
        self.claims = 0
        self.heartbeats = list(heartbeats)
        self.beats = 0
        self.heartbeat_fault: str | None = None  # "raise" or "hang"
        self.finished: list[tuple[RunStatus, RunError | None]] = []
        self.released: list[str] = []
        self.finish_ok = True
        self.finish_fault = False

    async def claim(
        self, run_id: uuid.UUID, owner: str, lease_s: float, max_attempts: int
    ) -> Claim:
        self.claims += 1
        return self.claim_result

    async def heartbeat(self, run_id: uuid.UUID, token: str, lease_s: float) -> bool | None:
        assert token == TOKEN
        self.beats += 1
        if self.heartbeat_fault == "raise":
            raise ConnectionError("database unreachable")
        if self.heartbeat_fault == "hang":
            await asyncio.sleep(30)
        return self.heartbeats.pop(0) if self.heartbeats else False

    async def finish(
        self, run_id: uuid.UUID, token: str, status: RunStatus, error: RunError | None = None
    ) -> bool:
        assert token == TOKEN
        if self.finish_fault:
            raise ConnectionError("database unreachable")
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
class GatedObserver:
    """Records the outcome, then blocks until released (a slow ledger write)."""

    gate: asyncio.Event = field(default_factory=asyncio.Event)
    outcomes: list[RunOutcome] = field(default_factory=list)

    def callbacks(self, ctx: RunContext) -> Sequence[BaseCallbackHandler]:
        return ()

    async def on_finish(self, ctx: RunContext, outcome: RunOutcome) -> None:
        self.outcomes.append(outcome)
        await self.gate.wait()


@dataclass
class Budget:
    deadline: float = math.inf


@dataclass
class Resources:
    settings: Settings
    runs: RunsRepository | None
    flows: FlowRegistry
    checkpointer: Checkpointer
    observers: list[Any]
    queue: RunQueue | None
    active_runs: ActiveRuns = field(default_factory=ActiveRuns)
    shutdown_budget: Budget = field(default_factory=Budget)
    observability: Observability = field(
        default_factory=lambda: Observability(metrics=LoggingMetrics())
    )


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
    settings: Settings = SETTINGS,
) -> tuple[Resources, FakeRuns]:
    claim = Claim(outcome, run, TOKEN if outcome is ClaimOutcome.CLAIMED else None)
    fake = FakeRuns(claim, heartbeats)
    saver = saver or InMemorySaver()
    flows = {
        name: FlowSpec(target=f"{MODULE}:{name}", version=FLOW_VERSION, timeout_s=timeout_s)
        for name in ("work", "ask", "slow", "boom", "drain", "chain", "fanout")
    }
    ctx_factory: Any = lambda name: object()  # noqa: E731
    registry = FlowRegistry(flows, saver, ctx_factory)
    res = Resources(settings, cast(RunsRepository, fake), registry, saver, [], FakeQueue())
    return res, fake


async def _events(res: Resources, run: Run, **kw: Any) -> list[RunEvent]:
    return [e async for e in execute_run(res, run.run_id, **kw)]
