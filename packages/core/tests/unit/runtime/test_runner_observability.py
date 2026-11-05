import asyncio
import contextvars
import types
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from graph_agent._internal.observability.tracing import (
    ATTR_FLOW,
    ATTR_RUN_ID,
    ATTR_THREAD_ID,
    Observability,
)
from graph_agent._internal.runtime.runner import (
    METRIC_ATTEMPT_SECONDS,
    METRIC_ATTEMPTS,
    METRIC_CANCELLED_BEFORE_START,
    execute_run,
)
from graph_agent._internal.runtime.runs import ClaimOutcome
from unit.runtime._runner_doubles import _events, _resources, _run

# Like OTel's context: a token is only valid in the context that set it.
SPAN_VAR: contextvars.ContextVar[object | None] = contextvars.ContextVar("span", default=None)

type Sample = tuple[str, float, Mapping[str, str]]


@dataclass
class Recorder:
    gauges: list[Sample] = field(default_factory=list)
    counters: list[Sample] = field(default_factory=list)

    def gauge(self, name: str, value: float, attributes: Mapping[str, str] | None = None) -> None:
        self.gauges.append((name, value, attributes or {}))

    def counter(
        self, name: str, value: float = 1, attributes: Mapping[str, str] | None = None
    ) -> None:
        self.counters.append((name, value, attributes or {}))

    def attempts(self) -> list[str]:
        return [a["status"] for n, _, a in self.counters if n == METRIC_ATTEMPTS]


@dataclass
class Backend:
    metrics: Recorder
    spans: list[Mapping[str, str]] = field(default_factory=list)
    open_spans: int = 0

    @contextmanager
    def run_span(self, attributes: Mapping[str, str]) -> Iterator[object]:
        self.spans.append(attributes)
        self.open_spans += 1
        token = SPAN_VAR.set(object())
        try:
            yield SPAN_VAR.get()
        finally:
            SPAN_VAR.reset(token)  # ValueError when closed from another context
            self.open_spans -= 1

    def shutdown(self) -> None: ...


def _instrument(res: Any) -> Backend:
    backend = Backend(Recorder())
    res.observability = Observability(metrics=backend.metrics, backend=backend)
    return backend


async def test_success_sets_span_attributes_and_counters(
    user_module: types.ModuleType,
) -> None:
    run = _run("work")
    res, _ = _resources(run)
    backend = _instrument(res)
    await _events(res, run)
    assert backend.spans == [
        {ATTR_FLOW: "work", ATTR_RUN_ID: str(run.run_id), ATTR_THREAD_ID: str(run.thread_id)}
    ]
    assert backend.open_spans == 0
    rec = backend.metrics
    assert rec.attempts() == ["success"]
    assert [n for n, *_ in rec.counters].count(METRIC_ATTEMPT_SECONDS) == 1


async def test_client_disconnect_counts_cancelled_and_ends_the_span(
    user_module: types.ModuleType,
) -> None:
    run = _run("slow")
    res, _ = _resources(run)
    backend = _instrument(res)
    gen = execute_run(res, run.run_id)
    await anext(gen)
    assert backend.open_spans == 1
    await gen.aclose()
    await res.active_runs.settled()
    assert backend.open_spans == 0
    assert backend.metrics.attempts() == ["cancelled"]


async def test_closing_from_another_task_leaves_the_consumer_context_clean(
    user_module: types.ModuleType,
) -> None:
    run = _run("slow")
    res, _ = _resources(run)
    backend = _instrument(res)
    gen = execute_run(res, run.run_id)
    await anext(gen)
    assert SPAN_VAR.get() is None  # the span is not current in the consumer's context
    await asyncio.create_task(gen.aclose())  # Starlette closes the stream from another task
    await res.active_runs.settled()
    assert SPAN_VAR.get() is None
    assert backend.open_spans == 0
    assert backend.metrics.attempts() == ["cancelled"]


async def test_budget_overrun_counts_the_attempt_released_once(
    user_module: types.ModuleType,
) -> None:
    run = _run("slow")
    res, fake = _resources(run)
    backend = _instrument(res)
    res.shutdown_budget.deadline = asyncio.get_running_loop().time()  # already spent
    gen = execute_run(res, run.run_id)
    await anext(gen)
    await gen.aclose()
    await res.active_runs.settled()
    assert fake.finished == []  # the cleanup did not fit the budget
    assert backend.metrics.attempts() == ["released"]


async def test_error_lost_and_hand_over_count_once(
    user_module: types.ModuleType,
) -> None:
    run = _run("boom")
    res, _ = _resources(run)
    backend = _instrument(res)
    await _events(res, run)
    assert backend.metrics.attempts() == ["error"]

    run = _run("slow")
    res, _ = _resources(run, heartbeats=[None])  # lease lost at the first heartbeat
    backend = _instrument(res)
    await _events(res, run)
    assert backend.metrics.attempts() == ["released"]
    assert backend.open_spans == 0

    run = _run("slow")
    res, _ = _resources(run)
    backend = _instrument(res)
    task = asyncio.create_task(_events(res, run))
    await asyncio.sleep(0.05)
    res.active_runs.request_drain()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await res.active_runs.settled()
    assert backend.metrics.attempts() == ["released"]
    assert backend.open_spans == 0


async def test_claim_already_cancelled_is_not_an_attempt() -> None:
    run = _run("work")
    res, _ = _resources(run, ClaimOutcome.CANCELLED)
    backend = _instrument(res)
    await _events(res, run)
    assert backend.metrics.attempts() == []
    assert [n for n, *_ in backend.metrics.counters] == [METRIC_CANCELLED_BEFORE_START]
