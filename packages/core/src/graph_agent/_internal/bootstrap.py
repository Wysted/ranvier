"""Composition root: builds every shared resource once and tears it down in reverse order.

`open_container` is the only place that wires pools, limits, LLM registry, tools, MCP sources,
tracing and the checkpointer (design sections 2, 8). Each resource is entered into one
`AsyncExitStack` right after it is created, so a failure mid-startup closes what is already open.
"""

import asyncio
import logging
import math
import threading
from collections.abc import AsyncIterator, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass, field
from functools import partial
from typing import Any

from psycopg_pool import AsyncConnectionPool

from graph_agent._internal.agents.factory import build_agent
from graph_agent._internal.agents.mcp import McpToolSource, open_mcp_sources
from graph_agent._internal.agents.tools import ToolRegistry
from graph_agent._internal.config.settings import Settings
from graph_agent._internal.db.pool import (
    CheckpointerPool,
    log_connection_budget,
    open_app_pool,
    open_checkpointer_pool,
)
from graph_agent._internal.limits import (
    HANDOVER_MARGIN_S,
    CpuPool,
    Limits,
    LoopLagSampler,
    graceful_wait_s,
    install_default_executor,
)
from graph_agent._internal.llm import LlmRegistry
from graph_agent._internal.observability.tracing import Observability, setup_observability
from graph_agent._internal.queue import ProcrastinateRunQueue, make_app
from graph_agent._internal.runtime.checkpointer import (
    Checkpointer,
    create_checkpointer,
    create_memory_checkpointer,
)
from graph_agent._internal.runtime.context import FlowContextImpl
from graph_agent._internal.runtime.events import RunObserver
from graph_agent._internal.runtime.flows import FlowRegistry
from graph_agent._internal.runtime.runner import ActiveRuns, RunQueue, settle_runs, start_drain
from graph_agent._internal.runtime.runs import RunsRepository
from graph_agent._internal.usage import UsageObserver, UsageRepository

logger = logging.getLogger(__name__)

GAUGE_INTERVAL_S = 10.0

type GaugeSource = Callable[[], float]


class ShutdownBudget:
    """One deadline shared by every teardown step that can block (flush, T26 drain)."""

    def __init__(self, grace_s: float) -> None:
        self._grace_s = grace_s
        self.deadline = math.inf  # until close starts

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        """Idempotent: the signal handler starts the clock, the container close must not move it."""
        if math.isinf(self.deadline):
            self.deadline = loop.time() + self._grace_s


@dataclass(slots=True)
class Container:
    """Everything the entrypoints share. Pools are None in memory mode."""

    settings: Settings
    app_pool: AsyncConnectionPool | None
    checkpointer_pool: CheckpointerPool | None
    checkpointer: Checkpointer
    runs: RunsRepository | None  # None in memory mode: no in-memory repository
    llm: LlmRegistry
    limits: Limits
    cpu_pool: CpuPool
    lag_sampler: LoopLagSampler
    observability: Observability
    tools: ToolRegistry
    mcp: McpToolSource
    shutdown_budget: ShutdownBudget
    flows: FlowRegistry
    observers: list[RunObserver] = field(default_factory=list)
    active_runs: ActiveRuns = field(default_factory=ActiveRuns)
    queue: RunQueue | None = None  # T24 sets it; None: inline-only process

    def begin_shutdown(self) -> None:
        """Idempotent, sync, loop thread: start the clock, refuse new inline runs, drain the rest.

        The entrypoint calls it on SIGTERM/SIGINT, before it waits for connections or jobs; the
        close of `open_container` then waits (bounded) for the runs and closes the pools.
        """
        self.limits.stop_admission()
        start_drain(self.active_runs, self.shutdown_budget)


@asynccontextmanager
async def open_container(
    settings: Settings,
    *,
    memory: bool = False,
    extra_connections: int = 0,
    queue_pool_max: int | None = None,
) -> AsyncIterator[Container]:
    """Open every resource; closing is the exact reverse and also runs when startup fails.

    `extra_connections`: connections the entrypoint holds outside the pools (the worker's), only
    counted in the logged connection budget. `queue_pool_max` overrides the Procrastinate pool
    size (the API passes `API_QUEUE_POOL_MAX`: it only defers).
    """
    async with AsyncExitStack() as stack:
        loop = asyncio.get_running_loop()
        # First in, last out: later teardown steps (observability flush) still use to_thread.
        _warn_short_grace(settings)
        executor = install_default_executor(loop, settings.limits.sync_threads)
        stack.callback(_restore_default_executor, loop, executor)

        # Sync and may import the OTel extra: off the loop.
        obs = await asyncio.to_thread(setup_observability, settings)
        budget = ShutdownBudget(settings.limits.shutdown_grace_s)
        stack.push_async_callback(_shutdown_observability, obs, budget)

        gauges: dict[str, GaugeSource] = {}
        app_pool: AsyncConnectionPool | None = None
        cp_pool: CheckpointerPool | None = None
        runs: RunsRepository | None = None
        queue: RunQueue | None = None
        if memory:
            checkpointer: Checkpointer = create_memory_checkpointer()
        else:
            db = settings.database
            queue_pool_max = queue_pool_max or db.procrastinate_pool_max
            log_connection_budget(
                db.app_pool_max,
                db.checkpointer_pool_max,
                queue_pool_max + extra_connections,
            )
            url = db.url.get_secret_value()
            app_pool = await open_app_pool(url, db.app_pool_max)
            stack.push_async_callback(app_pool.close)
            runs = RunsRepository(app_pool)  # the pool owns the connections: no teardown
            cp_pool = await open_checkpointer_pool(url, db.checkpointer_pool_max)
            stack.push_async_callback(cp_pool.close)
            queue_app = make_app(settings, queue_pool_max)
            await stack.enter_async_context(queue_app.open_async())
            queue = ProcrastinateRunQueue(queue_app)
            for label, pool in (("app", app_pool), ("checkpointer", cp_pool)):
                gauges[f"db_pool_{label}_size"] = _pool_stat(pool, "pool_size")
                gauges[f"db_pool_{label}_waiting"] = _pool_stat(pool, "requests_waiting")
            # Inside the running loop: AsyncPostgresSaver captures it in __init__.
            checkpointer = create_checkpointer(cp_pool)

        llm = LlmRegistry(settings.llm)
        stack.push_async_callback(llm.aclose)
        # model() does sync I/O on first use per provider; warm the cache off the loop.
        await asyncio.to_thread(_prime_models, llm, list(settings.llm.profiles))

        limits = Limits(settings.limits)
        sampler = LoopLagSampler(settings.limits.loop_lag_warn_ms)
        await stack.enter_async_context(sampler)

        # The constructor allocates semaphores and touches the filesystem: off the loop.
        cpu_pool = await asyncio.to_thread(CpuPool, settings.limits.cpu_workers)
        stack.callback(cpu_pool.close)  # registered first: kills workers if the warm-up fails
        await cpu_pool.start()  # workers up before run admission opens

        tools = ToolRegistry(settings.tools, settings.limits, cpu_pool)
        mcp = await open_mcp_sources(settings.mcp.servers, stack)
        # The first resolution imports user modules (blocking I/O); flow factories call
        # `ctx.tools()` on the loop, so warm the registry off it.
        await asyncio.to_thread(tools.resolve, list(settings.tools))

        def flow_context(name: str) -> FlowContextImpl:
            return FlowContextImpl(
                flow=name,
                config=settings.flows[name].config,
                model_fn=llm.model,
                tools_fn=lambda *names: tools.resolve(names),
                agent_fn=partial(build_agent, models=llm.model, registry=tools, mcp=mcp),
            )

        flows = FlowRegistry(settings.flows, checkpointer, flow_context)

        active_runs = ActiveRuns()
        gauges |= {
            "active_attempts": lambda: len(active_runs),
            "loop_lag_ms": lambda: sampler.current_ms,
            "inline_runs": lambda: limits.active_runs,
            "inline_admission_waiters": lambda: limits.waiters,
            "cpu_free_slots": lambda: cpu_pool.free_slots,
            # shortcut: private ThreadPoolExecutor._work_queue (queued, not busy, threads);
            # replace with an own counter if occupancy itself is needed.
            "sync_executor_queued": lambda: executor._work_queue.qsize(),
        }
        publisher = asyncio.create_task(_publish_gauges(obs, gauges), name="gauge-publisher")
        publisher.add_done_callback(_log_publisher_death)
        stack.push_async_callback(_cancel, publisher)
        # Registered last, so it runs first: start the shared clock (unless the entrypoint did),
        # drain the runs in flight and await them before any pool closes.
        stack.push_async_callback(_settle_runs, active_runs, budget)

        yield Container(
            settings=settings,
            app_pool=app_pool,
            checkpointer_pool=cp_pool,
            checkpointer=checkpointer,
            runs=runs,
            llm=llm,
            limits=limits,
            cpu_pool=cpu_pool,
            lag_sampler=sampler,
            observability=obs,
            tools=tools,
            mcp=mcp,
            shutdown_budget=budget,
            flows=flows,
            active_runs=active_runs,
            queue=queue,
            # Memory mode has no ledger: the runner still counts for the `end` event.
            observers=[UsageObserver(UsageRepository(app_pool))] if app_pool is not None else [],
        )


def _warn_short_grace(settings: Settings) -> None:
    """A drain waits for the superstep in flight: the real wait should outlast the longest I/O."""
    grace = settings.limits.shutdown_grace_s
    wait = graceful_wait_s(grace)
    if grace > 0 and wait == 0:
        logger.warning(
            "limits.shutdown_grace_s (%gs) is not above the %gs hand-over margin: drain disabled, "
            "a shutdown cancels runs at once and their nodes re-run",
            grace,
            HANDOVER_MARGIN_S,
        )
        return
    tool_default = settings.limits.tool_timeout_s
    timeouts = [
        *((f"tool {name}", t.timeout_s or tool_default) for name, t in settings.tools.items()),
        *((f"mcp server {name}", m.timeout_s) for name, m in settings.mcp.servers.items()),
        *((f"llm profile {name}", p.timeout_s) for name, p in settings.llm.profiles.items()),
    ]
    if timeouts and wait < (longest := max(timeouts, key=lambda item: item[1]))[1]:
        logger.warning(
            "the shutdown drain waits %gs (limits.shutdown_grace_s %gs minus the %gs hand-over "
            "margin), less than the %gs of %s: a node still inside it at the deadline is "
            "cancelled and the run is handed over, so that node re-runs",
            wait,
            grace,
            HANDOVER_MARGIN_S,
            longest[1],
            longest[0],
        )


def _prime_models(llm: LlmRegistry, profiles: list[str]) -> None:
    for name in profiles:
        llm.model(name)


def _pool_stat(pool: AsyncConnectionPool[Any], key: str) -> GaugeSource:
    return lambda: pool.get_stats().get(key, 0)


def _restore_default_executor(
    loop: asyncio.AbstractEventLoop, executor: ThreadPoolExecutor
) -> None:
    # uvloop exposes no public default executor to put back: give the loop a fresh stock one.
    loop.set_default_executor(ThreadPoolExecutor(thread_name_prefix="asyncio"))
    executor.shutdown(wait=False)


async def _settle_runs(active: ActiveRuns, budget: ShutdownBudget) -> None:
    start_drain(active, budget)  # idempotent: the entrypoint may have begun already
    await settle_runs(active, budget)


async def _shutdown_observability(obs: Observability, budget: ShutdownBudget) -> None:
    """`Observability.shutdown` blocks (exporters); it gets what is left of the shutdown budget.

    A daemon thread, not `to_thread`: an abandoned flush (collector down) must not keep the
    process alive past the deadline.
    """
    loop = asyncio.get_running_loop()
    budget.start(loop)  # idempotent; bounds the flush even if startup aborted before the drain
    done: asyncio.Future[None] = loop.create_future()

    def settle(exc: BaseException | None) -> None:
        if done.done():  # cancelled by the timeout
            return
        if exc is None:
            done.set_result(None)
        else:
            done.set_exception(exc)

    def flush() -> None:
        try:
            obs.shutdown()
            outcome = None
        except BaseException as exc:  # handed to the awaiting side below
            outcome = exc
        with suppress(RuntimeError):  # the loop may be gone when an abandoned flush ends
            loop.call_soon_threadsafe(settle, outcome)

    threading.Thread(target=flush, name="observability-shutdown", daemon=True).start()
    try:
        async with asyncio.timeout_at(budget.deadline):
            await done
    except TimeoutError:
        logger.warning("observability shutdown exceeded the shutdown grace, abandoning flush")


def _sample(obs: Observability, sources: dict[str, GaugeSource]) -> None:
    for name, source in sources.items():
        obs.metrics.gauge(name, float(source()))


async def _publish_gauges(obs: Observability, sources: dict[str, GaugeSource]) -> None:
    while True:
        await asyncio.sleep(GAUGE_INTERVAL_S)
        _sample(obs, sources)


def _log_publisher_death(task: asyncio.Task[None]) -> None:
    if not task.cancelled() and (exc := task.exception()) is not None:
        logger.error("gauge publisher died", exc_info=exc)


async def _cancel(task: asyncio.Task[None]) -> None:
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
