"""Composition root: builds every shared resource once and tears it down in reverse order.

`open_container` is the only place that wires pools, limits, LLM registry, tools, MCP sources,
tracing and the checkpointer (design sections 2, 8). Each resource is entered into one
`AsyncExitStack` right after it is created, so a failure mid-startup closes what is already open.
"""

import asyncio
import logging
import math
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
from graph_agent._internal.limits import CpuPool, Limits, LoopLagSampler, install_default_executor
from graph_agent._internal.llm import LlmRegistry
from graph_agent._internal.observability.tracing import Observability, setup_observability
from graph_agent._internal.runtime.checkpointer import (
    Checkpointer,
    create_checkpointer,
    create_memory_checkpointer,
)
from graph_agent._internal.runtime.context import FlowContextImpl
from graph_agent._internal.runtime.events import RunObserver
from graph_agent._internal.runtime.flows import FlowRegistry
from graph_agent._internal.runtime.runner import ActiveRuns, RunQueue
from graph_agent._internal.runtime.runs import RunsRepository

logger = logging.getLogger(__name__)

GAUGE_INTERVAL_S = 10.0

type GaugeSource = Callable[[], float]


class ShutdownBudget:
    """One deadline shared by every teardown step that can block (flush, T26 drain)."""

    def __init__(self, grace_s: float) -> None:
        self._grace_s = grace_s
        self.deadline = math.inf  # until close starts

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
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
    observers: list[RunObserver] = field(default_factory=list)  # T32 appends
    active_runs: ActiveRuns = field(default_factory=ActiveRuns)  # T26 drains these
    queue: RunQueue | None = None  # T24 sets it; None: inline-only process


@asynccontextmanager
async def open_container(settings: Settings, *, memory: bool = False) -> AsyncIterator[Container]:
    """Open every resource; closing is the exact reverse and also runs when startup fails."""
    async with AsyncExitStack() as stack:
        loop = asyncio.get_running_loop()
        # First in, last out: later teardown steps (observability flush) still use to_thread.
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
        if memory:
            checkpointer: Checkpointer = create_memory_checkpointer()
        else:
            db = settings.database
            log_connection_budget(
                db.app_pool_max, db.checkpointer_pool_max, db.procrastinate_pool_max
            )
            url = db.url.get_secret_value()
            app_pool = await open_app_pool(url, db.app_pool_max)
            stack.push_async_callback(app_pool.close)
            runs = RunsRepository(app_pool)  # the pool owns the connections: no teardown
            cp_pool = await open_checkpointer_pool(url, db.checkpointer_pool_max)
            stack.push_async_callback(cp_pool.close)
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

        gauges |= {
            "loop_lag_ms": lambda: sampler.current_ms,
            "active_runs": lambda: limits.active_runs,
            "admission_waiters": lambda: limits.waiters,
            "cpu_free_slots": lambda: cpu_pool.free_slots,
            # shortcut: private ThreadPoolExecutor._work_queue (queued, not busy, threads);
            # replace with an own counter if occupancy itself is needed.
            "sync_executor_queued": lambda: executor._work_queue.qsize(),
        }
        publisher = asyncio.create_task(_publish_gauges(obs, gauges), name="gauge-publisher")
        publisher.add_done_callback(_log_publisher_death)
        stack.push_async_callback(_cancel, publisher)
        stack.callback(budget.start, loop)  # registered last: runs first, starts the shared clock

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


async def _shutdown_observability(obs: Observability, budget: ShutdownBudget) -> None:
    """`Observability.shutdown` blocks (exporters); it gets what is left of the shutdown budget."""
    try:
        async with asyncio.timeout_at(budget.deadline):
            await asyncio.to_thread(obs.shutdown)
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
