"""Composition root: builds every shared resource once and tears it down in reverse order.

`open_container` is the only place that wires pools, limits, LLM registry, tools, MCP sources,
tracing and the checkpointer (design sections 2, 8). Each resource is entered into one
`AsyncExitStack` right after it is created, so a failure mid-startup closes what is already open.
"""

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass, field

from psycopg_pool import AsyncConnectionPool

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
from graph_agent._internal.runtime.events import RunObserver

logger = logging.getLogger(__name__)

GAUGE_INTERVAL_S = 10.0

type GaugeSource = Callable[[], float]


@dataclass(slots=True)
class Container:
    """Everything the entrypoints share. Pools are None in memory mode."""

    settings: Settings
    app_pool: AsyncConnectionPool | None
    checkpointer_pool: CheckpointerPool | None
    checkpointer: Checkpointer
    llm: LlmRegistry
    limits: Limits
    cpu_pool: CpuPool
    lag_sampler: LoopLagSampler
    observability: Observability
    tools: ToolRegistry
    mcp: McpToolSource
    observers: list[RunObserver] = field(default_factory=list)  # T32 appends


@asynccontextmanager
async def open_container(settings: Settings, *, memory: bool = False) -> AsyncIterator[Container]:
    """Open every resource; closing is the exact reverse and also runs when startup fails."""
    async with AsyncExitStack() as stack:
        loop = asyncio.get_running_loop()
        # First in, last out: later teardown steps (observability flush) still use to_thread.
        executor = install_default_executor(loop, settings.limits.sync_threads)
        stack.callback(executor.shutdown, wait=False)

        # Sync and may import the OTel extra: off the loop.
        obs = await asyncio.to_thread(setup_observability, settings)
        stack.push_async_callback(_shutdown_observability, obs, settings.limits.shutdown_grace_s)

        gauges: dict[str, GaugeSource] = {}
        app_pool: AsyncConnectionPool | None = None
        cp_pool: CheckpointerPool | None = None
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

        cpu_pool = CpuPool(settings.limits.cpu_workers)
        stack.callback(cpu_pool.close)  # registered first: kills workers if the warm-up fails
        await cpu_pool.start()  # workers up before run admission opens

        tools = ToolRegistry(settings.tools, settings.limits, cpu_pool)
        mcp = await open_mcp_sources(settings.mcp.servers, stack)

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
        stack.push_async_callback(_cancel, publisher)

        yield Container(
            settings=settings,
            app_pool=app_pool,
            checkpointer_pool=cp_pool,
            checkpointer=checkpointer,
            llm=llm,
            limits=limits,
            cpu_pool=cpu_pool,
            lag_sampler=sampler,
            observability=obs,
            tools=tools,
            mcp=mcp,
        )


def _prime_models(llm: LlmRegistry, profiles: list[str]) -> None:
    for name in profiles:
        llm.model(name)


def _pool_stat(pool: AsyncConnectionPool | CheckpointerPool, key: str) -> GaugeSource:
    return lambda: pool.get_stats().get(key, 0)


async def _shutdown_observability(obs: Observability, grace_s: float) -> None:
    """`Observability.shutdown` blocks (exporters); it gets at most the shutdown grace."""
    try:
        async with asyncio.timeout(grace_s):
            await asyncio.to_thread(obs.shutdown)
    except TimeoutError:
        logger.warning("observability shutdown exceeded %.0fs, abandoning flush", grace_s)


async def _publish_gauges(obs: Observability, sources: dict[str, GaugeSource]) -> None:
    while True:
        await asyncio.sleep(GAUGE_INTERVAL_S)
        for name, source in sources.items():
            obs.metrics.gauge(name, float(source()))


async def _cancel(task: "asyncio.Task[None]") -> None:
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
