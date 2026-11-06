"""Bench: N concurrent runs through the real container with a fake LLM (design section 11, Bench).

Each run is a loop flow of `--steps` supersteps; every step awaits a fake chat model that sleeps
`--step-latency-s` (a stand-in for the LLM call). Runs go through `execute_run` against Postgres
like an inline API run minus HTTP and admission (`limits.admit()` is not called): claim,
heartbeat task, synchronous checkpoints, usage ledger. Runs last seconds, below the default
`runs.heartbeat_s` (15 s), so no heartbeat is ever written. The script creates a throwaway
database on the server of `--admin-url` and drops it at the end.

    cd packages/core && uv run python ../../bench/bench_runs.py -n 50 --steps 5 --step-latency-s 0.2

Reports p50/p95 run time (row creation to terminal event), the maximum event-loop lag (own 10 ms
sampler and the container's 1 s one) and the peak RSS of the process (VmHWM, parent process only:
the CPU-pool workers are not counted). No provider keys, no network beyond Postgres.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import sys
import time
import uuid
from collections.abc import Sequence
from contextlib import aclosing, suppress
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import urlsplit

import psycopg
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.graph import END, START, StateGraph
from psycopg import sql
from pydantic import SecretStr

from ranvier._internal.bootstrap import Container, open_container
from ranvier._internal.cli import _migrate_database
from ranvier._internal.config.manifest import FlowSpec
from ranvier._internal.config.settings import (
    DEFAULT_DATABASE_URL,
    DatabaseSettings,
    LimitsSettings,
    Settings,
)
from ranvier._internal.runtime.context import FlowContext
from ranvier._internal.runtime.events import EndEvent, ErrorEvent
from ranvier._internal.runtime.runner import execute_run

FLOW = "bench"
# The manifest imports the target by module name: this file is on sys.path as the script dir.
FLOW_TARGET = "bench_runs:bench_flow"
LAG_SAMPLE_S = 0.01
MS = 1000.0
KIB_PER_MIB = 1024


class LatencyChatModel(BaseChatModel):
    """Fake model: sleeps `latency_s`, replies "ok". Stateless, so runs may share one."""

    latency_s: float

    @property
    def _llm_type(self) -> str:
        return "bench-fake"

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        raise NotImplementedError("bench model is async only")

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        await asyncio.sleep(self.latency_s)
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content="ok"))])


class State(TypedDict, total=False):
    step: int
    reply: str


def bench_flow(ctx: FlowContext) -> StateGraph[State, Any, State, State]:
    """Manifest target: a loop of `steps` model calls (config: steps, step_latency_s)."""
    steps = int(ctx.config["steps"])
    model = LatencyChatModel(latency_s=float(ctx.config["step_latency_s"]))

    async def step(state: State) -> State:
        reply = await model.ainvoke("ping")
        return {"step": state.get("step", 0) + 1, "reply": str(reply.content)}

    def again(state: State) -> str:
        return "step" if state["step"] < steps else END

    return (
        StateGraph(State)
        .add_node("step", step)
        .add_edge(START, "step")
        .add_conditional_edges("step", again, ["step", END])
    )


def rss_mib(field: str) -> float:
    """`VmRSS` or `VmHWM` of this process, in MiB (Linux)."""
    for line in Path("/proc/self/status").read_text().splitlines():
        if line.startswith(f"{field}:"):
            return int(line.split()[1]) / KIB_PER_MIB
    raise RuntimeError(f"{field} not found in /proc/self/status")


class LagSampler:
    """Max overshoot of a short sleep: the time the loop was blocked or starved."""

    def __init__(self) -> None:
        self.max_ms = 0.0
        self._task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> LagSampler:
        self._task = asyncio.create_task(self._run(), name="bench-lag")
        return self

    async def __aexit__(self, *exc: object) -> None:
        assert self._task is not None
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task

    async def _run(self) -> None:
        while True:
            start = time.perf_counter()
            await asyncio.sleep(LAG_SAMPLE_S)
            self.max_ms = max(self.max_ms, (time.perf_counter() - start - LAG_SAMPLE_S) * MS)


def percentile(values: Sequence[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(q * (len(ordered) - 1)))]


async def one_run(c: Container) -> tuple[float, str]:
    """Create the row and drive it like an inline run; returns (seconds, terminal status)."""
    assert c.runs is not None
    start = time.perf_counter()
    run = await c.runs.create(thread_id=uuid.uuid4(), flow=FLOW, flow_version=1, input={})
    status = "no_terminal_event"
    async with aclosing(execute_run(c, run.run_id)) as events:
        async for event in events:
            if isinstance(event, EndEvent):
                status = event.status
            elif isinstance(event, ErrorEvent):
                status = f"error:{event.code}"
    return time.perf_counter() - start, status


def admin_exec(admin_url: str, statement: sql.Composed) -> None:
    with psycopg.connect(admin_url, autocommit=True, connect_timeout=3) as conn:
        conn.execute(statement)


async def bench(args: argparse.Namespace) -> dict[str, Any]:
    name = f"ranvier_bench_{uuid.uuid4().hex[:12]}"
    await asyncio.to_thread(
        admin_exec, args.admin_url, sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name))
    )
    try:
        url = urlsplit(args.admin_url)._replace(path=f"/{name}").geturl()
        settings = Settings(
            _env_file=None,
            database=DatabaseSettings(url=SecretStr(url)),
            limits=LimitsSettings(shutdown_grace_s=10),
            flows={
                FLOW: FlowSpec(
                    target=FLOW_TARGET,
                    config={"steps": args.steps, "step_latency_s": args.step_latency_s},
                )
            },
        )
        await _migrate_database(settings)
        async with open_container(settings) as c, LagSampler() as lag:
            await c.flows.get(FLOW)  # build once: the bench measures runs, not the first import
            rss_before = rss_mib("VmRSS")
            started = time.perf_counter()
            results = await asyncio.gather(*(one_run(c) for _ in range(args.n)))
            wall_s = time.perf_counter() - started
            container_lag_ms = c.lag_sampler.max_ms
            rss_after = rss_mib("VmRSS")
        times = [t for t, _ in results]
        return {
            "n": args.n,
            "steps": args.steps,
            "step_latency_s": args.step_latency_s,
            "ideal_run_s": round(args.steps * args.step_latency_s, 3),
            "wall_s": round(wall_s, 3),
            "p50_run_s": round(statistics.median(times), 3),
            "p95_run_s": round(percentile(times, 0.95), 3),
            "max_run_s": round(max(times), 3),
            "max_loop_lag_ms": round(lag.max_ms, 1),
            "container_max_loop_lag_ms": round(container_lag_ms, 1),
            "rss_before_mib": round(rss_before, 1),
            "rss_after_mib": round(rss_after, 1),
            "peak_rss_mib": round(rss_mib("VmHWM"), 1),
            "statuses": sorted({s for _, s in results}),
            "python": platform.python_version(),
            "machine": f"{platform.system()} {platform.release()} {platform.machine()}",
        }
    finally:
        await asyncio.to_thread(
            admin_exec,
            args.admin_url,
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)),
        )


def main() -> int:
    p = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    p.add_argument("-n", type=int, default=50, help="concurrent runs (default 50)")
    p.add_argument("--steps", type=int, default=5, help="model calls per run (default 5)")
    p.add_argument("--step-latency-s", type=float, default=0.2, help="fake LLM latency (0.2)")
    p.add_argument(
        "--admin-url", default=DEFAULT_DATABASE_URL, help="server that may CREATE DATABASE"
    )
    args = p.parse_args()
    if min(args.n, args.steps) < 1 or args.step_latency_s < 0:
        p.error("n and steps must be >= 1 and the latency >= 0")
    summary = asyncio.run(bench(args))
    print(json.dumps(summary, indent=2))
    return 0 if summary["statuses"] == ["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
