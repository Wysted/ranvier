"""Check 5: AsyncPostgresSaver per-instance lock, measured.

Workload: RUNS concurrent "runs", each on its own thread, doing STEPS supersteps of
`aput_writes` + `aput` (what the Pregel loop does per step), then one `aget_tuple`. Built with the
platform's own `open_checkpointer_pool` and `PooledPostgresSaver`. Variants:
  upstream x1   one AsyncPostgresSaver over the pool (per-instance asyncio.Lock)
  upstream x2   two AsyncPostgresSaver instances over the same pool, runs split between them
  pooled        PooledPostgresSaver (lock replaced by a semaphore sized to the pool)
Each variant runs direct (local socket) and through a TCP proxy adding ONE_WAY_MS per direction
(simulates a DB one network hop away). `inflight` = max pool connections held at once.
Also checks a nested call on the same saver inside its `alist` (non-reentrant lock).
"""

import asyncio
import statistics
import time
import uuid
from contextlib import asynccontextmanager

from common import PORT, drop_db, dsn, recreate_db
from graph_agent._internal.db.pool import open_checkpointer_pool
from graph_agent._internal.runtime.checkpointer import PooledPostgresSaver
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

DB = "s4_saver"
RUNS = 32  # limits.max_active_runs default
STEPS = 5
PAYLOAD = "x" * 2048
ONE_WAY_MS = 1.0
POOL_SIZES = (2, 5, 10)
REPEATS = 3


async def delay_proxy(one_way_s: float) -> tuple[asyncio.Server, int]:
    async def pipe(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        try:
            while data := await r.read(65536):
                await asyncio.sleep(one_way_s)
                w.write(data)
                await w.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            w.close()

    async def handle(cr: asyncio.StreamReader, cw: asyncio.StreamWriter) -> None:
        sr, sw = await asyncio.open_connection("127.0.0.1", int(PORT))
        await asyncio.gather(pipe(cr, sw), pipe(sr, cw))

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


def count_inflight(pool) -> dict[str, int]:
    stats = {"now": 0, "max": 0}
    orig = pool.connection

    @asynccontextmanager
    async def connection(*a, **kw):
        async with orig(*a, **kw) as conn:
            stats["now"] += 1
            stats["max"] = max(stats["max"], stats["now"])
            try:
                yield conn
            finally:
                stats["now"] -= 1

    pool.connection = connection
    return stats


async def one_run(saver: AsyncPostgresSaver) -> None:
    config = {"configurable": {"thread_id": str(uuid.uuid4()), "checkpoint_ns": ""}}
    for step in range(1, STEPS + 1):
        cp = empty_checkpoint()
        cp["channel_values"] = {"messages": [PAYLOAD] * step}
        cp["channel_versions"] = {"messages": str(step)}
        config = await saver.aput(config, cp, {"step": step}, {"messages": str(step)})
        await saver.aput_writes(
            config, [("messages", PAYLOAD)], task_id=str(uuid.uuid4())
        )
    assert await saver.aget_tuple(config) is not None


async def bench(url: str, variant: str, pool_size: int) -> tuple[float, int]:
    pool = await open_checkpointer_pool(url, pool_size)
    stats = count_inflight(pool)
    try:
        if variant == "pooled":
            savers: list[AsyncPostgresSaver] = [PooledPostgresSaver(pool)]
        else:
            savers = [AsyncPostgresSaver(pool) for _ in range(int(variant[-1]))]
        t = time.perf_counter()
        await asyncio.gather(*(one_run(savers[i % len(savers)]) for i in range(RUNS)))
        return time.perf_counter() - t, stats["max"]
    finally:
        await pool.close()


async def nested_in_alist(url: str, cls: type[AsyncPostgresSaver]) -> str:
    pool = await open_checkpointer_pool(url, 5)
    try:
        saver = PooledPostgresSaver(pool) if cls is PooledPostgresSaver else cls(pool)
        cfg = {"configurable": {"thread_id": "nested", "checkpoint_ns": ""}}
        await saver.aput(cfg, empty_checkpoint(), {}, {})

        async def nested() -> None:
            async for _ in saver.alist(cfg):
                await saver.aget_tuple(cfg)

        try:
            await asyncio.wait_for(nested(), 2)
            return "ok"
        except TimeoutError:
            return "DEADLOCK (timed out after 2 s)"
    finally:
        await pool.close()


async def main() -> None:
    recreate_db(DB)
    pool = await open_checkpointer_pool(dsn(DB), 2)
    await PooledPostgresSaver(pool).setup()
    await pool.close()

    proxy, proxy_port = await delay_proxy(ONE_WAY_MS / 1000)
    urls = {
        "direct": dsn(DB),
        f"+{2 * ONE_WAY_MS:g}ms RTT": dsn(DB).replace(f":{PORT}/", f":{proxy_port}/"),
    }
    print(
        f"RUNS={RUNS} STEPS={STEPS} -> {RUNS * STEPS * 2 + RUNS} DB calls per sample, "
        f"median of {REPEATS}"
    )
    for net, url in urls.items():
        await bench(url, "pooled", 2)  # warm-up
        for size in POOL_SIZES:
            for variant in ("upstream x1", "upstream x2", "pooled"):
                samples = [await bench(url, variant, size) for _ in range(REPEATS)]
                wall = statistics.median(s[0] for s in samples)
                inflight = max(s[1] for s in samples)
                print(
                    f"{net:>12} pool={size:<2} {variant:<12} wall={wall * 1000:7.1f} ms "
                    f"calls/s={(RUNS * STEPS * 2 + RUNS) / wall:7.0f} inflight={inflight}"
                )
    for cls in (AsyncPostgresSaver, PooledPostgresSaver):
        print(
            f"nested aget_tuple inside alist, {cls.__name__}: {await nested_in_alist(dsn(DB), cls)}"
        )
    proxy.close()
    drop_db(DB)
    print("saver_check: OK")


asyncio.run(main())
