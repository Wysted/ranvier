"""S5 spike: the process pool API recommended for limits.py, exercised on 3.13 and 3.14.

Compares two shapes behind the same `run(fn, *args, timeout_s)`:
- SharedPool: one ProcessPoolExecutor(cpu_workers); a timeout kills every worker.
- SlotPool (recommended): cpu_workers single-worker executors; a timeout kills only its own.

Run: bench/spikes/S5/run.sh (stdlib only). Throwaway; T09 owns the real implementation.
"""

import asyncio
import multiprocessing as mp
import os
import signal
import sys
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Any

from pool_check import kill_pool, proc_state, slow, stuck

CTX = mp.get_context("forkserver")  # explicit: 3.13 defaults to fork on Linux


class SharedPool:
    def __init__(self, workers: int, *, bounded: bool = True) -> None:
        self._workers = workers
        self._pool = self._new()
        # Never more submitted tasks than workers: the executor pre-feeds its call queue and
        # marks those futures running, so a queued task could not be cancelled on timeout.
        # bounded=False is the negative control for that claim.
        self._slots = asyncio.Semaphore(workers if bounded else 1_000_000)
        self.recycles = 0

    def _new(self) -> ProcessPoolExecutor:
        return ProcessPoolExecutor(max_workers=self._workers, mp_context=CTX)

    async def run(self, fn: Callable[..., Any], *args: Any, timeout_s: float) -> Any:
        async with asyncio.timeout(timeout_s), self._slots:
            pool = self._pool
            try:
                fut = pool.submit(fn, *args)
                return await asyncio.wrap_future(fut)
            except BrokenProcessPool:
                self._recycle(pool)
                raise
            except asyncio.CancelledError:
                if not fut.cancel() and not fut.done():
                    self._recycle(pool)
                raise

    def _recycle(self, pool: ProcessPoolExecutor) -> None:
        if pool is not self._pool:  # a concurrent timeout already replaced it
            return
        self._pool = self._new()
        self.recycles += 1
        kill_pool(pool)

    def worker_pids(self) -> list[int]:
        return list(self._pool._processes)  # observation only

    def close(self) -> None:
        kill_pool(self._pool)


class SlotPool:
    def __init__(self, workers: int) -> None:
        self._pools = [self._new() for _ in range(workers)]
        self._free: asyncio.Queue[int] = asyncio.Queue()
        for slot in range(workers):
            self._free.put_nowait(slot)
        self.recycles = 0

    @staticmethod
    def _new() -> ProcessPoolExecutor:
        return ProcessPoolExecutor(max_workers=1, mp_context=CTX)

    async def run(self, fn: Callable[..., Any], *args: Any, timeout_s: float) -> Any:
        async with asyncio.timeout(timeout_s):  # covers the wait for a free slot too
            slot = await self._free.get()
            try:
                pool = self._pools[slot]
                try:
                    fut = pool.submit(
                        fn, *args
                    )  # raises at once if its idle worker died
                    return await asyncio.wrap_future(fut)
                except BrokenProcessPool:  # worker crash or OOM kill
                    self._recycle(slot)
                    raise
                except asyncio.CancelledError:  # timeout or caller cancelled
                    if not fut.cancel() and not fut.done():
                        self._recycle(slot)  # running: only a kill stops it
                    raise
            finally:
                self._free.put_nowait(slot)

    def _recycle(self, slot: int) -> None:
        old, self._pools[slot] = self._pools[slot], self._new()
        self.recycles += 1
        kill_pool(old)

    def worker_pids(self) -> list[int]:
        return [
            pid for pool in self._pools for pid in (pool._processes or {})
        ]  # observation only

    def close(self) -> None:
        for pool in self._pools:
            kill_pool(pool)


def crash() -> None:
    os.kill(os.getpid(), signal.SIGKILL)


async def outcome(coro) -> str:
    try:
        res = await coro
    except (TimeoutError, BrokenProcessPool) as exc:
        return type(exc).__name__
    return type(res).__name__


async def main() -> None:
    print(f"python={sys.version.split()[0]}")

    # i) isolation: a stuck task times out while a healthy one (another run) is in flight.
    for cls in (SharedPool, SlotPool):
        pp = cls(workers=2)
        res = await asyncio.gather(
            outcome(pp.run(stuck, timeout_s=0.5)),
            outcome(pp.run(slow, 1.0, timeout_s=5)),
        )
        print(
            f"[i] {cls.__name__}: stuck={res[0]} healthy={res[1]} (want TimeoutError, int) recycles={pp.recycles}"
        )
        pp.close()

    pp = SlotPool(workers=2)

    # a) two stuck tasks time out together: each kills only its own worker.
    res = await asyncio.gather(
        *(outcome(pp.run(stuck, timeout_s=0.5)) for _ in range(2))
    )
    print(f"[a] concurrent timeouts: {res} recycles={pp.recycles} (expect 2)")

    # b) caller cancelled while the task runs (client disconnect): worker must not survive.
    task = asyncio.create_task(pp.run(stuck, timeout_s=30))
    await asyncio.sleep(0.5)
    pids = pp.worker_pids()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await asyncio.sleep(0.2)
    print(
        f"[b] caller cancelled: recycles={pp.recycles} (expect 3) live workers before cancel={[proc_state(p) for p in pids]} (expect all gone)"
    )

    # c) a queued task times out while every slot is busy with healthy work: no recycle.
    variants = (
        ("SharedPool(bounded=False)", lambda: SharedPool(workers=1, bounded=False)),
        ("SharedPool", lambda: SharedPool(workers=1)),
        ("SlotPool", lambda: SlotPool(workers=1)),
    )
    for name, make in variants:
        pp1 = make()
        await pp1.run(slow, 0, timeout_s=5)  # spawn the single worker
        busy = asyncio.create_task(outcome(pp1.run(slow, 1.5, timeout_s=5)))
        await asyncio.sleep(0.2)
        other = asyncio.create_task(outcome(pp1.run(slow, 0.1, timeout_s=5)))
        late = await outcome(pp1.run(slow, 0.1, timeout_s=0.3))
        print(
            f"[c] {name}: queued task {late} recycles={pp1.recycles} (expect 0) "
            f"healthy={[await busy, await other]} (expect int, int)"
        )
        pp1.close()

    # e) a worker dies on its own (segfault, OOM killer): the next call must not fail forever.
    before = pp.recycles
    first = await outcome(pp.run(crash, timeout_s=5))
    pid = await pp.run(slow, 0.1, timeout_s=5)
    print(
        f"[e] crashing task: {first} recycles+={pp.recycles - before} (expect 1) next call ok={isinstance(pid, int)}"
    )

    # f) an idle worker is killed from outside: its executor breaks with no future in flight.
    for p in pp.worker_pids():
        os.kill(p, signal.SIGKILL)
    await asyncio.sleep(0.3)
    calls = [await outcome(pp.run(slow, 0.1, timeout_s=5)) for _ in range(4)]
    print(
        f"[f] all idle workers killed, next 4 calls: {calls} (one BrokenProcessPool per dead slot, then int)"
    )

    pp.close()
    await asyncio.sleep(0.2)
    print(
        f"[z] after close: mp.active_children()={len(mp.active_children())} (expect 0)"
    )


if __name__ == "__main__":
    asyncio.run(main())
