"""Platform-owned global limits: run admission, thread executor, CPU process slots, loop lag.

Everything is created once in the composition root; nothing here is a module-level singleton.

Entrypoints that use `CpuPool` need the `if __name__ == "__main__"` guard: the forkserver
re-imports the parent's `__main__` in its children.
"""

import asyncio
import contextlib
import logging
import multiprocessing
import signal
import time
from collections.abc import AsyncIterator, Callable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from contextlib import asynccontextmanager
from types import TracebackType
from typing import Any, Final, Self

from graph_agent._internal.config.settings import LimitsSettings

_log = logging.getLogger(__name__)

LAG_INTERVAL_S = 1.0
_MS_PER_S = 1000.0
# Left of `limits.shutdown_grace_s` for the hand-over of runs the server cancels at its own
# timeout (release + defer, 2 statements) and for flushing telemetry.
HANDOVER_MARGIN_S: Final = 5.0


def graceful_wait_s(grace_s: float) -> float:
    """How long uvicorn / the Procrastinate worker wait for runs before cancelling them."""
    return max(grace_s - HANDOVER_MARGIN_S, 0.0)


class TooManyRuns(Exception):
    """No run slot became free within `limits.admission_wait_s` (HTTP 429 `too_many_runs`)."""


class ShuttingDown(Exception):
    """Admission is closed: the process is shutting down (HTTP 503 `shutting_down`)."""


class Limits:
    """Run admission semaphore."""

    def __init__(self, settings: LimitsSettings) -> None:
        self._max = settings.max_active_runs
        self._wait_s = settings.admission_wait_s
        self._sem = asyncio.Semaphore(self._max)
        self._active = 0
        self._waiters = 0
        self._stopped = False

    def stop_admission(self) -> None:
        """Refuse every later `admit` (and the ones still waiting for a slot). Terminal."""
        self._stopped = True

    @asynccontextmanager
    async def admit(self) -> AsyncIterator[None]:
        if self._stopped:
            raise ShuttingDown("shutting down")
        self._waiters += 1
        try:
            async with asyncio.timeout(self._wait_s):
                await self._sem.acquire()
        except TimeoutError:
            raise TooManyRuns(f"{self._max} runs active, none freed in {self._wait_s}s") from None
        finally:
            self._waiters -= 1
        if self._stopped:  # woke up while the shutdown began
            self._sem.release()
            raise ShuttingDown("shutting down")
        self._active += 1
        try:
            yield
        finally:
            self._active -= 1
            self._sem.release()

    @property
    def active_runs(self) -> int:
        return self._active

    @property
    def waiters(self) -> int:
        return self._waiters


def install_default_executor(
    loop: asyncio.AbstractEventLoop, sync_threads: int
) -> ThreadPoolExecutor:
    """Bound the threads used by `to_thread` / `run_in_executor(None, ...)`."""
    executor = ThreadPoolExecutor(max_workers=sync_threads, thread_name_prefix="ga-sync")
    loop.set_default_executor(executor)
    return executor


_FORKSERVER = multiprocessing.get_context("forkserver")  # 3.13 defaults to fork


def _ignore_sigint() -> None:
    """Pool worker initializer: Ctrl+C (and systemd's cgroup-wide SIGTERM) reach every process,
    but only the parent decides when workers die (it kills them on close; they exit if it dies)."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)


def _kill_pool(pool: ProcessPoolExecutor) -> None:
    kill = getattr(pool, "kill_workers", None)
    if kill is not None:
        kill()
        return
    # shortcut: private ProcessPoolExecutor._processes on 3.13,
    # delete when requires-python >=3.14 (OD-7 b)
    procs = dict(pool._processes or {})
    pool.shutdown(wait=False, cancel_futures=True)
    for proc in procs.values():
        try:
            proc.kill()
        except (ValueError, ProcessLookupError):
            continue


class CpuPool:
    """`cpu_workers` single-worker process executors; one call per slot (see docs/spikes/S5.md).

    A timeout, caller cancellation or worker crash replaces only that slot's executor and kills
    its worker. Submit only from the event-loop thread and swap before kill (3.13 backport race).
    """

    def __init__(self, workers: int) -> None:
        self._pools = [self._new() for _ in range(workers)]
        self._free: asyncio.Queue[int] = asyncio.Queue()
        for slot in range(workers):
            self._free.put_nowait(slot)
        self.recycles = 0
        self._closed = False

    @staticmethod
    def _new() -> ProcessPoolExecutor:
        return ProcessPoolExecutor(
            max_workers=1, mp_context=_FORKSERVER, initializer=_ignore_sigint
        )

    async def start(self) -> None:
        """Spawn all workers off the loop; call before admitting runs (no concurrent recycle)."""
        await asyncio.to_thread(self._warm)

    def _warm(self) -> None:
        for pool in self._pools:
            pool.submit(int).result()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    async def run(self, fn: Callable[..., Any], *args: Any, timeout_s: float) -> Any:
        """Raises TimeoutError (also while waiting for a slot) or BrokenProcessPool."""
        if self._closed:
            raise RuntimeError("CpuPool is closed")
        async with asyncio.timeout(timeout_s):
            slot = await self._free.get()
            try:
                try:
                    fut = self._pools[slot].submit(fn, *args)  # raises if the idle worker died
                except BrokenProcessPool:
                    self._recycle(slot)
                    raise
                try:
                    return await asyncio.wrap_future(fut)
                except BrokenProcessPool:
                    self._recycle(slot)
                    raise
                except asyncio.CancelledError:
                    # Running work only stops with a kill; a not-yet-started one cancels cleanly.
                    if not fut.cancel() and not fut.done():
                        self._recycle(slot)
                    raise
            finally:
                self._free.put_nowait(slot)

    def _recycle(self, slot: int) -> None:
        # shortcut: recycle spawns on the loop (~2 ms per timeout/crash),
        # move to a thread if loop lag shows it
        old = self._pools[slot]
        if not self._closed:
            self._pools[slot] = self._new()
        self.recycles += 1
        _kill_pool(old)

    @property
    def free_slots(self) -> int:
        return self._free.qsize()

    def close(self) -> None:
        """Terminal: kills all workers; later `run` calls raise and recycles stop respawning."""
        self._closed = True
        for pool in self._pools:
            _kill_pool(pool)


class LoopLagSampler:
    """Measures event-loop sleep drift; usable as `await stack.enter_async_context(sampler)`."""

    def __init__(self, warn_ms: float, interval_s: float = LAG_INTERVAL_S) -> None:
        self._warn_ms = warn_ms
        self._interval_s = interval_s
        self._task: asyncio.Task[None] | None = None
        self.current_ms = 0.0
        self.max_ms = 0.0

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="loop-lag-sampler")

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()

    async def _run(self) -> None:
        while True:
            start = time.perf_counter()
            await asyncio.sleep(self._interval_s)
            lag_ms = max(0.0, time.perf_counter() - start - self._interval_s) * _MS_PER_S
            self.current_ms = lag_ms
            self.max_ms = max(self.max_ms, lag_ms)
            if lag_ms > self._warn_ms:
                _log.warning("event loop lag %.0f ms (warn at %.0f ms)", lag_ms, self._warn_ms)
