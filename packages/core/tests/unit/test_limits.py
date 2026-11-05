import asyncio
import logging
import os
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool

import pytest
from blockbuster import blockbuster_ctx

from graph_agent._internal.config.settings import LimitsSettings
from graph_agent._internal.limits import (
    CpuPool,
    Limits,
    LoopLagSampler,
    TooManyRuns,
    install_default_executor,
)

TIMEOUT_S = 0.5


@pytest.fixture(autouse=True)
def _blockbuster(request: pytest.FixtureRequest) -> Iterator[None]:
    """Overrides conftest: starting a forkserver worker blocks the loop for a moment by design.

    Only the executor's own spawn code path is exempted, and only for the CpuPool tests.
    """
    with blockbuster_ctx() as bb:
        if request.node.name.startswith("test_cpu_pool"):
            for func in bb.functions.values():
                func.can_block_in("concurrent/futures/process.py", {"submit", "__init__"})
                func.can_block_in("_internal/limits.py", {"_new", "close", "_recycle"})
        yield


# Worker callables must be importable in the forkserver child, so use only stdlib ones.
_STUCK = ("import signal; signal.signal(signal.SIGTERM, signal.SIG_IGN)\nwhile True: pass",)


def _sleep(seconds: float) -> str:
    return f"import time; time.sleep({seconds})"


_SELFKILL = ("import os, signal; os.kill(os.getpid(), signal.SIGKILL)",)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _pids(pool: CpuPool) -> list[int]:
    return [pid for p in pool._pools for pid in (p._processes or {})]


async def _wait_gone(pid: int) -> bool:
    for _ in range(50):
        if not _alive(pid):
            return True
        await asyncio.sleep(0.02)
    return False


def _limits(max_runs: int = 1, wait_s: float = 0.1) -> Limits:
    return Limits(LimitsSettings(max_active_runs=max_runs, admission_wait_s=wait_s))


async def test_admit_times_out_when_full_and_releases() -> None:
    limits = _limits()
    async with limits.admit():
        assert limits.active_runs == 1
        with pytest.raises(TooManyRuns):
            async with limits.admit():
                pass
        assert limits.waiters == 0
    assert limits.active_runs == 0
    async with limits.admit():  # slot was released
        pass


async def test_admit_waiter_gets_slot_when_freed() -> None:
    limits = _limits(wait_s=2)
    entered = asyncio.Event()

    async def second() -> None:
        async with limits.admit():
            entered.set()

    async with limits.admit():
        task = asyncio.create_task(second())
        await asyncio.sleep(0.05)
        assert limits.waiters == 1
        assert not entered.is_set()
    await asyncio.wait_for(task, 1)
    assert entered.is_set()


async def test_admit_releases_on_cancellation() -> None:
    limits = _limits(wait_s=5)
    holding = asyncio.Event()

    async def hold() -> None:
        async with limits.admit():
            holding.set()
            await asyncio.sleep(10)

    task = asyncio.create_task(hold())
    await holding.wait()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert limits.active_runs == 0
    async with limits.admit():
        pass


async def test_install_default_executor_is_bounded() -> None:
    loop = asyncio.get_running_loop()
    executor = install_default_executor(loop, 3)
    assert isinstance(executor, ThreadPoolExecutor)
    assert executor._max_workers == 3
    assert await asyncio.to_thread(os.getpid) == os.getpid()  # runs on the installed executor


async def test_lag_sampler_sees_blocked_loop(caplog: pytest.LogCaptureFixture) -> None:
    sampler = LoopLagSampler(warn_ms=50, interval_s=0.05)
    caplog.set_level(logging.WARNING)
    async with sampler:
        await asyncio.sleep(0.06)
        end = time.perf_counter() + 0.3
        while time.perf_counter() < end:  # busy-wait: time.sleep would trip blockbuster
            pass
        await asyncio.sleep(0.1)
    assert sampler.max_ms >= 150
    assert "event loop lag" in caplog.text
    assert sampler._task is None


async def test_cpu_pool_runs_and_reuses_slots() -> None:
    pool = CpuPool(1)
    try:
        first = await pool.run(os.getpid, timeout_s=10)
        second = await pool.run(os.getpid, timeout_s=10)
        assert first == second  # same worker reused
        assert pool.recycles == 0
        assert pool.free_slots == 1
    finally:
        pool.close()


async def test_cpu_pool_timeout_kills_worker_and_spares_other_slot() -> None:
    pool = CpuPool(2)
    try:
        healthy = asyncio.create_task(pool.run(exec, _sleep(1.0), timeout_s=10))
        await asyncio.sleep(0.2)
        stuck = asyncio.create_task(pool.run(exec, *_STUCK, timeout_s=TIMEOUT_S))
        await asyncio.sleep(0.2)
        before = set(_pids(pool))
        with pytest.raises(TimeoutError):
            await stuck
        assert pool.recycles == 1
        assert await healthy is None  # untouched by the other slot's kill
        after = set(_pids(pool))
        (killed,) = before - after  # only the stuck slot's worker was replaced
        assert await _wait_gone(killed)
        assert isinstance(await pool.run(os.getpid, timeout_s=10), int)
    finally:
        pool.close()


async def test_cpu_pool_caller_cancellation_kills_worker() -> None:
    pool = CpuPool(1)
    try:
        task = asyncio.create_task(pool.run(exec, *_STUCK, timeout_s=30))
        await asyncio.sleep(0.5)
        (pid,) = _pids(pool)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert pool.recycles == 1
        assert await _wait_gone(pid)
        assert pool.free_slots == 1
    finally:
        pool.close()


async def test_cpu_pool_queued_timeout_does_not_recycle() -> None:
    pool = CpuPool(1)
    try:
        busy = asyncio.create_task(pool.run(exec, _sleep(1.0), timeout_s=10))
        await asyncio.sleep(0.2)
        with pytest.raises(TimeoutError):
            await pool.run(os.getpid, timeout_s=0.2)  # waits for the only slot
        assert pool.recycles == 0
        assert await busy is None
    finally:
        pool.close()


async def test_cpu_pool_worker_crash_recycles_slot() -> None:
    pool = CpuPool(1)
    try:
        with pytest.raises(BrokenProcessPool):
            await pool.run(exec, *_SELFKILL, timeout_s=10)
        assert pool.recycles == 1
        assert isinstance(await pool.run(os.getpid, timeout_s=10), int)
    finally:
        pool.close()
