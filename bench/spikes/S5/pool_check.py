"""S5 spike: kill a stuck ProcessPoolExecutor worker and recycle the pool (3.13 and 3.14).

Run: bench/spikes/S5/run.sh (or `uv run --no-project --python 3.14 bench/spikes/S5/pool_check.py`).
Stdlib only. Throwaway. Reads `pool._processes` only to observe pids, except in the 3.13 backport.
"""

import asyncio
import multiprocessing as mp
import os
import signal
import sys
import threading
import time
from concurrent.futures import BrokenExecutor, ProcessPoolExecutor

WORKERS = 2
TIMEOUT_S = 1.0
LEAK_ROUNDS = 5
REAP_WAIT_S = 5.0


def stuck() -> None:
    """CPU-bound task that never ends and ignores SIGTERM (worst case)."""
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    while True:
        pass


def slow(seconds: float) -> int:
    time.sleep(seconds)
    return os.getpid()


def ids() -> tuple[int, int]:
    return os.getpid(), os.getppid()


def kill_pool(pool: ProcessPoolExecutor) -> str:
    """Candidate `recycle()` primitive: same behavior on 3.13 and 3.14."""
    kill = getattr(pool, "kill_workers", None)
    if kill is not None:
        kill()
        return "kill_workers"
    # 3.13 backport of 3.14 `_force_shutdown(_KILL)`; `_processes` (pid -> Process) is private.
    procs = dict(pool._processes or {})
    pool.shutdown(wait=False, cancel_futures=True)
    for proc in procs.values():
        try:
            proc.kill()
        except (ValueError, ProcessLookupError):
            continue
    return "backport(_processes)"


def proc_state(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/status") as fh:
            for line in fh:
                if line.startswith("State:"):
                    return line.split(":", 1)[1].strip()
    except FileNotFoundError:
        return "gone"
    return "unknown"


async def wait_gone(pids: list[int]) -> tuple[float, dict[int, str]]:
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < REAP_WAIT_S:
        states = {p: proc_state(p) for p in pids}
        if all(s == "gone" for s in states.values()):
            return time.perf_counter() - t0, states
        await asyncio.sleep(0.01)
    return time.perf_counter() - t0, {p: proc_state(p) for p in pids}


def manager_threads() -> int:
    return sum(
        type(t).__name__ == "_ExecutorManagerThread" for t in threading.enumerate()
    )


async def outcome(fut) -> str:
    try:
        res = await asyncio.wait_for(asyncio.wrap_future(fut), REAP_WAIT_S)
    except asyncio.CancelledError:
        return "cancelled"
    except BrokenExecutor as exc:
        return f"{type(exc).__name__}({exc})"
    except TimeoutError:
        return "still running after 5 s (orphan!)"
    return f"result={res}"


def new_pool(ctx) -> ProcessPoolExecutor:
    return ProcessPoolExecutor(max_workers=WORKERS, mp_context=ctx)


async def main() -> None:
    loop = asyncio.get_running_loop()
    ctx = mp.get_context("forkserver")
    print(
        f"python={sys.version.split()[0]} default_start_method={mp.get_start_method()} "
        f"has_kill_workers={hasattr(ProcessPoolExecutor, 'kill_workers')}"
    )

    # --- Check 1: one timed-out task, one running sibling, two pending ---
    pool = new_pool(ctx)
    fut_a = loop.run_in_executor(pool, stuck)
    fut_b = pool.submit(slow, 3.0)  # running on worker 2 when A times out
    fut_c = pool.submit(slow, 0.1)  # pending
    fut_d = pool.submit(slow, 0.1)  # pending
    try:
        await asyncio.wait_for(fut_a, TIMEOUT_S)
    except TimeoutError:
        print(
            f"[1] A timed out after {TIMEOUT_S}s; asyncio future cancelled={fut_a.cancelled()}"
        )
    old_pids = list(pool._processes)
    print(
        f"[1] before recycle: workers={old_pids} states={[proc_state(p) for p in old_pids]} "
        f"B.running={fut_b.running()} C.running={fut_c.running()} D.running={fut_d.running()}"
    )
    t0 = time.perf_counter()
    how = kill_pool(pool)
    kill_ms = (time.perf_counter() - t0) * 1000
    pool = new_pool(ctx)
    print(f"[1] recycle via {how}: call took {kill_ms:.2f} ms")
    for name, fut in (("B running", fut_b), ("C pending", fut_c), ("D pending", fut_d)):
        print(f"[1] {name}: {await outcome(fut)}")
    gone_s, states = await wait_gone(old_pids)
    print(f"[1] old workers after {gone_s * 1000:.0f} ms: {states}")
    got = await asyncio.gather(
        *(loop.run_in_executor(pool, slow, 0.2) for _ in range(WORKERS))
    )
    print(
        f"[1] new pool works: pids={sorted(set(got))} reused_old={bool(set(got) & set(old_pids))}"
    )
    probe = new_pool(ctx)
    probe.submit(ids).result()
    kill_pool(probe)
    try:
        probe.submit(ids)
        print("[1] submit to a killed pool: accepted (unexpected)")
    except (RuntimeError, BrokenExecutor) as exc:
        print(f"[1] submit to a killed pool: {type(exc).__name__}: {exc}")

    # --- Check 2: repeated timeouts leak neither workers nor manager threads ---
    forkserver_pid = (await loop.run_in_executor(pool, ids))[1]
    killed: list[int] = []
    for _ in range(LEAK_ROUNDS):
        fut = loop.run_in_executor(pool, stuck)
        try:
            await asyncio.wait_for(fut, 0.3)
        except TimeoutError:
            pass
        killed += list(pool._processes)
        kill_pool(pool)
        pool = new_pool(ctx)
    await loop.run_in_executor(pool, ids)
    _, states = await wait_gone(killed)
    await asyncio.sleep(0.2)
    alive = [p for p, s in states.items() if s != "gone"]
    print(
        f"[2] {LEAK_ROUNDS} recycles: killed={len(killed)} still_present={alive} "
        f"manager_threads={manager_threads()} (expect 1) forkserver_pid={forkserver_pid} "
        f"mp.active_children()={len(mp.active_children())} (new pool has 1 worker)"
    )

    # --- Check 3: terminate_workers (SIGTERM) vs a SIGTERM-ignoring worker (3.14 only) ---
    if hasattr(ProcessPoolExecutor, "terminate_workers"):
        probe = new_pool(ctx)
        fut = probe.submit(stuck)
        await asyncio.sleep(0.5)
        pids = list(probe._processes)
        probe.terminate_workers()
        await asyncio.sleep(1.0)
        print(
            f"[3] terminate_workers, SIGTERM-ignoring worker after 1 s: {[proc_state(p) for p in pids]}"
        )
        # pool is already shut down; kill the survivors by pid to clean up.
        for p in pids:
            if proc_state(p) != "gone":
                os.kill(p, signal.SIGKILL)
        print(
            f"[3] after SIGKILL: {(await wait_gone(pids))[1]} future={await outcome(fut)}"
        )

    pool.shutdown(wait=True)
    print("done: interpreter exit follows (run.sh times it)")


if __name__ == "__main__":
    asyncio.run(main())
