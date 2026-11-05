"""Check 4: `lock=` semantics. Jobs are deferred BY NAME (as queue.py will), one worker, concurrency 4.

Scenario 1: same lock -> sequential, different lock -> parallel.
Scenario 2: the first job of a lock fails and is retried (wait 2 s): does a later job on the same
lock run in between? Then the first job exhausts its attempts: does the later job run after?
Scenario 3: `queueing_lock` dedupes only while the job is `todo`.
"""

import asyncio
import time

from common import drop_db, make_app, recreate_db
from procrastinate import RetryStrategy, exceptions

DB = "s4_lock"
CONCURRENCY = 4
DUR_S = 1.0
RETRY_WAIT_S = 2

t0 = time.monotonic()
events: list[tuple[float, str, str]] = []
attempts: dict[str, int] = {}


def log(label: str, what: str) -> None:
    events.append((round(time.monotonic() - t0, 2), label, what))


async def main() -> None:
    recreate_db(DB)
    app = make_app(DB)

    @app.task(name="sleeper")
    async def sleeper(label: str) -> None:
        log(label, "start")
        await asyncio.sleep(DUR_S)
        log(label, "end")

    @app.task(name="flaky", retry=RetryStrategy(max_attempts=2, wait=RETRY_WAIT_S))
    async def flaky(label: str, fail_times: int) -> None:
        attempts[label] = attempts.get(label, 0) + 1
        log(label, f"start attempt {attempts[label]}")
        await asyncio.sleep(0.2)
        if attempts[label] <= fail_times:
            log(label, f"fail attempt {attempts[label]}")
            raise RuntimeError("boom")
        log(label, "end")

    async with app.open_async():
        await app.schema_manager.apply_schema_async()

        async def defer(name: str, lock: str, **kw: object) -> int:
            return await app.configure_task(name=name, lock=lock).defer_async(**kw)

        # Scenario 1
        await defer("sleeper", "a", label="A1")
        await defer("sleeper", "a", label="A2")
        await defer("sleeper", "b", label="B1")
        # Scenario 2: C1 fails once then succeeds; E1 fails on every attempt (2 attempts)
        await defer("flaky", "c", label="C1", fail_times=1)
        await defer("sleeper", "c", label="C2")
        await defer("flaky", "e", label="E1", fail_times=99)
        await defer("sleeper", "e", label="E2")

        # Scenario 3: queueing_lock
        q = app.configure_task(name="sleeper", lock="q", queueing_lock="run-1")
        await q.defer_async(label="Q1")
        try:
            await q.defer_async(label="Q1-dup")
            print("queueing_lock while todo: second defer ACCEPTED")
        except exceptions.AlreadyEnqueued:
            print("queueing_lock while todo: second defer -> AlreadyEnqueued")

        worker = asyncio.create_task(
            app.run_worker_async(
                concurrency=CONCURRENCY, wait=True, fetch_job_polling_interval=0.2
            )
        )
        while not any(lbl == "Q1" and w == "start" for _, lbl, w in events):
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.1)  # Q1 is `doing` now
        try:
            await q.defer_async(label="Q1-redefer")
            print("queueing_lock while doing: second defer ACCEPTED (waits on lock q)")
        except exceptions.AlreadyEnqueued:
            print("queueing_lock while doing: second defer -> AlreadyEnqueued")

        expected_ends = {"A1", "A2", "B1", "C1", "C2", "E2", "Q1", "Q1-redefer"}
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            ended = {lbl for _, lbl, w in events if w == "end"}
            if expected_ends <= ended:
                break
            await asyncio.sleep(0.1)
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        async with app.connector.pool.connection() as conn:
            cur = await conn.execute(
                "SELECT args->>'label', status, attempts FROM procrastinate_jobs ORDER BY id"
            )
            print("final jobs:", await cur.fetchall())

    for e in events:
        print(e)
    at = {(lbl, w): t for t, lbl, w in events}
    # Scenario 1
    assert at[("A2", "start")] >= at[("A1", "end")], "same lock overlapped"
    assert at[("B1", "start")] < at[("A1", "end")], "different locks did not overlap"
    # Scenario 2: C2 waits for C1's retry (head-of-line), even while C1 sits in `todo`
    assert at[("C2", "start")] >= at[("C1", "end")], (
        "C2 ran between C1's failure and retry"
    )
    assert at[("E2", "start")] >= at[("E1", "fail attempt 2")], (
        "E2 ran before E1 was exhausted"
    )
    drop_db(DB)
    print("lock_check: OK")


asyncio.run(main())
