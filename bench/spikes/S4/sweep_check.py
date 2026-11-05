"""Checks 2b, 2c and 1b: the stalled-jobs sweep vs a re-deferred run, sweep exclusion, and the
periodic dedup gap.

A stalled job is simulated as `doing` with `worker_id` NULL, which `get_stalled_jobs` reports
(queries.sql:39-53); the real SIGKILL path is stalled_check.py and 2c below.
`python sweep_check.py` drives; `python sweep_check.py worker` is the worker killed mid-sweep.
"""

import asyncio
import signal
import sys
import time
import uuid
from collections.abc import Iterable

import psycopg
from common import drop_db, dsn, make_app, recreate_db
from procrastinate import exceptions, manager
from procrastinate.jobs import Job, Status
from procrastinate.schema import SchemaManager

DB = "s4_sweep"
PERIODIC_QUEUE = "s4_periodic"  # no worker listens: the probe jobs never run
RUN_NAMESPACE = uuid.UUID(int=0)  # any fixed namespace; T25 picks its own
# Any fixed bigint for pg_try_advisory_xact_lock; T25 names its own.
SWEEP_LOCK_KEY = 4_250_001
SWEEP_QUEUE = "s4_sweep"
HEARTBEAT_S = 0.5
STALLED_S = 2.0  # seconds_since_heartbeat used by the sweep (30 in production)
KILLED_HOLD_S = 600.0  # the killed worker's sweep never ends on its own

app = make_app(DB)
ran: list[str] = []
swept: list[str] = []
hold_s = 0.0  # how long a sweep keeps the advisory lock after requeueing


@app.task(name="execute_run")
async def execute_run(run_id: str) -> None:
    ran.append(run_id)


def execute_job(
    run_id: str,
):  # same shape as design §5: lock=thread_id, queueing_lock=run_id
    return app.configure_task(
        name="execute_run", lock=f"thread-{run_id}", queueing_lock=run_id
    )


async def redefer_orphan(run_id: str) -> str:
    try:
        await execute_job(run_id).defer_async(run_id=run_id)
    except exceptions.AlreadyEnqueued:
        return "AlreadyEnqueued"
    return "deferred"


async def requeue(job: Job) -> str:
    try:
        await app.job_manager.retry_job(job)
    except exceptions.UniqueViolation as exc:
        if exc.constraint_name != manager.QUEUEING_LOCK_CONSTRAINT:
            raise
        # A `todo` job for the same run exists: close the dead one so the thread lock frees.
        await app.job_manager.finish_job(job, status=Status.FAILED, delete_job=False)
        return "finished FAILED (a todo job with its queueing_lock exists)"
    return "retried"


def label(job: Job) -> str:
    return job.task_kwargs.get("run_id", job.task_name)


async def requeue_stalled(jobs: Iterable[Job]) -> list[tuple[str, str]]:
    """Recommended sweep step 1 (before orphan re-defer); one bad job never stops the sweep."""
    outcome = []
    for job in jobs:
        try:
            outcome.append((label(job), await requeue(job)))
        except exceptions.ConnectorException as exc:  # race: job left `doing` meanwhile
            reason = str(exc.__cause__ or exc.__context__).splitlines()[0]
            outcome.append((label(job), f"skipped: {reason}"))
    return outcome


# No lock=, on purpose: a dead sweep left `doing` would block every later one.
@app.task(name="sweep", queue=SWEEP_QUEUE, queueing_lock="sweep")
async def sweep(timestamp: int = 0) -> str:
    """The T25 sweep shape: exclusion by a transaction-scoped advisory lock on a dedicated
    connection, so Postgres releases it when the worker dies and no session state leaks."""
    async with (
        await psycopg.AsyncConnection.connect(dsn(DB)) as lock_conn,
        lock_conn.transaction(),
    ):
        cur = await lock_conn.execute(
            "SELECT pg_try_advisory_xact_lock(%s)", (SWEEP_LOCK_KEY,)
        )
        if not (await cur.fetchone())[0]:
            swept.append("skipped")
            return "skipped"
        stalled = await app.job_manager.get_stalled_jobs(
            seconds_since_heartbeat=STALLED_S
        )
        outcome = f"ran, requeue_stalled={await requeue_stalled(stalled)}"
        await asyncio.sleep(hold_s)
    swept.append(outcome)
    return outcome


async def jobs(conn: psycopg.AsyncConnection, run_id: str) -> list[tuple[int, str]]:
    cur = await conn.execute(
        "SELECT id, status FROM procrastinate_jobs WHERE queueing_lock = %s ORDER BY id",
        (run_id,),
    )
    return await cur.fetchall()


async def stall(conn: psycopg.AsyncConnection, run_id: str) -> None:
    await conn.execute(
        "UPDATE procrastinate_jobs SET status = 'doing', worker_id = NULL "
        "WHERE queueing_lock = %s AND status = 'todo'",
        (run_id,),
    )


async def stalled_sweep(conn: psycopg.AsyncConnection) -> None:
    # A: recommended order. Requeue first; the orphan re-defer then hits AlreadyEnqueued.
    await execute_job("R1").defer_async(run_id="R1")
    await stall(conn, "R1")
    print(
        "A requeue_stalled:",
        await requeue_stalled(await app.job_manager.get_stalled_jobs()),
    )
    print("A orphan re-defer after it:", await redefer_orphan("R1"))
    print("A jobs:", await jobs(conn, "R1"))
    assert [s for _, s in await jobs(conn, "R1")] == ["todo"]

    # B: a todo job already exists (re-deferred earlier, e.g. a lease expired before the
    # heartbeat went stale). Plain retry_job fails and the dead job keeps the thread lock.
    await execute_job("R2").defer_async(run_id="R2")
    await stall(conn, "R2")
    print("B orphan re-defer while doing:", await redefer_orphan("R2"))
    (dead,) = await app.job_manager.get_stalled_jobs()
    try:
        await app.job_manager.retry_job(dead)
        print("B plain retry_job: ok")
    except exceptions.UniqueViolation as exc:
        print("B plain retry_job ->", type(exc).__name__, exc.constraint_name)
    print("B jobs after plain retry:", await jobs(conn, "R2"))
    print("B requeue_stalled:", await requeue_stalled([dead]))
    print("B jobs:", await jobs(conn, "R2"))

    # C: race. The job leaves `doing` between get_stalled_jobs and retry_job.
    await execute_job("R3").defer_async(run_id="R3")
    await stall(conn, "R3")
    stalled = await app.job_manager.get_stalled_jobs()
    await conn.execute(
        "UPDATE procrastinate_jobs SET status = 'succeeded' WHERE queueing_lock = 'R3'"
    )
    print("C requeue_stalled:", await requeue_stalled(stalled))

    worker = asyncio.create_task(
        app.run_worker_async(
            queues=["default"], concurrency=2, wait=True, fetch_job_polling_interval=0.2
        )
    )
    for _ in range(100):
        if {"R1", "R2"} <= set(ran):
            break
        await asyncio.sleep(0.1)
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)
    print("worker ran:", sorted(ran))
    print("B jobs after worker:", await jobs(conn, "R2"))
    assert sorted(ran) == ["R1", "R2"], ran
    assert [s for _, s in await jobs(conn, "R2")] == ["failed", "succeeded"]


def periodic_gap(conn: psycopg.Connection) -> None:
    # procrastinate_defer_periodic_job_v2 dedupes on (task, periodic_id, timestamp) but deletes
    # older rows (schema.sql:193-204): a lagging deferrer re-inserts an old timestamp.
    q = (
        "SELECT procrastinate_defer_periodic_job_v2("
        f"'{PERIODIC_QUEUE}', NULL, NULL, 'fire_cron', 0, 'p', %s::bigint, %s::jsonb)"
    )
    order = [("B", 60), ("B", 120), ("A (lagging)", 60)]
    got = [
        conn.execute(q, (t, psycopg.types.json.Jsonb({"timestamp": t}))).fetchone()[0]
        for _, t in order
    ]
    print("periodic defers", [f"{w}:T{t}" for w, t in order], "-> job ids", got)
    rows = conn.execute(
        "SELECT (args->>'timestamp')::int FROM procrastinate_jobs "
        "WHERE task_name = 'fire_cron' ORDER BY id"
    ).fetchall()
    print("fire_cron jobs by timestamp:", [r[0] for r in rows])
    assert len(rows) == len(order)
    # Recommended T25 guard: run id = uuid5((periodic_id, timestamp)), insert ON CONFLICT.
    conn.execute("CREATE TABLE s4_runs (id uuid PRIMARY KEY)")
    inserted = [
        conn.execute(
            "INSERT INTO s4_runs VALUES (%s) ON CONFLICT DO NOTHING",
            (uuid.uuid5(RUN_NAMESPACE, f"p:{ts}"),),
        ).rowcount
        for (ts,) in rows
    ]
    print("runs inserted per job (uuid5 key + ON CONFLICT DO NOTHING):", inserted)
    assert inserted == [1, 1, 0]


async def lock_held(conn: psycopg.AsyncConnection) -> bool:
    cur = await conn.execute(
        "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted "
        "AND classid = 0 AND objid = %s",
        (SWEEP_LOCK_KEY,),
    )
    return (await cur.fetchone())[0] > 0


async def wait_until(what: str, check, timeout: float = 15) -> None:
    deadline = time.monotonic() + timeout
    while not await check():
        if time.monotonic() > deadline:
            raise TimeoutError(what)
        await asyncio.sleep(0.1)


async def sweep_jobs(conn: psycopg.AsyncConnection) -> list[tuple[int, str]]:
    return await jobs(conn, "sweep")


async def sweep_exclusion(conn: psycopg.AsyncConnection) -> None:
    global hold_s

    # 2c-a: the worker running a sweep is SIGKILLed while the sweep holds the lock.
    async def lock_released() -> bool:
        return not await lock_held(conn)

    await sweep.defer_async()
    killed = await asyncio.create_subprocess_exec(sys.executable, __file__, "worker")
    await wait_until("killed sweep holds lock", lambda: lock_held(conn))
    print(
        "a sweep 1 (worker subprocess) holds the advisory lock:", await lock_held(conn)
    )
    killed.send_signal(signal.SIGKILL)
    await killed.wait()
    await wait_until("lock released", lock_released)
    print("a after SIGKILL: lock held =", await lock_held(conn))
    print("a sweep jobs after kill:", await sweep_jobs(conn))
    print("a next firing (queueing_lock only dedupes todo):", await defer_sweep())
    await asyncio.sleep(
        STALLED_S + HEARTBEAT_S
    )  # let the dead worker's heartbeat go stale
    worker = asyncio.create_task(
        app.run_worker_async(
            queues=[SWEEP_QUEUE],
            concurrency=1,
            update_heartbeat_interval=HEARTBEAT_S,
            fetch_job_polling_interval=0.2,
        )
    )

    async def all_succeeded() -> bool:
        return {s for _, s in await sweep_jobs(conn)} == {"succeeded"}

    await wait_until("sweeps succeeded", all_succeeded)
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)
    print("a sweeps run by the next worker:", swept)
    print("a sweep jobs:", await sweep_jobs(conn))
    assert swept[0] == "ran, requeue_stalled=[('sweep', 'retried')]", swept
    assert len(swept) == 2 and swept[1].startswith("ran"), swept

    # 2c-b: two sweeps overlap (e.g. a duplicate firing, or a stalled-but-alive sweep requeued).
    swept.clear()
    hold_s = 0.5
    print("b two concurrent sweeps:", await asyncio.gather(sweep(), sweep()))
    assert sorted(s.split(",")[0] for s in swept) == ["ran", "skipped"], swept


async def defer_sweep() -> str:
    try:
        await sweep.defer_async()
    except exceptions.AlreadyEnqueued:
        return "AlreadyEnqueued"
    return "deferred"


async def run_killed_worker() -> None:
    global hold_s
    hold_s = KILLED_HOLD_S
    async with app.open_async():
        await app.run_worker_async(
            queues=[SWEEP_QUEUE],
            update_heartbeat_interval=HEARTBEAT_S,
            fetch_job_polling_interval=0.2,
        )


async def main() -> None:
    recreate_db(DB)
    with psycopg.connect(dsn(DB), autocommit=True) as conn:
        conn.execute(SchemaManager.get_schema())  # no params: '%' needs no escaping
        periodic_gap(conn)
    async with (
        app.open_async(),
        await psycopg.AsyncConnection.connect(dsn(DB), autocommit=True) as conn,
    ):
        await stalled_sweep(conn)
        await sweep_exclusion(conn)
    drop_db(DB)
    print("sweep_check: OK")


if __name__ == "__main__":
    asyncio.run(run_killed_worker() if sys.argv[1:2] == ["worker"] else main())
