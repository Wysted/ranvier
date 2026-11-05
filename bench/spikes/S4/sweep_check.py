"""Checks 2b and 1b: the stalled-jobs sweep vs a re-deferred run, and the periodic dedup gap.

A stalled job is simulated as `doing` with `worker_id` NULL, which `get_stalled_jobs` reports
(queries.sql:39-53); the real SIGKILL path is stalled_check.py.
"""

import asyncio
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

app = make_app(DB)
ran: list[str] = []


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


async def requeue_stalled(jobs: Iterable[Job]) -> list[tuple[str, str]]:
    """Recommended sweep step 1 (before orphan re-defer); one bad job never stops the sweep."""
    outcome = []
    for job in jobs:
        try:
            outcome.append((job.task_kwargs["run_id"], await requeue(job)))
        except exceptions.ConnectorException as exc:  # race: job left `doing` meanwhile
            reason = str(exc.__cause__ or exc.__context__).splitlines()[0]
            outcome.append((job.task_kwargs["run_id"], f"skipped: {reason}"))
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
    drop_db(DB)
    print("sweep_check: OK")


asyncio.run(main())
