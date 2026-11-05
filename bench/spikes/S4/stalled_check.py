"""Check 2: stalled jobs. A worker process is SIGKILLed mid-job; the job is requeued.

`python stalled_check.py` drives the scenario; `python stalled_check.py worker <fast>` is the
worker subprocess (fast=1 makes the long task finish quickly on the second worker).
"""

import asyncio
import signal
import subprocess
import sys
import time

import psycopg
from common import drop_db, dsn, make_app, recreate_db

DB = "s4_stalled"
HEARTBEAT_S = 1.0
STALLED_TIMEOUT_S = 3.0

app = make_app(DB)


def log(label: str, what: str) -> None:
    with psycopg.connect(dsn(DB), autocommit=True) as conn:
        conn.execute("INSERT INTO s4_log (label, what) VALUES (%s, %s)", (label, what))


@app.task(name="long")
async def long(label: str) -> None:
    await asyncio.to_thread(log, label, "start")
    await asyncio.sleep(0.2 if sys.argv[2] == "1" else 600)
    await asyncio.to_thread(log, label, "end")


async def run_worker() -> None:
    async with app.open_async():
        await app.run_worker_async(
            concurrency=2,
            update_heartbeat_interval=HEARTBEAT_S,
            stalled_worker_timeout=STALLED_TIMEOUT_S,
            fetch_job_polling_interval=0.2,
        )


def spawn(fast: str) -> subprocess.Popen[bytes]:
    return subprocess.Popen([sys.executable, __file__, "worker", fast])


async def snapshot(conn: psycopg.AsyncConnection, title: str) -> None:
    cur = await conn.execute(
        "SELECT id, args->>'label', status, lock, attempts, worker_id FROM procrastinate_jobs "
        "ORDER BY id"
    )
    jobs = await cur.fetchall()
    cur = await conn.execute("SELECT id FROM procrastinate_workers ORDER BY id")
    print(f"[{title}] jobs={jobs} workers={[r[0] for r in await cur.fetchall()]}")


async def wait_log(
    conn: psycopg.AsyncConnection, label: str, what: str, timeout: float
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        cur = await conn.execute(
            "SELECT 1 FROM s4_log WHERE label = %s AND what = %s", (label, what)
        )
        if await cur.fetchone():
            return
        await asyncio.sleep(0.1)
    raise TimeoutError(f"{label} {what}")


async def drive() -> None:
    recreate_db(DB)
    async with app.open_async():
        await app.schema_manager.apply_schema_async()
        async with await psycopg.AsyncConnection.connect(
            dsn(DB), autocommit=True
        ) as conn:
            await conn.execute(
                "CREATE TABLE s4_log (label text, what text, at timestamptz DEFAULT now())"
            )
            await app.configure_task(name="long", lock="t1").defer_async(label="J1")
            await app.configure_task(name="long", lock="t1").defer_async(label="J2")

            w1 = spawn("0")
            await wait_log(conn, "J1", "start", 15)
            w1.send_signal(signal.SIGKILL)
            w1.wait()
            print("worker 1 SIGKILLed while J1 runs")
            await snapshot(conn, "after kill")
            stalled = await app.job_manager.get_stalled_jobs(
                seconds_since_heartbeat=STALLED_TIMEOUT_S
            )
            print("get_stalled_jobs right after kill:", [j.id for j in stalled])

            w2 = spawn(
                "1"
            )  # a fresh worker prunes dead workers at startup, nothing more
            await asyncio.sleep(STALLED_TIMEOUT_S + 2)
            await snapshot(conn, f"w2 running, {STALLED_TIMEOUT_S + 2:.0f}s later")
            stalled = list(
                await app.job_manager.get_stalled_jobs(
                    seconds_since_heartbeat=STALLED_TIMEOUT_S
                )
            )
            print(
                "get_stalled_jobs:",
                [(j.id, j.task_kwargs["label"], j.attempts) for j in stalled],
            )
            assert [j.task_kwargs["label"] for j in stalled] == ["J1"]
            for job in stalled:
                await app.job_manager.retry_job(job)
            await snapshot(conn, "after retry_job")
            await wait_log(conn, "J1", "end", 15)
            await wait_log(conn, "J2", "end", 15)
            await snapshot(conn, "done")
            cur = await conn.execute("SELECT label, what FROM s4_log ORDER BY at")
            print("log:", await cur.fetchall())
            w2.send_signal(signal.SIGTERM)
            w2.wait(timeout=10)
            print("worker 2 exit code after SIGTERM:", w2.returncode)
    drop_db(DB)
    print("stalled_check: OK")


if __name__ == "__main__":
    asyncio.run(run_worker() if sys.argv[1:2] == ["worker"] else drive())
