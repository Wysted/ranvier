"""Check 1: one periodic task per cron, registered from data at runtime, with kwargs.

Two workers share the registry (as two worker processes would): the DB dedupes each firing.
Run with TZ set to a non-UTC zone to show cron evaluation is UTC.
"""

import asyncio
import datetime as dt
import os
import time

import croniter
from common import drop_db, make_app, recreate_db
from procrastinate import periodic

DB = "s4_periodic"
RUN_FOR_S = 7.5
# Data as it would come from the manifest. The optional seconds field is the 6th (LAST) column
# (croniter): "*/2 * * * * *" would mean "every second of even minutes". Seconds crons are only
# here to keep the spike fast.
CRONS = [
    {"flow": "summarize", "schedule": "* * * * * */2", "input": {"source": "a"}},
    {"flow": "summarize", "schedule": "* * * * * */3", "input": {"source": "b"}},
    {"flow": "report", "schedule": "0 3 * * *", "input": {}},  # 5-field daily, 03:00 UTC
    # 5-field, every minute: shows the startup catch-up of the latest firing (< 10 min ago).
    {"flow": "report", "schedule": "* * * * *", "input": {"catch_up": True}},
]

fired: list[tuple[str, int, dict, float]] = []


async def main() -> None:
    print(f"TZ={os.environ.get('TZ')} time.tzname={time.tzname}")
    recreate_db(DB)
    app = make_app(DB)

    @app.task(name="fire_cron")
    async def fire_cron(timestamp: int, periodic_id: str, flow: str, input: dict) -> None:
        fired.append((periodic_id, timestamp, input, time.time()))

    for i, cron in enumerate(CRONS):
        periodic_id = f"{cron['flow']}:{i}"
        kwargs = {"periodic_id": periodic_id, "flow": cron["flow"], "input": cron["input"]}
        app.periodic(cron=cron["schedule"], periodic_id=periodic_id, task_kwargs=kwargs)(fire_cron)

    async with app.open_async():
        await app.schema_manager.apply_schema_async()
        started = time.time()
        workers = [
            asyncio.create_task(app.run_worker_async(name=f"w{n}", concurrency=2, wait=True))
            for n in range(2)
        ]
        await asyncio.sleep(RUN_FOR_S)
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        async with app.connector.pool.connection() as conn:
            cur = await conn.execute(
                "SELECT args->>'periodic_id', (args->>'timestamp')::bigint, count(*) "
                "FROM procrastinate_jobs GROUP BY 1, 2 HAVING count(*) > 1"
            )
            print("duplicate (periodic_id, timestamp) jobs in DB:", await cur.fetchall())
            cur = await conn.execute("SELECT count(*) FROM procrastinate_jobs")
            print("jobs in DB:", (await cur.fetchone())[0])

    for pid, ts, inp, at in sorted(fired):
        utc = dt.datetime.fromtimestamp(ts, dt.UTC).isoformat()
        print(f"fired {pid} ts={ts} ({utc}) input={inp} lag={at - ts:.2f}s")
    by_id: dict[str, list[int]] = {}
    for pid, ts, _, _ in fired:
        by_id.setdefault(pid, []).append(ts)
    assert all(ts % 2 == 0 for ts in by_id["summarize:0"]), by_id
    assert all(ts % 3 == 0 for ts in by_id["summarize:1"]), by_id
    assert len(fired) == len({(p, t) for p, t, _, _ in fired}), "a firing ran twice"
    # "0 3 * * *" fires only through the startup catch-up, iff 03:00 UTC was < MAX_DELAY ago.
    last_3am = croniter.croniter("0 3 * * *").get_prev(ret_type=float, start_time=started)
    expected = [int(last_3am)] if started - last_3am <= periodic.MAX_DELAY else []
    print(f"report:2 expected={expected} got={by_id.get('report:2', [])}")
    assert by_id.get("report:2", []) == expected, by_id
    assert by_id["report:3"] and all(ts % 60 == 0 for ts in by_id["report:3"]), by_id
    print(
        "registry kwargs after firing (mutated in place):",
        app.periodic_registry.periodic_tasks[("fire_cron", "summarize:0")].configure_kwargs,
    )

    # 5-field cron as procrastinate evaluates it (float epoch start): next 03:00 is UTC, not local.
    start = dt.datetime(2026, 11, 4, 12, 0, tzinfo=dt.UTC).timestamp()
    nxt = croniter.croniter("0 3 * * *").get_next(ret_type=float, start_time=start)
    nxt_utc = dt.datetime.fromtimestamp(nxt, dt.UTC)
    print("next '0 3 * * *' after 2026-11-04T12:00Z:", nxt_utc.isoformat())
    assert nxt_utc == dt.datetime(2026, 11, 5, 3, 0, tzinfo=dt.UTC)
    drop_db(DB)
    print("periodic_check: OK")


asyncio.run(main())
