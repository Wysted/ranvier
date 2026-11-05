"""T25: sweep (stalled jobs, orphans), prune, cron firings and the Procrastinate schema step."""

import asyncio
import signal
import sys
import types
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, TypedDict

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from procrastinate.jobs import Status
from procrastinate.periodic import PeriodicDeferrer
from psycopg_pool import AsyncConnectionPool, PoolTimeout
from pydantic import SecretStr

from graph_agent._internal.config.manifest import CronSpec, FlowSpec
from graph_agent._internal.config.settings import DatabaseSettings, RunsSettings, Settings
from graph_agent._internal.db.migrate import MigrationError, advisory_lock, apply_migrations
from graph_agent._internal.db.pool import open_app_pool, open_checkpointer_pool
from graph_agent._internal.observability.tracing import LoggingMetrics, Observability
from graph_agent._internal.queue import (
    PROCRASTINATE_MIGRATIONS,
    ProcrastinateRunQueue,
    apply_procrastinate_schema,
    make_app,
)
from graph_agent._internal.runtime.checkpointer import (
    Checkpointer,
    create_checkpointer,
    setup_checkpointer,
)
from graph_agent._internal.runtime.events import RunObserver
from graph_agent._internal.runtime.flows import FlowRegistry
from graph_agent._internal.runtime.runner import ActiveRuns, RunQueue
from graph_agent._internal.runtime.runs import RunsRepository, RunStatus
from graph_agent._internal.worker import (
    FIRE_CRON,
    SWEEP_LOCK_KEY,
    cron_periodic_id,
    fire_cron,
    prune_threads,
    run_sweep,
    run_worker,
)

MODULE = "t25_integration_flows"
FLOW = "steps"
OTHER = "other"
POLL_S = 0.02
WAIT_S = 10.0
SHORT_LEASE_S = 0.05
CRON = CronSpec(schedule="0 3 * * *", input={"source": "daily"})
EXECUTE = "graph_agent.execute_run"
ran: list[str] = []


class State(TypedDict, total=False):
    x: int


async def _step(state: State) -> State:
    ran.append("step")
    return {"x": state.get("x", 0) + 1}


@pytest.fixture
def user_module() -> Iterator[None]:
    mod = types.ModuleType(MODULE)
    g = StateGraph(State)
    for name in ("a", "b", "c"):
        g.add_node(name, _step)
    g.add_edge(START, "a").add_edge("a", "b").add_edge("b", "c").add_edge("c", END)
    mod.steps = g  # type: ignore[attr-defined]
    sys.modules[MODULE] = mod
    ran.clear()
    yield
    del sys.modules[MODULE]


@dataclass
class Budget:
    deadline: float = float("inf")


@dataclass
class Resources:
    settings: Settings
    runs: RunsRepository | None
    flows: FlowRegistry
    checkpointer: Checkpointer
    queue: RunQueue | None
    observers: list[RunObserver] = field(default_factory=list)
    active_runs: ActiveRuns = field(default_factory=ActiveRuns)
    shutdown_budget: Budget = field(default_factory=Budget)
    observability: Observability = field(
        default_factory=lambda: Observability(metrics=LoggingMetrics())
    )


@pytest.fixture
async def pool(clean_database: str) -> AsyncIterator[AsyncConnectionPool]:
    pool = await open_app_pool(clean_database, max_size=6)
    try:
        await apply_migrations(pool)
        async with advisory_lock(pool) as conn:
            await apply_procrastinate_schema(conn)
        yield pool
    finally:
        await pool.close()


@pytest.fixture
def settings(clean_database: str) -> Settings:  # sync: reading the TOML source is blocking I/O
    return Settings(
        _env_file=None,
        database=DatabaseSettings(url=SecretStr(clean_database)),
        runs=RunsSettings(heartbeat_s=0.1, lease_s=2.0, pending_redefer_s=0.1),
        flows={
            FLOW: FlowSpec(target=f"{MODULE}:steps", crons=[CRON]),
            OTHER: FlowSpec(target=f"{MODULE}:steps", crons=[CRON, CRON]),  # identical: collapse
        },
    )


@pytest.fixture
async def res(
    settings: Settings, pool: AsyncConnectionPool, user_module: None
) -> AsyncIterator[Resources]:
    app = make_app(settings)
    async with app.open_async():
        saver = InMemorySaver()
        flows = FlowRegistry(
            {FLOW: settings.flows[FLOW]},
            saver,
            lambda name: object(),  # type: ignore[arg-type,return-value]
        )
        yield Resources(settings, RunsRepository(pool), flows, saver, ProcrastinateRunQueue(app))


def _parts(res: Resources) -> tuple[RunsRepository, ProcrastinateRunQueue]:
    assert res.runs is not None and isinstance(res.queue, ProcrastinateRunQueue)
    return res.runs, res.queue


@asynccontextmanager
async def _running_worker(res: Resources) -> AsyncIterator[None]:
    task = asyncio.create_task(run_worker(res))
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def _new_run(res: Resources, thread: uuid.UUID | None = None) -> tuple[uuid.UUID, uuid.UUID]:
    runs, _ = _parts(res)
    thread = thread or uuid.uuid4()
    run = await runs.create(thread_id=thread, flow=FLOW, flow_version=1, input={})
    return run.run_id, thread


async def _rows(pool: AsyncConnectionPool, query: str, *params: Any) -> list[tuple[Any, ...]]:
    async with pool.connection() as conn:
        cur = await conn.execute(query, params)
        return await cur.fetchall()


async def _status(pool: AsyncConnectionPool, run_id: uuid.UUID) -> str:
    rows = await _rows(pool, "SELECT status FROM graph_agent.runs WHERE run_id = %s", run_id)
    return str(rows[0][0])


async def _until(pool: AsyncConnectionPool, run_id: uuid.UUID, want: RunStatus) -> None:
    async with asyncio.timeout(WAIT_S):
        while await _status(pool, run_id) != want:  # noqa: ASYNC110 (polls a DB row)
            await asyncio.sleep(POLL_S)


async def _execute_jobs(pool: AsyncConnectionPool) -> list[tuple[Any, ...]]:
    return await _rows(
        pool,
        "SELECT status::text, priority FROM graph_agent.procrastinate_jobs "
        "WHERE task_name = %s ORDER BY id",
        EXECUTE,
    )


# --- sweep: orphans ----------------------------------------------------------------------------


async def test_orphan_run_is_redeferred_unchanged_and_runs_after_a_reclaim(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    runs, queue = _parts(res)
    run_id, _ = await _new_run(res)
    claim = await runs.claim(run_id, "dead-worker", SHORT_LEASE_S, max_attempts=3)
    assert claim.token is not None
    await asyncio.sleep(SHORT_LEASE_S * 2)  # the lease expires; nobody heartbeats

    assert await run_sweep(res, runs, queue)

    assert await _status(pool, run_id) == RunStatus.RUNNING  # the sweep never writes pending
    assert [j[0] for j in await _execute_jobs(pool)] == ["todo"]
    assert await run_sweep(res, runs, queue)  # idempotent: queueing_lock dedupes the defer
    assert len(await _execute_jobs(pool)) == 1
    async with _running_worker(res):
        await _until(pool, run_id, RunStatus.SUCCESS)
    assert ran == ["step"] * 3


async def test_stale_pending_run_is_redeferred_oldest_first(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    runs, queue = _parts(res)
    first, _ = await _new_run(res)
    second, _ = await _new_run(res)
    await asyncio.sleep(0.2)  # older than runs.pending_redefer_s
    fresh, _ = await _new_run(res)

    assert await run_sweep(res, runs, queue)

    rows = await _rows(
        pool,
        "SELECT (args->>'run_id')::uuid FROM graph_agent.procrastinate_jobs "
        "WHERE task_name = %s ORDER BY id",
        EXECUTE,
    )
    assert [r[0] for r in rows] == [first, second]  # `fresh` may still be mid-defer
    assert fresh not in [r[0] for r in rows]


# --- sweep: stalled jobs -----------------------------------------------------------------------


async def _stall(pool: AsyncConnectionPool, run_id: uuid.UUID) -> None:
    await _rows(
        pool,
        "UPDATE graph_agent.procrastinate_jobs SET status = 'doing', worker_id = NULL "
        "WHERE queueing_lock = %s AND status = 'todo' RETURNING id",
        str(run_id),
    )


async def test_stalled_job_is_requeued(res: Resources, pool: AsyncConnectionPool) -> None:
    runs, queue = _parts(res)
    run_id, thread = await _new_run(res)
    await queue.defer(run_id, thread)
    await _stall(pool, run_id)
    assert [j[0] for j in await _execute_jobs(pool)] == ["doing"]

    assert await run_sweep(res, runs, queue)

    assert [j[0] for j in await _execute_jobs(pool)] == ["todo"]
    async with _running_worker(res):
        await _until(pool, run_id, RunStatus.SUCCESS)


async def test_stalled_job_beside_a_todo_duplicate_is_closed_and_the_run_runs_once(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    """S4 scenario B: retry_job hits the queueing lock; the dead job must still free the thread."""
    runs, queue = _parts(res)
    run_id, thread = await _new_run(res)
    await queue.defer(run_id, thread)
    await _stall(pool, run_id)
    await queue.defer(run_id, thread)  # accepted: the first job is `doing`
    assert [j[0] for j in await _execute_jobs(pool)] == ["doing", "todo"]

    assert await run_sweep(res, runs, queue)

    assert [j[0] for j in await _execute_jobs(pool)] == [Status.FAILED.value, "todo"]
    async with _running_worker(res):
        await _until(pool, run_id, RunStatus.SUCCESS)
    assert ran == ["step"] * 3


# --- sweep: exclusion --------------------------------------------------------------------------


class _SlowRuns(RunsRepository):
    """Holds the sweep inside its critical section."""

    async def expired(self, max_attempts: int) -> Any:
        await asyncio.sleep(0.5)
        return await super().expired(max_attempts)


async def test_concurrent_sweeps_run_one_and_skip_the_other(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    _, queue = _parts(res)
    slow = _SlowRuns(pool)
    results = await asyncio.gather(run_sweep(res, slow, queue), run_sweep(res, slow, queue))
    assert sorted(results) == [False, True]


_HOLDER = """
import asyncio, sys
from types import SimpleNamespace
from pydantic import SecretStr
from graph_agent._internal.config.settings import DatabaseSettings, Settings
from graph_agent._internal.worker import run_sweep

class Hold:
    async def get_stalled_jobs(self, **kwargs):
        await asyncio.sleep(600)

settings = Settings(_env_file=None, database=DatabaseSettings(url=SecretStr(sys.argv[1])))
queue = SimpleNamespace(app=SimpleNamespace(job_manager=Hold()))
asyncio.run(run_sweep(SimpleNamespace(settings=settings), None, queue))
"""


async def _sweep_lock_held(pool: AsyncConnectionPool) -> bool:
    rows = await _rows(
        pool,
        "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted "
        "AND ((classid::bigint << 32) | objid::bigint) = %s",
        SWEEP_LOCK_KEY,
    )
    return bool(rows[0][0])


async def test_sweep_killed_mid_flight_frees_the_lock_for_the_next_one(
    res: Resources, pool: AsyncConnectionPool, clean_database: str
) -> None:
    runs, queue = _parts(res)
    proc = await asyncio.create_subprocess_exec(sys.executable, "-c", _HOLDER, clean_database)
    try:
        async with asyncio.timeout(WAIT_S):
            while not await _sweep_lock_held(pool):  # noqa: ASYNC110
                await asyncio.sleep(POLL_S)
        assert not await run_sweep(res, runs, queue)  # the dying sweep still owns the lock
        proc.send_signal(signal.SIGKILL)
        await proc.wait()
        async with asyncio.timeout(WAIT_S):
            while await _sweep_lock_held(pool):  # noqa: ASYNC110
                await asyncio.sleep(POLL_S)
        assert await run_sweep(res, runs, queue)
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()


# --- crons -------------------------------------------------------------------------------------

FIRED_AT = 1_700_000_000


async def test_cron_firing_creates_a_run_on_a_new_thread_once(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    runs, queue = _parts(res)
    pid = cron_periodic_id(FLOW, CRON)
    for timestamp in (FIRED_AT, FIRED_AT, FIRED_AT + 60):  # the repeat is a lagging deferrer
        await fire_cron(
            res,
            runs,
            queue,
            timestamp=timestamp,
            flow=FLOW,
            input=dict(CRON.input),
            periodic_id=pid,
        )
    rows = await _rows(
        pool, "SELECT thread_id, input, metadata FROM graph_agent.runs ORDER BY created_at"
    )
    assert len(rows) == 2
    assert rows[0][0] != rows[1][0]
    assert rows[0][1] == {"source": "daily"}
    assert rows[0][2] == {"cron": pid, "scheduled_at": FIRED_AT}
    assert len(await _execute_jobs(pool)) == 2


async def test_cron_retry_after_a_failed_defer_defers_the_existing_run(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    runs, queue = _parts(res)
    pid = cron_periodic_id(FLOW, CRON)

    class _FailingQueue(ProcrastinateRunQueue):
        async def defer(self, *args: Any, **kwargs: Any) -> None:
            raise ConnectionError("defer failed")

    async def fire(through: ProcrastinateRunQueue) -> None:
        await fire_cron(
            res,
            runs,
            through,
            timestamp=FIRED_AT,
            flow=FLOW,
            input=dict(CRON.input),
            periodic_id=pid,
        )

    with pytest.raises(ConnectionError):  # attempt 1: the row is inserted, the defer fails
        await fire(_FailingQueue(queue.app))
    assert await _execute_jobs(pool) == []
    await fire(queue)  # attempt 2: created=False, still deferred
    assert len(await _execute_jobs(pool)) == 1
    assert len(await _rows(pool, "SELECT run_id FROM graph_agent.runs")) == 1


async def test_registered_cron_fires_through_the_periodic_deferrer_and_runs(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    _, queue = _parts(res)
    async with _running_worker(res):  # registers the tasks on the app
        async with asyncio.timeout(WAIT_S):
            while FIRE_CRON not in queue.app.tasks:  # noqa: ASYNC110
                await asyncio.sleep(POLL_S)
        cron_tasks = {
            pid: pt
            for (task, pid), pt in queue.app.periodic_registry.periodic_tasks.items()
            if task == FIRE_CRON
        }
        # Stable ids from content; identical crons of one flow collapse.
        assert set(cron_tasks) == {cron_periodic_id(FLOW, CRON), cron_periodic_id(OTHER, CRON)}
        kwargs = [pt.configure_kwargs["task_kwargs"] for pt in cron_tasks.values()]
        assert kwargs[0] is not kwargs[1]  # Procrastinate writes `timestamp` into it

        deferrer = PeriodicDeferrer(queue.app.periodic_registry)
        await deferrer.defer_jobs([(cron_tasks[cron_periodic_id(FLOW, CRON)], FIRED_AT)])
        async with asyncio.timeout(WAIT_S):
            while (  # noqa: ASYNC110 (polls a DB row)
                not (rows := await _rows(pool, "SELECT run_id, status FROM graph_agent.runs"))
                or rows[0][1] != RunStatus.SUCCESS
            ):
                await asyncio.sleep(POLL_S)
    assert len(rows) == 1


@pytest.fixture
def public_path_settings(clean_database: str) -> Settings:  # sync: reads the TOML source
    url = f"{clean_database}?options=-c%20search_path%3Dpublic"
    return Settings(_env_file=None, database=DatabaseSettings(url=SecretStr(url)))


async def test_make_app_pins_the_schema_whatever_the_url_search_path_says(
    public_path_settings: Settings,
) -> None:
    app = make_app(public_path_settings)
    async with app.open_async():
        rows = await app.connector.execute_query_all_async(query="SHOW search_path")
    assert rows == [{"search_path": "graph_agent"}]


# --- prune -------------------------------------------------------------------------------------


async def _checkpoints(pool: AsyncConnectionPool, thread: uuid.UUID) -> int:
    rows = await _rows(
        pool, "SELECT count(*) FROM graph_agent.checkpoints WHERE thread_id = %s", str(thread)
    )
    return int(rows[0][0])


async def _age(pool: AsyncConnectionPool, thread: uuid.UUID, days: int) -> None:
    await _rows(
        pool,
        "UPDATE graph_agent.runs SET finished_at = now() - make_interval(days => %s), "
        "status = 'success' WHERE thread_id = %s RETURNING run_id",
        days,
        thread,
    )


async def test_prune_keeps_the_latest_checkpoint_then_deletes_the_thread(
    res: Resources, pool: AsyncConnectionPool, clean_database: str
) -> None:
    runs, _ = _parts(res)
    cfg = res.settings.threads
    async with await open_checkpointer_pool(clean_database, 3) as cp_pool:
        await setup_checkpointer(cp_pool)
        saver = create_checkpointer(cp_pool)
        res.checkpointer = saver
        graph = StateGraph(State)
        graph.add_node("a", _step).add_node("b", _step).add_edge(START, "a")
        graph.add_edge("a", "b").add_edge("b", END)
        compiled = graph.compile(checkpointer=saver)
        threads = {name: uuid.uuid4() for name in ("recent", "stale", "doomed")}
        for thread in threads.values():
            await _new_run(res, thread)
            await compiled.ainvoke({"x": 0}, {"configurable": {"thread_id": str(thread)}})
        full = await _checkpoints(pool, threads["recent"])
        assert full > 1
        await _age(pool, threads["recent"], cfg.ttl_days - 1)
        await _age(pool, threads["stale"], cfg.ttl_days + 1)
        await _age(pool, threads["doomed"], cfg.delete_after_days + 1)

        await prune_threads(res, runs)

        assert await _checkpoints(pool, threads["recent"]) == full
        assert await _checkpoints(pool, threads["stale"]) == 1
        assert await _checkpoints(pool, threads["doomed"]) == 0
        remaining = await _rows(pool, "SELECT DISTINCT thread_id FROM graph_agent.runs")
        assert {r[0] for r in remaining} == {threads["recent"], threads["stale"]}
        # The pruned thread still resumes: its latest checkpoint is intact.
        state = await compiled.aget_state({"configurable": {"thread_id": str(threads["stale"])}})
        assert state.values == {"x": 2}


@pytest.fixture
async def history_graph(res: Resources, clean_database: str) -> AsyncIterator[Any]:
    """A compiled graph on a real Postgres saver (installed on `res`)."""
    async with await open_checkpointer_pool(clean_database, 3) as cp_pool:
        await setup_checkpointer(cp_pool)
        res.checkpointer = create_checkpointer(cp_pool)
        graph = StateGraph(State)
        graph.add_node("a", _step).add_node("b", _step).add_edge(START, "a")
        graph.add_edge("a", "b").add_edge("b", END)
        yield graph.compile(checkpointer=res.checkpointer)


async def _thread_with_history(
    res: Resources, graph: Any, pool: AsyncConnectionPool, days: int
) -> uuid.UUID:
    thread = uuid.uuid4()
    await _new_run(res, thread)
    await graph.ainvoke({"x": 0}, {"configurable": {"thread_id": str(thread)}})
    await _age(pool, thread, days)
    return thread


class _ReusedRuns(RunsRepository):
    """A client reuses every idle thread while `keep_latest` is still running (minutes in prod)."""

    async def idle_threads(self, idle_for: timedelta) -> Any:
        found = await super().idle_threads(idle_for)
        for thread_id, _ in found:
            await self.create(thread_id=thread_id, flow=FLOW, flow_version=1, input={})
        return found


async def test_prune_spares_a_thread_reused_after_the_idle_list_was_built(
    res: Resources, pool: AsyncConnectionPool, history_graph: Any
) -> None:
    doomed = await _thread_with_history(
        res, history_graph, pool, res.settings.threads.delete_after_days + 1
    )
    full = await _checkpoints(pool, doomed)

    await prune_threads(res, _ReusedRuns(pool))

    assert await _checkpoints(pool, doomed) == full
    rows = await _rows(
        pool, "SELECT status FROM graph_agent.runs WHERE thread_id = %s ORDER BY created_at", doomed
    )
    assert [r[0] for r in rows] == ["success", "pending"]  # the 202 the client got stays valid


class _RacedRuns(RunsRepository):
    """The idle re-check passes, then a client reuses the thread before the row delete runs."""

    async def thread_is_idle(self, thread_id: uuid.UUID, idle_for: timedelta) -> bool:
        await self.create(thread_id=thread_id, flow=FLOW, flow_version=1, input={})
        return True


async def test_conditional_delete_keeps_the_rows_of_a_thread_reused_after_the_recheck(
    res: Resources, pool: AsyncConnectionPool, history_graph: Any, caplog: pytest.LogCaptureFixture
) -> None:
    doomed = await _thread_with_history(
        res, history_graph, pool, res.settings.threads.delete_after_days + 1
    )

    with caplog.at_level("WARNING", logger="graph_agent._internal.worker"):
        await prune_threads(res, _RacedRuns(pool))

    assert await _checkpoints(pool, doomed) == 0  # the shortcut window (see _delete_idle_thread)
    rows = await _rows(
        pool, "SELECT status FROM graph_agent.runs WHERE thread_id = %s ORDER BY created_at", doomed
    )
    assert [r[0] for r in rows] == ["success", "pending"]  # the DELETE itself spared the thread
    assert "became active during its delete" in caplog.text


async def test_prune_stops_at_the_first_thread_when_the_database_is_down(
    res: Resources,
    pool: AsyncConnectionPool,
    history_graph: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    runs, _ = _parts(res)
    delete_after = res.settings.threads.delete_after_days + 1
    for _ in range(3):
        await _thread_with_history(res, history_graph, pool, delete_after)
    calls = 0

    async def adelete_thread(thread_id: str) -> None:
        nonlocal calls
        calls += 1
        raise PoolTimeout("couldn't get a connection")

    monkeypatch.setattr(res.checkpointer, "adelete_thread", adelete_thread)

    with caplog.at_level("WARNING", logger="graph_agent._internal.worker"):
        await prune_threads(res, runs)

    assert calls == 1
    assert caplog.text.count("database unreachable") == 1


async def test_prune_goes_on_after_a_thread_that_cannot_be_pruned(
    res: Resources,
    pool: AsyncConnectionPool,
    history_graph: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runs, _ = _parts(res)
    cfg = res.settings.threads
    broken = await _thread_with_history(res, history_graph, pool, cfg.ttl_days + 1)
    stale = await _thread_with_history(res, history_graph, pool, cfg.ttl_days + 1)
    doomed = await _thread_with_history(res, history_graph, pool, cfg.delete_after_days + 1)
    full = await _checkpoints(pool, broken)
    real = res.checkpointer.aprune

    async def aprune(thread_ids: Any, **kwargs: Any) -> None:
        if str(broken) in thread_ids:
            raise NotImplementedError("delta channels")  # what upstream raises for such threads
        await real(thread_ids, **kwargs)

    monkeypatch.setattr(res.checkpointer, "aprune", aprune)

    await prune_threads(res, runs)

    assert await _checkpoints(pool, broken) == full
    assert await _checkpoints(pool, stale) == 1
    assert await _checkpoints(pool, doomed) == 0
    remaining = await _rows(pool, "SELECT DISTINCT thread_id FROM graph_agent.runs")
    assert {r[0] for r in remaining} == {broken, stale}


async def test_prune_ignores_a_thread_with_a_running_run(
    res: Resources, pool: AsyncConnectionPool
) -> None:
    runs, _ = _parts(res)
    run_id, _ = await _new_run(res)
    claim = await runs.claim(run_id, "w", res.settings.runs.lease_s, max_attempts=3)
    assert claim.token is not None
    await _rows(
        pool,
        "UPDATE graph_agent.runs SET created_at = now() - interval '400 days' "
        "WHERE run_id = %s RETURNING run_id",
        run_id,
    )
    assert await runs.idle_threads(timedelta(days=1)) == []


# --- migrate: Procrastinate schema -------------------------------------------------------------


async def test_procrastinate_schema_is_recorded_once_in_our_schema(
    pool: AsyncConnectionPool,
) -> None:
    tables = await _rows(
        pool,
        "SELECT table_schema FROM information_schema.tables "
        "WHERE table_name = 'procrastinate_jobs'",
    )
    assert tables == [("graph_agent",)]  # pinned: not in public, whatever the role is called
    async with advisory_lock(pool) as conn:
        assert await apply_procrastinate_schema(conn) == []
    recorded = await _rows(pool, f"SELECT count(*) FROM {PROCRASTINATE_MIGRATIONS}")
    assert recorded[0][0] > 0


async def test_procrastinate_schema_without_records_fails_loudly(
    pool: AsyncConnectionPool,
) -> None:
    await _rows(pool, f"DELETE FROM {PROCRASTINATE_MIGRATIONS} RETURNING name")
    async with advisory_lock(pool) as conn:
        with pytest.raises(MigrationError, match="is empty"):
            await apply_procrastinate_schema(conn)
