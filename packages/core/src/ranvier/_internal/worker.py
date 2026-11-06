"""Procrastinate worker side: the `execute_run` task (design section 5).

Worker processes build their own container; nothing is shared with the API process. The sweep,
prune and cron tasks (housekeeping) live on the same app.
"""

import asyncio
import hashlib
import json
import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import aclosing, asynccontextmanager
from datetime import timedelta
from typing import Final

import psycopg
from procrastinate import App, RetryStrategy, exceptions, manager
from procrastinate.jobs import Status
from psycopg import AsyncConnection

from ranvier._internal.config.manifest import CronSpec
from ranvier._internal.db.pool import with_options
from ranvier._internal.limits import graceful_wait_s
from ranvier._internal.queue import EXECUTE_RUN, ProcrastinateRunQueue
from ranvier._internal.runtime.checkpointer import prune_checkpoints
from ranvier._internal.runtime.runner import (
    RunnerResources,
    execute_run,
    start_drain,
)
from ranvier._internal.runtime.runs import ErrorCode, RunError, RunsRepository

logger = logging.getLogger(__name__)

# `max_attempts=N` means N+1 executions and stalled requeues spend the same counter (S4). A job
# waiting for its retry blocks later jobs of the same thread, so the wait stays short. On
# exhaustion the job is failed and the run stays `pending`: T25's pending re-defer picks it up.
# `retry_exceptions` must stay narrow: retrying CancelledError would re-defer the job the
# hand-over already deferred and collide on the queueing_lock. Each ThreadBusy retry logs one INFO
# traceback from Procrastinate; accepted, no filter.
THREAD_BUSY_WAIT_S: Final = 2
THREAD_BUSY_MAX_ATTEMPTS: Final = 30
# Finishing a job sends no notification, so the next job of the same lock is found by polling.
FETCH_POLL_INTERVAL_S: Final = 1.0


class ThreadBusy(Exception):
    """Another run of the thread is running: Procrastinate retries the job with backoff."""


def register_tasks(container: RunnerResources) -> App:
    """Register the worker tasks on the container's queue app and return it."""
    queue = container.queue
    if not isinstance(queue, ProcrastinateRunQueue):
        raise RuntimeError("the worker needs the Procrastinate queue in the container")

    @queue.app.task(
        name=EXECUTE_RUN,
        retry=RetryStrategy(
            max_attempts=THREAD_BUSY_MAX_ATTEMPTS,
            wait=THREAD_BUSY_WAIT_S,
            retry_exceptions={ThreadBusy},
        ),
    )
    async def execute_run_job(run_id: str) -> None:
        # The run row is the run status, never the job status: a sweep can close a job this one
        # still runs, so Procrastinate may log "not in doing or todo status" on finish. Expected.
        try:
            async with aclosing(execute_run(container, uuid.UUID(run_id))) as events:
                async for _ in events:  # no client: just drain
                    pass
        except RunError as exc:
            if exc.code is ErrorCode.THREAD_BUSY:
                raise ThreadBusy(str(exc)) from exc
            raise

    if container.runs is None:
        raise RuntimeError("the worker needs the runs repository in the container")
    _register_housekeeping(container, queue, container.runs)
    return queue.app


# --- housekeeping: sweep, prune, crons (design section 5) ------------------------------------

SWEEP: Final = "ranvier.sweep"
PRUNE: Final = "ranvier.prune"
FIRE_CRON: Final = "ranvier.fire_cron"
SWEEP_PERIODIC_ID: Final = "sweep"
PRUNE_PERIODIC_ID: Final = "prune"
SWEEP_CRON: Final = "* * * * *"  # every minute
PRUNE_CRON: Final = "17 3 * * *"  # daily, off the hour
# Any fixed bigint shared by every sweep (the migrations lock uses another key).
SWEEP_LOCK_KEY: Final = 0x72616E7673776570  # "ranvswep"
PRUNE_LOCK_KEY: Final = 0x72616E767072756E  # "ranvprun"
# A thread idle longer than `ttl_days` + this is no longer `keep_latest`-pruned (it was, once a
# day, while inside the window); it waits for its whole-thread delete.
PRUNE_WINDOW: Final = timedelta(days=7)
# Heartbeat age after which a job counts as stalled (Procrastinate's `stalled_worker_timeout`).
STALLED_HEARTBEAT_S: Final = 30
# A lost host frees the sweep lock in ~25 s: idle 10 + 3 probes x 5 s. The timeout is off so a
# server-side default cannot kill the connection that holds the lock between statements.
LOCK_CONN_OPTIONS: Final = (
    "-c tcp_keepalives_idle=10 -c tcp_keepalives_interval=5 -c tcp_keepalives_count=3 "
    "-c idle_in_transaction_session_timeout=0"
)
LOCK_CONNECT_TIMEOUT_S: Final = 10
# Procrastinate's standalone LISTEN connection and the sweep's and prune's lock connections
# (logged by `open_container(extra_connections=...)`).
WORKER_EXTRA_CONNECTIONS: Final = 3
CRON_NAMESPACE: Final = uuid.UUID("5d0b8c3e-7a64-4f0e-9a52-6c1b3f8e2d17")
SWEEP_SKIPPED_METRIC: Final = "sweep_skipped_consecutive"
PERIODIC_ID_DIGEST_CHARS: Final = 16
# Per-thread housekeeping failures that must not stop the rest: `aprune` raises
# NotImplementedError for delta-channel threads, psycopg errors cover concurrent writes.
_THREAD_ERRORS: Final = (NotImplementedError, psycopg.Error)
# A cron firing is idempotent (uuid5 ids, insert ignores conflicts), so a short retry is safe.
FIRE_CRON_MAX_ATTEMPTS: Final = 3
FIRE_CRON_RETRY_WAIT_S: Final = 5


def cron_periodic_id(flow: str, cron: CronSpec) -> str:
    """Stable id from the cron's content, never its list position (reordering must not re-fire)."""
    content = json.dumps([cron.schedule, cron.input], sort_keys=True, separators=(",", ":"))
    return f"{flow}:{hashlib.sha256(content.encode()).hexdigest()[:PERIODIC_ID_DIGEST_CHARS]}"


async def requeue_stalled(app: App) -> None:
    """Retry jobs whose worker died. Runs BEFORE the orphan re-defer (see `redefer_orphans`)."""
    for job in await app.job_manager.get_stalled_jobs(seconds_since_heartbeat=STALLED_HEARTBEAT_S):
        try:
            await app.job_manager.retry_job(job)
        except exceptions.UniqueViolation as exc:
            try:
                if exc.constraint_name != manager.QUEUEING_LOCK_CONSTRAINT:
                    raise
                # A `todo` job for the same run exists: close the dead one so its thread lock frees.
                await app.job_manager.finish_job(job, status=Status.FAILED, delete_job=False)
            except exceptions.ConnectorException:
                logger.warning("stalled job %s not requeued", job.id, exc_info=True)
        except exceptions.ConnectorException:  # raced: the job left `doing` meanwhile
            logger.warning("stalled job %s not requeued", job.id, exc_info=True)


async def redefer_orphans(
    res: RunnerResources, runs: RunsRepository, queue: ProcrastinateRunQueue
) -> None:
    """Re-defer `running` runs with an expired lease as they are, and stale `pending` ones.

    Never writes `pending`. A job that already exists makes the defer a no-op; a job that loses
    the claim exits.
    """
    expired = await runs.expired(res.settings.runs.max_attempts)
    if expired.exhausted:
        logger.warning("runs out of attempts: %s", expired.exhausted)
    stale = await runs.stale_pending(res.settings.runs.pending_redefer_s)
    for run_id, thread_id in expired.redefer:
        await queue.defer(run_id, thread_id, recovery=True)
    for run_id, thread_id in stale:  # oldest first
        await queue.defer(run_id, thread_id)


async def run_sweep(
    res: RunnerResources, runs: RunsRepository, queue: ProcrastinateRunQueue
) -> bool:
    """One sweep; False if another one holds the lock.

    The lock is `pg_try_advisory_xact_lock` on a dedicated connection held for the whole sweep:
    Postgres frees it when the connection or its host dies and no session state outlives it.
    """
    async with _try_advisory_xact_lock(res, SWEEP_LOCK_KEY) as held:
        if not held:
            return False
        await requeue_stalled(queue.app)
        await redefer_orphans(res, runs, queue)
    return True


@asynccontextmanager
async def _try_advisory_xact_lock(res: RunnerResources, key: int) -> AsyncIterator[bool]:
    """Yields whether the lock was taken; held (on a dedicated connection) until the block ends."""
    url = res.settings.database.url.get_secret_value()
    conn = await AsyncConnection.connect(
        url, options=with_options(url, LOCK_CONN_OPTIONS), connect_timeout=LOCK_CONNECT_TIMEOUT_S
    )
    async with conn, conn.transaction():
        cur = await conn.execute("SELECT pg_try_advisory_xact_lock(%s)", (key,))
        row = await cur.fetchone()
        yield row is not None and bool(row[0])


async def _delete_idle_thread(
    res: RunnerResources, runs: RunsRepository, thread_id: uuid.UUID, delete_after: timedelta
) -> None:
    # The idle list can be minutes old (`keep_latest` of other threads runs in between): a thread
    # reused since then must keep its history and its pending rows.
    if not await runs.thread_is_idle(thread_id, delete_after):
        logger.info("thread %s not deleted: active again", thread_id)
        return
    # Checkpoints first (1y): a failed row delete leaves the thread retryable next time.
    # shortcut: a run created between the check above and the row delete loses its checkpoints
    # (the conditional delete spares its row); closing it needs the per-thread lock (T26 note).
    await res.checkpointer.adelete_thread(str(thread_id))
    if await runs.delete_idle_thread(thread_id, delete_after) == 0:
        logger.warning(
            "thread %s became active during its delete: checkpoints gone, run rows kept", thread_id
        )


async def run_prune(res: RunnerResources, runs: RunsRepository) -> bool:
    """One prune; False if another one is running (same lock pattern as the sweep)."""
    async with _try_advisory_xact_lock(res, PRUNE_LOCK_KEY) as held:
        if not held:
            return False
        await prune_threads(res, runs)
    return True


async def prune_threads(res: RunnerResources, runs: RunsRepository) -> None:
    """`keep_latest` past `threads.ttl_days` idle, whole-thread delete past `delete_after_days`.

    Per thread: one that fails (a delta-channel thread cannot be pruned, a write conflicts) logs
    a warning and the rest go on. Deletions run first so retention never depends on `keep_latest`.
    A thread is pruned only while its idle age is within `PRUNE_WINDOW` past the ttl (one prune a
    day: a few times), then left alone until its delete.
    """
    cfg = res.settings.threads
    delete_after = timedelta(days=cfg.delete_after_days)
    idle = await runs.idle_threads(
        timedelta(days=cfg.ttl_days), window=PRUNE_WINDOW, delete_after=delete_after
    )
    doomed = [t for t, age in idle if age > delete_after]
    stale = [t for t, age in idle if age <= delete_after]
    if await _per_thread(
        doomed, lambda t: _delete_idle_thread(res, runs, t, delete_after), "deleted"
    ):
        await _per_thread(stale, lambda t: prune_checkpoints(res.checkpointer, [t]), "pruned")


def _database_down(exc: Exception) -> bool:
    """Client-side connection failure (refused, dropped, `PoolTimeout`): no SQLSTATE, unlike a
    deadlock or a serialization failure, which are per-thread."""
    return isinstance(exc, psycopg.OperationalError) and exc.sqlstate is None


async def _per_thread(
    threads: list[uuid.UUID], action: Callable[[uuid.UUID], Awaitable[None]], what: str
) -> bool:
    """Run `action` per thread; a thread that fails logs and the rest go on.

    False: the database is unreachable, so one warning and the loop stops (not one per thread).
    """
    for thread_id in threads:
        try:
            await action(thread_id)
        except _THREAD_ERRORS as exc:
            if _database_down(exc):
                logger.warning("database unreachable, prune stopped at thread %s", thread_id)
                return False
            logger.warning("thread %s not %s", thread_id, what, exc_info=True)
    return True


async def fire_cron(
    res: RunnerResources,
    runs: RunsRepository,
    queue: ProcrastinateRunQueue,
    *,
    timestamp: int,
    flow: str,
    input: dict[str, object],
    periodic_id: str,
) -> None:
    """One cron firing: a run on a new thread, idempotent per (periodic_id, timestamp).

    A lagging deferrer can defer an old firing twice (S4): the ids are deterministic and the
    insert ignores conflicts, so no second run exists. The defer happens either way (the first
    attempt may have failed after the insert): a duplicate is dropped by the queueing lock or
    exits at the claim.
    """
    spec = res.settings.flows.get(flow)
    if spec is None:
        logger.error("cron %s: flow %r is not in the manifest", periodic_id, flow)
        return
    key = f"{periodic_id}:{timestamp}"
    run_id = uuid.uuid5(CRON_NAMESPACE, f"run:{key}")
    thread_id = uuid.uuid5(CRON_NAMESPACE, f"thread:{key}")
    created = await runs.create_once(
        run_id=run_id,
        thread_id=thread_id,
        flow=flow,
        flow_version=spec.version,
        input=input,
        metadata={"cron": periodic_id, "scheduled_at": timestamp},
    )
    if not created:  # a retried attempt after a failed defer: defer again (a repeat is a no-op)
        logger.debug("cron firing %s already has its run", key)
    await queue.defer(run_id, thread_id)


def _register_housekeeping(
    res: RunnerResources, queue: ProcrastinateRunQueue, runs: RunsRepository
) -> None:
    app = queue.app
    skipped = 0  # consecutive skipped sweeps: a stuck lock holder shows up as a growing gauge

    # No `lock=`: a dead sweep left `doing` would block every later one. `queueing_lock` drops a
    # duplicate firing while one is `todo`; the advisory lock covers the `doing` case.
    @app.periodic(cron=SWEEP_CRON, periodic_id=SWEEP_PERIODIC_ID, queueing_lock="sweep")
    @app.task(name=SWEEP)
    async def sweep_job(timestamp: int) -> None:
        nonlocal skipped
        skipped = 0 if await run_sweep(res, runs, queue) else skipped + 1
        if skipped:
            logger.debug("sweep skipped: another one holds the lock (%d in a row)", skipped)
        res.observability.metrics.gauge(SWEEP_SKIPPED_METRIC, float(skipped))

    @app.periodic(cron=PRUNE_CRON, periodic_id=PRUNE_PERIODIC_ID, queueing_lock="prune")
    @app.task(name=PRUNE)
    async def prune_job(timestamp: int) -> None:
        if not await run_prune(res, runs):
            logger.info("prune skipped: another one is running")

    @app.task(
        name=FIRE_CRON,
        retry=RetryStrategy(max_attempts=FIRE_CRON_MAX_ATTEMPTS, wait=FIRE_CRON_RETRY_WAIT_S),
    )
    async def fire_cron_job(
        timestamp: int, flow: str, input: dict[str, object], periodic_id: str
    ) -> None:
        await fire_cron(
            res, runs, queue, timestamp=timestamp, flow=flow, input=input, periodic_id=periodic_id
        )

    crons = {  # identical crons collapse into one firing
        cron_periodic_id(name, cron): (name, cron)
        for name, spec in res.settings.flows.items()
        for cron in spec.crons
    }
    for periodic_id, (name, cron) in crons.items():
        # A fresh kwargs dict per cron: Procrastinate writes `timestamp` into it.
        app.periodic(
            cron=cron.schedule,
            periodic_id=periodic_id,
            task_kwargs={"flow": name, "input": dict(cron.input), "periodic_id": periodic_id},
        )(fire_cron_job)


async def run_worker(container: RunnerResources) -> None:
    """Serve jobs, `limits.max_active_runs` at a time (heartbeat defaults), until cancelled.

    Cancelling this task is the graceful stop (the entrypoint does it on SIGTERM/SIGINT;
    Procrastinate's own signal handlers are off). Order: stop fetching, ask the runs in flight
    to drain (each one stops after its superstep, releases its lease and defers itself) and wait
    for the jobs, bounded by the shutdown budget. The caller's `open_container` close awaits every
    cleanup before it closes the pools. Jobs still running at `graceful_wait_s` are aborted
    (they hand over too).
    """
    app = register_tasks(container)
    limits = container.settings.limits
    worker = asyncio.create_task(
        app.run_worker_async(
            concurrency=limits.max_active_runs,
            fetch_job_polling_interval=FETCH_POLL_INTERVAL_S,
            install_signal_handlers=False,
            shutdown_graceful_timeout=graceful_wait_s(limits.shutdown_grace_s),
        ),
        name="procrastinate-worker",
    )
    try:
        await asyncio.shield(worker)
    except asyncio.CancelledError:
        if worker.done():  # the worker ended by itself, this cancel is ours alone
            raise
        worker.cancel()  # Procrastinate stops fetching, waits for its running jobs, then re-raises
        start_drain(container.active_runs, container.shutdown_budget)
        loop = asyncio.get_running_loop()
        # `open_container`'s close settles the runs; here only the worker's own stop is bounded.
        _, pending = await asyncio.wait(
            {worker}, timeout=max(container.shutdown_budget.deadline - loop.time(), 0)
        )
        if pending:
            logger.error("worker did not stop within the shutdown grace")
        elif not worker.cancelled() and (exc := worker.exception()) is not None:
            logger.error("worker failed while stopping", exc_info=exc)
        raise
