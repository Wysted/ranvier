"""Procrastinate app factory and the `RunQueue` implementation (design section 5, contract C4).

`queue` sits below `runtime`: it defers the worker task BY NAME and never imports `worker` or the
runtime. Task names live here so both sides agree on them.
"""

import asyncio
import logging
import uuid
from pathlib import Path
from typing import Any, Final

from procrastinate import App, PsycopgConnector
from procrastinate.exceptions import AlreadyEnqueued
from procrastinate.schema import SchemaManager
from psycopg import AsyncConnection

from ranvier._internal.config.settings import Settings
from ranvier._internal.db.migrate import SCHEMA, MigrationError
from ranvier._internal.db.pool import MIN_POOL_SIZE, PIN_SEARCH_PATH, with_options

logger = logging.getLogger(__name__)

EXECUTE_RUN: Final = "ranvier.execute_run"
# Recovery defers (a `running` run handed over or re-deferred) outrank normal ones: fetch skips a
# job while an earlier todo job of its lock has the same priority, even one waiting for a retry
# (S4), so a recovery job would sit behind every queued run of its thread.
NORMAL_PRIORITY: Final = 0  # Procrastinate's default
RECOVERY_PRIORITY: Final = 1


# The API process only defers jobs (one INSERT each): one connection is enough.
API_QUEUE_POOL_MAX: Final = 1


def make_app(settings: Settings, pool_max: int | None = None) -> App:
    """A Procrastinate app with its own pool (`pool_max` or `database.procrastinate_pool_max`).

    The caller opens it (`async with app.open_async()`). Workers register their tasks on the
    same app (`worker.register_tasks`); the API only defers by name.
    """
    db = settings.database
    connector = PsycopgConnector(
        conninfo=db.url.get_secret_value(),
        min_size=MIN_POOL_SIZE,
        max_size=pool_max or db.procrastinate_pool_max,
        # Pinned like the saver's pool: the Procrastinate tables live in our schema, whatever the
        # role is called. The LISTEN connection is built from the same kwargs.
        kwargs={"options": with_options(db.url.get_secret_value(), PIN_SEARCH_PATH)},
    )
    return App(connector=connector)


class ProcrastinateRunQueue:
    """`runtime.runner.RunQueue` over Procrastinate."""

    def __init__(self, app: App) -> None:
        self.app = app

    async def defer(
        self, run_id: uuid.UUID, thread_id: uuid.UUID, *, recovery: bool = False
    ) -> None:
        # lock: one job of a thread at a time, FIFO within one priority. queueing_lock: at
        # most one `todo` job per run; a job deferred while the run's job is `doing` is accepted
        # and exits at the claim.
        job = self.app.configure_task(
            name=EXECUTE_RUN,
            lock=str(thread_id),
            queueing_lock=str(run_id),
            priority=RECOVERY_PRIORITY if recovery else NORMAL_PRIORITY,
        )
        try:
            await job.defer_async(run_id=str(run_id))
        except AlreadyEnqueued:
            logger.debug("run %s already has a queued job", run_id)


PROCRASTINATE_MIGRATIONS: Final = f"{SCHEMA}.procrastinate_migrations"
_JOBS_TABLE: Final = f"{SCHEMA}.procrastinate_jobs"


def _read_schema_files() -> tuple[str, list[tuple[str, str]]]:
    """`schema.sql` and the shipped migration files by name (sync file I/O)."""
    files = sorted(Path(SchemaManager.get_migrations_path()).glob("*.sql"))
    return SchemaManager.get_schema(), [(f.name, f.read_text(encoding="utf-8")) for f in files]


async def _run_script(conn: AsyncConnection[tuple[Any, ...]], sql: str, record: list[str]) -> None:
    async with conn.transaction():
        # Transaction-scoped, so the lock connection keeps no session state (rule 1d). No params:
        # `%` in the Procrastinate SQL needs no escaping.
        await conn.execute(f"SET LOCAL search_path = {SCHEMA}")
        await conn.execute(sql)
        for name in record:
            await conn.execute(
                f"INSERT INTO {PROCRASTINATE_MIGRATIONS} (name) VALUES (%s)", (name,)
            )


async def apply_procrastinate_schema(conn: AsyncConnection[tuple[Any, ...]]) -> list[str]:
    """Install or upgrade the Procrastinate schema in `ranvier`; return the files recorded.

    Procrastinate tracks nothing itself. A fresh database gets `schema.sql` plus every shipped
    migration name recorded as applied; an existing one gets the shipped files not yet recorded,
    in name order, one transaction each. The caller holds `advisory_lock` (autocommit `conn`) and
    has applied the SQL migrations (they create the record table). Upgrading Procrastinate needs
    the API and workers stopped first (S4).
    """
    schema_sql, migrations = await asyncio.to_thread(_read_schema_files)
    cur = await conn.execute("SELECT to_regclass(%s)", (_JOBS_TABLE,))
    row = await cur.fetchone()
    names = [name for name, _ in migrations]
    if row is not None and row[0] is None:
        await _run_script(conn, schema_sql, names)
        return names
    cur = await conn.execute(f"SELECT name FROM {PROCRASTINATE_MIGRATIONS}")
    applied = {r[0] for r in await cur.fetchall()}
    if not applied:  # schema without records: which files it holds is unknown
        raise MigrationError(f"{_JOBS_TABLE} exists but {PROCRASTINATE_MIGRATIONS} is empty")
    if unknown := sorted(applied - set(names)):
        raise MigrationError(f"recorded Procrastinate migrations not shipped any more: {unknown}")
    done: list[str] = []
    for name, sql in migrations:
        if name not in applied:
            await _run_script(conn, sql, [name])
            logger.info("applied Procrastinate migration %s", name)
            done.append(name)
    return done
