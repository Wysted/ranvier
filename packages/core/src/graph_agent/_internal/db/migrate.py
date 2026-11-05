"""Numbered-SQL migration runner (OD-5): `migrations/NNNN_name.sql`, one transaction each.

One file per transaction means `CREATE INDEX CONCURRENTLY` (not allowed in a transaction block)
is not supported.
"""

import asyncio
import logging
import re
from dataclasses import dataclass
from importlib import resources
from typing import Any

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)

SCHEMA = "graph_agent"
# Arbitrary constant shared by every process that migrates this database.
ADVISORY_LOCK_KEY = 0x6772617068  # "graph"
_FILE_RE = re.compile(r"^(\d{4})_[a-z0-9_]+\.sql$")


class MigrationError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    sql: str


def discover_migrations() -> list[Migration]:
    """Packaged migration files ordered by version; duplicate versions are an error."""
    found: dict[int, Migration] = {}
    for entry in resources.files(__package__).joinpath("migrations").iterdir():
        if not entry.name.endswith(".sql"):
            continue
        match = _FILE_RE.match(entry.name)
        if match is None:
            raise MigrationError(f"bad migration file name: {entry.name!r} (want NNNN_name.sql)")
        version = int(match[1])
        if version in found:
            raise MigrationError(f"duplicate migration version {version}: {entry.name!r}")
        found[version] = Migration(version, entry.name, entry.read_text(encoding="utf-8"))
    return [found[v] for v in sorted(found)]


async def apply_migrations(pool: AsyncConnectionPool) -> list[int]:
    """Apply pending migrations under a session advisory lock; return the versions applied."""
    # importlib.resources does sync file I/O; keep it off the event loop.
    migrations = await asyncio.to_thread(discover_migrations)
    applied_now: list[int] = []
    async with pool.connection() as conn:
        try:
            # Session-level lock + per-file transactions need autocommit on this connection.
            await conn.set_autocommit(True)
            await conn.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
            try:
                applied_now = await _apply_pending(conn, migrations)
            finally:
                await conn.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_KEY,))
            await conn.set_autocommit(False)
        except BaseException:
            # Session state (lock, autocommit) is unknown: close so the pool discards the
            # connection and the server releases the advisory lock.
            await conn.close()
            raise
    return applied_now


async def _apply_pending(
    conn: AsyncConnection[tuple[Any, ...]], migrations: list[Migration]
) -> list[int]:
    await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    await conn.execute(
        f"CREATE TABLE IF NOT EXISTS {SCHEMA}.schema_version ("
        "version integer PRIMARY KEY, name text NOT NULL, "
        "applied_at timestamptz NOT NULL DEFAULT now())"
    )
    cur = await conn.execute(f"SELECT version FROM {SCHEMA}.schema_version")
    applied = {row[0] for row in await cur.fetchall()}
    known = {m.version for m in migrations}
    if missing := sorted(applied - known):
        raise MigrationError(f"applied versions without a migration file: {missing}")
    pending = [m for m in migrations if m.version not in applied]
    if applied and (stale := [m.version for m in pending if m.version < max(applied)]):
        raise MigrationError(f"pending migrations older than the latest applied: {stale}")
    applied_now: list[int] = []
    for mig in pending:
        async with conn.transaction():
            # no params => simple protocol, so a file may hold many statements
            await conn.execute(mig.sql)
            await conn.execute(
                f"INSERT INTO {SCHEMA}.schema_version (version, name) VALUES (%s, %s)",
                (mig.version, mig.name),
            )
        applied_now.append(mig.version)
        logger.info("applied migration %s", mig.name)
    return applied_now
