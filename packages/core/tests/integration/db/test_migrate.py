import asyncio
import uuid
from collections.abc import AsyncIterator

import psycopg
import pytest
from psycopg_pool import AsyncConnectionPool

from graph_agent._internal.db.migrate import MigrationError, apply_migrations, discover_migrations
from graph_agent._internal.db.pool import open_app_pool

CONCURRENT_RUNNERS = 4


@pytest.fixture
async def pool(clean_database: str) -> AsyncIterator[AsyncConnectionPool]:
    pool = await open_app_pool(clean_database, max_size=CONCURRENT_RUNNERS)
    try:
        yield pool
    finally:
        await pool.close()


async def _versions(pool: AsyncConnectionPool) -> list[int]:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT version FROM graph_agent.schema_version ORDER BY 1")
        return [row[0] for row in await cur.fetchall()]


async def test_apply_twice_is_noop(pool: AsyncConnectionPool) -> None:
    first = await apply_migrations(pool)
    assert first == [m.version for m in discover_migrations()]
    assert await apply_migrations(pool) == []
    assert await _versions(pool) == first


async def test_concurrent_runners_do_not_race(pool: AsyncConnectionPool) -> None:
    results = await asyncio.gather(*(apply_migrations(pool) for _ in range(CONCURRENT_RUNNERS)))
    assert sum(1 for r in results if r) == 1  # exactly one runner applied the files
    assert await _versions(pool) == [m.version for m in discover_migrations()]


async def test_applied_version_without_file_fails(pool: AsyncConnectionPool) -> None:
    await apply_migrations(pool)
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO graph_agent.schema_version (version, name) VALUES (9999, 'ghost.sql')"
        )
    with pytest.raises(MigrationError, match="9999"):
        await apply_migrations(pool)


async def _insert_run(
    conn: psycopg.AsyncConnection[tuple[object, ...]], thread_id: uuid.UUID, status: str
) -> None:
    await conn.execute(
        "INSERT INTO graph_agent.runs (run_id, thread_id, flow, flow_version, status, input) "
        "VALUES (%s, %s, 'f', 1, %s, '{}')",
        (uuid.uuid4(), thread_id, status),
    )


async def test_one_running_run_per_thread(pool: AsyncConnectionPool) -> None:
    await apply_migrations(pool)
    thread = uuid.uuid4()
    async with pool.connection() as conn:
        await _insert_run(conn, thread, "running")
        await _insert_run(conn, thread, "success")  # non-running rows are unrestricted
        with pytest.raises(psycopg.errors.UniqueViolation):
            await _insert_run(conn, thread, "running")


async def test_input_xor_command(pool: AsyncConnectionPool) -> None:
    await apply_migrations(pool)
    async with pool.connection() as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            await conn.execute(
                "INSERT INTO graph_agent.runs (run_id, thread_id, flow, flow_version, status) "
                "VALUES (%s, %s, 'f', 1, 'pending')",
                (uuid.uuid4(), uuid.uuid4()),
            )
