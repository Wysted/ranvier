import asyncio
import uuid
from collections.abc import AsyncIterator

import psycopg
import pytest
from psycopg_pool import AsyncConnectionPool

from ranvier._internal.db.migrate import (
    MIGRATION_LOCK_POLL_S,
    MigrationError,
    advisory_lock,
    apply_migrations,
    discover_migrations,
)
from ranvier._internal.db.pool import open_app_pool

CONCURRENT_RUNNERS = 4


@pytest.fixture
async def pool(clean_database: str) -> AsyncIterator[AsyncConnectionPool]:
    pool = await open_app_pool(clean_database, max_size=CONCURRENT_RUNNERS)
    try:
        yield pool
    finally:
        await pool.close()


async def _discover() -> list[int]:
    return [m.version for m in await asyncio.to_thread(discover_migrations)]


async def _versions(pool: AsyncConnectionPool) -> list[int]:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT version FROM ranvier.schema_version ORDER BY 1")
        return [row[0] for row in await cur.fetchall()]


async def test_apply_twice_is_noop(pool: AsyncConnectionPool) -> None:
    first = await apply_migrations(pool)
    assert first == await _discover()
    assert await apply_migrations(pool) == []
    assert await _versions(pool) == first


async def test_concurrent_runners_do_not_race(pool: AsyncConnectionPool) -> None:
    results = await asyncio.gather(*(apply_migrations(pool) for _ in range(CONCURRENT_RUNNERS)))
    assert sum(1 for r in results if r) == 1  # exactly one runner applied the files
    assert await _versions(pool) == await _discover()


async def test_applied_version_without_file_fails(pool: AsyncConnectionPool) -> None:
    await apply_migrations(pool)
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO ranvier.schema_version (version, name) VALUES (9999, 'ghost.sql')"
        )
    with pytest.raises(MigrationError, match="9999"):
        await apply_migrations(pool)


async def _insert_run(
    conn: psycopg.AsyncConnection[tuple[object, ...]], thread_id: uuid.UUID, status: str
) -> None:
    await conn.execute(
        "INSERT INTO ranvier.runs "
        "(run_id, thread_id, flow, flow_version, status, input, lease_owner, lease_expires_at) "
        "VALUES (%s, %s, 'f', 1, %s, '{}', 'w', now() + interval '1 minute')",
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
                "INSERT INTO ranvier.runs (run_id, thread_id, flow, flow_version, status) "
                "VALUES (%s, %s, 'f', 1, 'pending')",
                (uuid.uuid4(), uuid.uuid4()),
            )


async def test_json_null_counts_as_absent(pool: AsyncConnectionPool) -> None:
    await apply_migrations(pool)
    async with pool.connection() as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            await conn.execute(
                "INSERT INTO ranvier.runs (run_id, thread_id, flow, flow_version, status, "
                "input) VALUES (%s, %s, 'f', 1, 'pending', 'null'::jsonb)",
                (uuid.uuid4(), uuid.uuid4()),
            )


async def test_running_requires_lease(pool: AsyncConnectionPool) -> None:
    await apply_migrations(pool)
    async with pool.connection() as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            await conn.execute(
                "INSERT INTO ranvier.runs (run_id, thread_id, flow, flow_version, status, "
                "input) VALUES (%s, %s, 'f', 1, 'running', '{}')",
                (uuid.uuid4(), uuid.uuid4()),
            )


async def test_connection_state_restored(clean_database: str) -> None:
    single = await open_app_pool(clean_database, max_size=1)
    try:
        await apply_migrations(single)
        async with single.connection() as conn:
            assert conn.autocommit is False
    finally:
        await single.close()


async def test_waiting_for_the_lock_is_logged_once_with_the_holder(
    pool: AsyncConnectionPool, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("WARNING", logger="ranvier._internal.db.migrate")
    async with advisory_lock(pool) as holder:
        task = asyncio.create_task(apply_migrations(pool))
        await asyncio.sleep(MIGRATION_LOCK_POLL_S * 4)  # several polls, one log line
        assert not task.done()
        cur = await holder.execute("SELECT pg_backend_pid()")
        holder_row = await cur.fetchone()
    await task
    warnings = [r.getMessage() for r in caplog.records if "migration lock" in r.getMessage()]
    assert holder_row is not None
    assert warnings == [f"migration lock is held by pid {holder_row[0]}; waiting"]
