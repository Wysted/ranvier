import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from psycopg_pool import AsyncConnectionPool

from ranvier._internal.db.migrate import apply_migrations
from ranvier._internal.db.pool import open_app_pool
from ranvier._internal.runtime.context import RunContext
from ranvier._internal.usage import Counts, UsageRepository


@pytest.fixture
async def pool(clean_database: str) -> AsyncIterator[AsyncConnectionPool]:
    pool = await open_app_pool(clean_database, max_size=2)
    try:
        await apply_migrations(pool)
        yield pool
    finally:
        await pool.close()


async def test_attempts_accumulate_and_report_groups(pool: AsyncConnectionPool) -> None:
    repo = UsageRepository(pool)
    ctx = RunContext(str(uuid.uuid4()), str(uuid.uuid4()), "flow-a", 1, 1, None)
    attempt = {("m", "writer"): Counts(10, 5, 2, 1, 1), ("m", ""): Counts(1, 1, 0, 0, 1)}
    await repo.add(ctx, attempt)
    await repo.add(ctx, attempt)  # recovered run: second attempt adds on top
    await repo.add(ctx, {})  # nothing to write is fine

    since = datetime.now(UTC) - timedelta(hours=1)
    by_flow = await repo.report_rows(since, "flow")
    assert [(r.group, r.model, r.input_tokens, r.calls) for r in by_flow] == [
        ("flow-a", "m", 22, 4)
    ]
    by_agent = {r.group: r for r in await repo.report_rows(since, "agent")}
    assert by_agent["writer"].output_tokens == 10
    assert await repo.report_rows(datetime.now(UTC) + timedelta(hours=1), "model") == []
