import asyncio
import uuid
from collections.abc import AsyncIterator

import pytest
from psycopg import errors
from psycopg_pool import AsyncConnectionPool

from graph_agent._internal.db.migrate import apply_migrations
from graph_agent._internal.db.pool import open_app_pool
from graph_agent._internal.runtime.runs import (
    ClaimOutcome,
    ErrorCode,
    RunError,
    RunsRepository,
    RunStatus,
)

LEASE_S = 30.0


@pytest.fixture
async def pool(clean_database: str) -> AsyncIterator[AsyncConnectionPool]:
    pool = await open_app_pool(clean_database, max_size=4)
    try:
        await apply_migrations(pool)
        yield pool
    finally:
        await pool.close()


@pytest.fixture
def repo(pool: AsyncConnectionPool) -> RunsRepository:
    return RunsRepository(pool)


async def _new(repo: RunsRepository, thread_id: uuid.UUID | None = None) -> uuid.UUID:
    run = await repo.create(
        thread_id=thread_id or uuid.uuid4(), flow="f", flow_version=1, input={"x": 1}
    )
    return run.run_id


async def _expire_lease(pool: AsyncConnectionPool, run_id: uuid.UUID) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE graph_agent.runs SET lease_expires_at = now() - interval '1 second' "
            "WHERE run_id = %s",
            (run_id,),
        )


async def test_create_get_list_pin(repo: RunsRepository) -> None:
    thread = uuid.uuid4()
    first = await repo.create(
        thread_id=thread, flow="f", flow_version=2, tenant_id="t", input={"a": 1}
    )
    second = await repo.create(thread_id=thread, flow="f", flow_version=2, command={"resume": 1})
    assert first.status is RunStatus.PENDING
    assert await repo.get(first.run_id) == first
    assert [r.run_id for r in await repo.list_for_thread(thread)] == [
        second.run_id,
        first.run_id,
    ]
    pin = await repo.thread_pin(thread)
    assert pin is not None
    assert (pin.flow, pin.flow_version, pin.tenant_id) == ("f", 2, "t")
    assert await repo.thread_pin(uuid.uuid4()) is None


async def test_input_xor_command(repo: RunsRepository) -> None:
    with pytest.raises(errors.CheckViolation):
        await repo.create(thread_id=uuid.uuid4(), flow="f", flow_version=1)


async def test_concurrent_claims_have_one_winner(repo: RunsRepository) -> None:
    run_id = await _new(repo)
    results = await asyncio.gather(*(repo.claim(run_id, f"o{i}", LEASE_S) for i in range(3)))
    outcomes = sorted(r.outcome for r in results)
    assert outcomes == [ClaimOutcome.CLAIMED, ClaimOutcome.LOST, ClaimOutcome.LOST]
    run = await repo.get(run_id)
    assert run is not None
    assert run.status is RunStatus.RUNNING
    assert run.attempt == 1
    assert run.lease_owner is not None
    assert run.lease_expires_at is not None


async def test_second_running_run_is_thread_busy(repo: RunsRepository) -> None:
    thread = uuid.uuid4()
    first, second = await _new(repo, thread), await _new(repo, thread)
    assert (await repo.claim(first, "a", LEASE_S)).outcome is ClaimOutcome.CLAIMED
    assert (await repo.claim(second, "b", LEASE_S)).outcome is ClaimOutcome.THREAD_BUSY
    stored = await repo.get(second)
    assert stored is not None
    assert stored.status is RunStatus.PENDING


async def test_expired_lease_is_reclaimable_and_fenced(
    repo: RunsRepository, pool: AsyncConnectionPool
) -> None:
    run_id = await _new(repo)
    await repo.claim(run_id, "a", LEASE_S)
    assert (await repo.claim(run_id, "b", LEASE_S)).outcome is ClaimOutcome.LOST
    await _expire_lease(pool, run_id)
    taken = await repo.claim(run_id, "b", LEASE_S)
    assert taken.outcome is ClaimOutcome.CLAIMED
    assert taken.run is not None
    assert taken.run.attempt == 2
    # The previous owner can no longer heartbeat or write a terminal state.
    assert await repo.heartbeat(run_id, "a", LEASE_S) is None
    assert not await repo.finish(run_id, "a", RunStatus.SUCCESS)
    assert await repo.heartbeat(run_id, "b", LEASE_S) is False
    assert await repo.finish(run_id, "b", RunStatus.SUCCESS)
    done = await repo.get(run_id)
    assert done is not None
    assert done.status is RunStatus.SUCCESS
    assert done.finished_at is not None


async def test_cancel_before_claim(repo: RunsRepository) -> None:
    run_id = await _new(repo)
    assert await repo.request_cancel(run_id) is RunStatus.CANCELLED
    assert (await repo.claim(run_id, "a", LEASE_S)).outcome is ClaimOutcome.LOST
    assert await repo.request_cancel(run_id) is None


async def test_cancel_running_is_seen_by_heartbeat(repo: RunsRepository) -> None:
    run_id = await _new(repo)
    await repo.claim(run_id, "a", LEASE_S)
    assert await repo.request_cancel(run_id) is RunStatus.RUNNING
    assert await repo.heartbeat(run_id, "a", LEASE_S) is True
    assert await repo.finish(run_id, "a", RunStatus.CANCELLED)


async def test_finish_error_stores_error_and_rejects_non_terminal(repo: RunsRepository) -> None:
    run_id = await _new(repo)
    await repo.claim(run_id, "a", LEASE_S)
    with pytest.raises(ValueError, match="terminal"):
        await repo.finish(run_id, "a", RunStatus.PENDING)
    error = RunError(ErrorCode.RUN_TIMEOUT, "slow", retryable=True)
    assert await repo.finish(run_id, "a", RunStatus.ERROR, error)
    stored = await repo.get(run_id)
    assert stored is not None
    assert stored.error is not None
    assert stored.error.code is ErrorCode.RUN_TIMEOUT
    assert stored.error.retryable


async def test_requeue_drain(repo: RunsRepository) -> None:
    run_id = await _new(repo)
    await repo.claim(run_id, "a", LEASE_S)
    assert not await repo.requeue(run_id, "other")
    assert await repo.requeue(run_id, "a")
    run = await repo.get(run_id)
    assert run is not None
    assert (run.status, run.attempt, run.lease_owner) == (RunStatus.PENDING, 0, None)


async def test_requeue_expired_and_exhaustion(
    repo: RunsRepository, pool: AsyncConnectionPool
) -> None:
    retry, exhausted, alive = await _new(repo), await _new(repo), await _new(repo)
    for run_id in (retry, exhausted, alive):
        await repo.claim(run_id, "a", LEASE_S)
    # `exhausted` has been claimed twice already.
    await _expire_lease(pool, exhausted)
    await repo.claim(exhausted, "a", LEASE_S)
    await _expire_lease(pool, exhausted)
    await _expire_lease(pool, retry)

    result = await repo.requeue_expired(max_attempts=2)
    assert result.pending == [retry]
    assert result.exhausted == [exhausted]
    gone = await repo.get(exhausted)
    assert gone is not None
    assert gone.status is RunStatus.ERROR
    assert gone.error is not None
    assert gone.error.code is ErrorCode.ORPHAN_RETRIES_EXHAUSTED
    back = await repo.get(retry)
    assert back is not None
    assert back.status is RunStatus.PENDING
    alive_run = await repo.get(alive)
    assert alive_run is not None
    assert alive_run.status is RunStatus.RUNNING


async def test_delete_thread_refused_while_running(repo: RunsRepository) -> None:
    thread = uuid.uuid4()
    run_id = await _new(repo, thread)
    await _new(repo, thread)
    await repo.claim(run_id, "a", LEASE_S)
    with pytest.raises(RunError) as refused:
        await repo.delete_thread(thread)
    assert refused.value.code is ErrorCode.THREAD_BUSY
    await repo.finish(run_id, "a", RunStatus.SUCCESS)
    assert await repo.delete_thread(thread) == 2
    assert await repo.list_for_thread(thread) == []
