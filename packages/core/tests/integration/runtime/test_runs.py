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
    Run,
    RunError,
    RunsRepository,
    RunStatus,
)

LEASE_S = 30.0
MAX_ATTEMPTS = 3


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


async def _new(repo: RunsRepository, thread_id: uuid.UUID | None = None) -> Run:
    return await repo.create(
        thread_id=thread_id or uuid.uuid4(), flow="f", flow_version=1, input={"x": 1}
    )


async def _claim(repo: RunsRepository, run: Run, owner: str = "a") -> str:
    claim = await repo.claim(run.run_id, owner, LEASE_S, MAX_ATTEMPTS)
    assert claim.outcome is ClaimOutcome.CLAIMED
    assert claim.token is not None
    return claim.token


async def _get(repo: RunsRepository, run: Run) -> Run:
    stored = await repo.get(run.run_id, run.thread_id)
    assert stored is not None
    return stored


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
    assert await repo.get(first.run_id, thread) == first
    assert await repo.get(first.run_id, uuid.uuid4()) is None
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
    run = await _new(repo)
    results = await asyncio.gather(
        *(repo.claim(run.run_id, f"o{i}", LEASE_S, MAX_ATTEMPTS) for i in range(3))
    )
    outcomes = sorted(r.outcome for r in results)
    assert outcomes == [ClaimOutcome.CLAIMED, ClaimOutcome.LOST, ClaimOutcome.LOST]
    stored = await _get(repo, run)
    assert stored.status is RunStatus.RUNNING
    assert stored.attempt == 1
    assert stored.lease_owner is not None
    assert stored.lease_owner.startswith("o")
    assert stored.lease_expires_at is not None


async def test_second_running_run_is_thread_busy(repo: RunsRepository) -> None:
    thread = uuid.uuid4()
    first, second = await _new(repo, thread), await _new(repo, thread)
    await _claim(repo, first)
    busy = await repo.claim(second.run_id, "b", LEASE_S, MAX_ATTEMPTS)
    assert busy.outcome is ClaimOutcome.THREAD_BUSY
    assert (await _get(repo, second)).status is RunStatus.PENDING


async def test_expired_lease_is_reclaimable_and_fenced(
    repo: RunsRepository, pool: AsyncConnectionPool
) -> None:
    run = await _new(repo)
    old = await _claim(repo, run, "a")
    assert (await repo.claim(run.run_id, "b", LEASE_S, MAX_ATTEMPTS)).outcome is ClaimOutcome.LOST
    await _expire_lease(pool, run.run_id)
    taken = await repo.claim(run.run_id, "b", LEASE_S, MAX_ATTEMPTS)
    assert taken.outcome is ClaimOutcome.CLAIMED
    assert taken.run is not None
    assert taken.run.attempt == 2
    assert taken.token is not None
    # The previous owner can no longer heartbeat or write a terminal state.
    assert await repo.heartbeat(run.run_id, old, LEASE_S) is None
    assert not await repo.finish(run.run_id, old, RunStatus.SUCCESS)
    assert await repo.heartbeat(run.run_id, taken.token, LEASE_S) is False
    assert await repo.finish(run.run_id, taken.token, RunStatus.SUCCESS)
    done = await _get(repo, run)
    assert done.status is RunStatus.SUCCESS
    assert done.finished_at is not None


async def test_same_owner_reclaim_fences_old_token(
    repo: RunsRepository, pool: AsyncConnectionPool
) -> None:
    run = await _new(repo)
    old = await _claim(repo, run, "w")
    await _expire_lease(pool, run.run_id)
    new = await _claim(repo, run, "w")
    assert new != old
    assert await repo.heartbeat(run.run_id, old, LEASE_S) is None
    assert not await repo.finish(run.run_id, old, RunStatus.SUCCESS)
    assert await repo.finish(run.run_id, new, RunStatus.SUCCESS)


async def test_takeover_respects_max_attempts(
    repo: RunsRepository, pool: AsyncConnectionPool
) -> None:
    run = await _new(repo)
    for _ in range(MAX_ATTEMPTS):
        await _claim(repo, run)
        await _expire_lease(pool, run.run_id)
    assert (await repo.claim(run.run_id, "a", LEASE_S, MAX_ATTEMPTS)).outcome is ClaimOutcome.LOST


async def test_cancel_before_claim(repo: RunsRepository) -> None:
    run = await _new(repo)
    assert await repo.request_cancel(run.run_id, run.thread_id) is RunStatus.CANCELLED
    assert (await repo.claim(run.run_id, "a", LEASE_S, MAX_ATTEMPTS)).outcome is ClaimOutcome.LOST
    assert await repo.request_cancel(run.run_id, run.thread_id) is None


async def test_request_cancel_is_scoped_to_thread(repo: RunsRepository) -> None:
    run = await _new(repo)
    assert await repo.request_cancel(run.run_id, uuid.uuid4()) is None
    assert (await _get(repo, run)).status is RunStatus.PENDING


async def test_cancel_running_is_seen_by_heartbeat(repo: RunsRepository) -> None:
    run = await _new(repo)
    token = await _claim(repo, run)
    assert await repo.request_cancel(run.run_id, run.thread_id) is RunStatus.RUNNING
    assert await repo.heartbeat(run.run_id, token, LEASE_S) is True
    assert await repo.finish(run.run_id, token, RunStatus.CANCELLED)


async def test_cancel_requested_is_claimed_into_cancelled(
    repo: RunsRepository, pool: AsyncConnectionPool
) -> None:
    run = await _new(repo)
    await _claim(repo, run)
    await repo.request_cancel(run.run_id, run.thread_id)
    await _expire_lease(pool, run.run_id)
    claim = await repo.claim(run.run_id, "b", LEASE_S, MAX_ATTEMPTS)
    assert claim.outcome is ClaimOutcome.CANCELLED
    assert claim.token is None
    stored = await _get(repo, run)
    assert stored.status is RunStatus.CANCELLED
    assert stored.finished_at is not None
    assert stored.lease_owner is None


async def test_cancel_requested_with_live_lease_is_never_claimed(repo: RunsRepository) -> None:
    run = await _new(repo)
    token = await _claim(repo, run)
    await repo.request_cancel(run.run_id, run.thread_id)
    assert (await repo.claim(run.run_id, "b", LEASE_S, MAX_ATTEMPTS)).outcome is ClaimOutcome.LOST
    assert (await _get(repo, run)).status is RunStatus.RUNNING
    assert await repo.heartbeat(run.run_id, token, LEASE_S) is True


async def test_finish_validates_status_and_error(repo: RunsRepository) -> None:
    run = await _new(repo)
    token = await _claim(repo, run)
    error = RunError(ErrorCode.RUN_TIMEOUT, "slow", retryable=True)
    with pytest.raises(ValueError, match="terminal"):
        await repo.finish(run.run_id, token, RunStatus.PENDING)
    with pytest.raises(ValueError, match="error"):
        await repo.finish(run.run_id, token, RunStatus.ERROR)
    with pytest.raises(ValueError, match="error"):
        await repo.finish(run.run_id, token, RunStatus.SUCCESS, error)
    assert await repo.finish(run.run_id, token, RunStatus.ERROR, error)
    stored = await _get(repo, run)
    assert stored.error is not None
    assert stored.error.code is ErrorCode.RUN_TIMEOUT
    assert stored.error.retryable


async def test_drain_release_keeps_attempt_and_is_reclaimable(repo: RunsRepository) -> None:
    run = await _new(repo)
    for _ in range(MAX_ATTEMPTS + 2):  # repeated deploys never exhaust retries
        token = await _claim(repo, run)
        assert not await repo.release_for_drain(run.run_id, "other")
        assert await repo.release_for_drain(run.run_id, token)
        # The drained owner is fenced out.
        assert await repo.heartbeat(run.run_id, token, LEASE_S) is None
        assert not await repo.finish(run.run_id, token, RunStatus.SUCCESS)
        assert not await repo.release_for_drain(run.run_id, token)
    stored = await _get(repo, run)
    assert (stored.status, stored.attempt) == (RunStatus.RUNNING, 0)
    await _claim(repo, run)


async def test_expired_sweep_and_exhaustion(
    repo: RunsRepository, pool: AsyncConnectionPool
) -> None:
    retry, exhausted, alive = await _new(repo), await _new(repo), await _new(repo)
    for run in (retry, exhausted, alive):
        await _claim(repo, run)
    # `exhausted` has been claimed twice already.
    await _expire_lease(pool, exhausted.run_id)
    await _claim(repo, exhausted)
    await _expire_lease(pool, exhausted.run_id)
    await _expire_lease(pool, retry.run_id)

    result = await repo.expired(max_attempts=2)
    assert result.redefer == [(retry.run_id, retry.thread_id)]
    assert result.exhausted == [exhausted.run_id]
    gone = await _get(repo, exhausted)
    assert gone.status is RunStatus.ERROR
    assert gone.error is not None
    assert gone.error.code is ErrorCode.ORPHAN_RETRIES_EXHAUSTED
    # Recovery never writes pending.
    assert (await _get(repo, retry)).status is RunStatus.RUNNING
    assert (await _get(repo, alive)).status is RunStatus.RUNNING


async def test_exhausted_and_cancel_requested_ends_cancelled(
    repo: RunsRepository, pool: AsyncConnectionPool
) -> None:
    swept, claimed = await _new(repo), await _new(repo)
    for run in (swept, claimed):
        for _ in range(MAX_ATTEMPTS):
            await _claim(repo, run)
            await _expire_lease(pool, run.run_id)
        await repo.request_cancel(run.run_id, run.thread_id)
    claim = await repo.claim(claimed.run_id, "a", LEASE_S, MAX_ATTEMPTS)
    assert claim.outcome is ClaimOutcome.CANCELLED
    result = await repo.expired(max_attempts=MAX_ATTEMPTS)
    assert result.exhausted == result.redefer == []
    for run in (swept, claimed):
        stored = await _get(repo, run)
        assert stored.status is RunStatus.CANCELLED
        assert stored.error is None
        assert stored.finished_at is not None


async def test_stale_pending(repo: RunsRepository, pool: AsyncConnectionPool) -> None:
    old, fresh = await _new(repo), await _new(repo)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE graph_agent.runs SET created_at = now() - interval '1 hour' WHERE run_id = %s",
            (old.run_id,),
        )
    assert await repo.stale_pending(older_than_s=60) == [(old.run_id, old.thread_id)]
    assert fresh.run_id not in [r for r, _ in await repo.stale_pending(older_than_s=60)]


async def test_delete_thread_refused_while_running(repo: RunsRepository) -> None:
    thread = uuid.uuid4()
    run = await _new(repo, thread)
    await _new(repo, thread)
    token = await _claim(repo, run)
    with pytest.raises(RunError) as refused:
        await repo.delete_thread(thread)
    assert refused.value.code is ErrorCode.THREAD_BUSY
    assert len(await repo.list_for_thread(thread)) == 2  # rolled back
    await repo.finish(run.run_id, token, RunStatus.SUCCESS)
    assert await repo.delete_thread(thread) == 2
    assert await repo.list_for_thread(thread) == []
