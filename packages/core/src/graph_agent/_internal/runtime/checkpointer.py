"""Checkpointer construction: `AsyncPostgresSaver` over the checkpointer pool, or `InMemorySaver`.

The pool is owned by the container (`db.pool.open_checkpointer_pool`); nothing here opens or
closes one. Checkpoint writes are not atomic with the app's UnitOfWork (design section 10).
"""

import asyncio

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg_pool import AsyncConnectionPool

from graph_agent._internal.db.pool import CheckpointerPool


class PooledPostgresSaver(AsyncPostgresSaver):
    """`AsyncPostgresSaver` whose DB calls run concurrently, bounded by the pool size."""

    def __init__(self, pool: CheckpointerPool) -> None:
        super().__init__(conn=pool)
        if isinstance(self.conn, AsyncConnectionPool) and self.pipe is None:
            # shortcut: replaces upstream's per-instance lock (serializes every call even over a
            # pool, issue #7259); remove when upstream drops it.
            self.lock = asyncio.Semaphore(self.conn.max_size)  # type: ignore[assignment]


type Checkpointer = PooledPostgresSaver | InMemorySaver


def create_checkpointer(pool: CheckpointerPool) -> PooledPostgresSaver:
    """Call from inside the running loop: upstream captures `asyncio.get_running_loop()`."""
    return PooledPostgresSaver(pool)


def create_memory_checkpointer() -> InMemorySaver:
    return InMemorySaver()


async def setup_checkpointer(pool: CheckpointerPool) -> None:
    """Create/migrate the checkpoint tables. Idempotent (upstream tracks `checkpoint_migrations`).

    Precondition: the caller holds the migrations session advisory lock (`pg_advisory_lock`) on a
    connection with no open transaction, so concurrent `migrate` runs cannot race here. The pool
    must be autocommit (as `open_checkpointer_pool` is): setup runs `CREATE INDEX CONCURRENTLY`.
    """
    await create_checkpointer(pool).setup()
