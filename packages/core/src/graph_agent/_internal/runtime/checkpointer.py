"""Checkpointer construction: `AsyncPostgresSaver` over the checkpointer pool, or `InMemorySaver`.

The pool is owned by the container (`db.pool.open_checkpointer_pool`); nothing here opens or
closes one. Checkpoint writes are not atomic with the app's UnitOfWork (design section 10).
"""

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool

type CheckpointerPool = AsyncConnectionPool[AsyncConnection[DictRow]]


def create_checkpointer(pool: CheckpointerPool) -> AsyncPostgresSaver:
    """Call from inside the running loop: upstream captures `asyncio.get_running_loop()`."""
    # shortcut: upstream serializes writes per saver instance (design section 14, S4 verifies),
    # so use one saver per process and size the pool for concurrent reads, not write parallelism;
    # revisit if checkpoint latency shows up under load.
    return AsyncPostgresSaver(conn=pool)


def create_memory_checkpointer() -> InMemorySaver:
    return InMemorySaver()


async def setup_checkpointer(pool: CheckpointerPool) -> None:
    """Create/migrate the checkpoint tables. Idempotent (upstream tracks `checkpoint_migrations`).

    shortcut: no cross-process lock, so two concurrent `migrate` runs can race on the saver's
    migrations; add an advisory lock (released in `finally`) if deploys ever overlap.
    """
    await create_checkpointer(pool).setup()
