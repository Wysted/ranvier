"""psycopg async pools (design section 8, "Postgres connections").

The caller owns the returned pools and closes them (lifespan / AsyncExitStack). `db` sits below
`config` in contract C1, so sizes and the URL arrive as plain arguments.
"""

import logging
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)

MIN_POOL_SIZE = 1


async def _open(url: str, max_size: int, kwargs: dict[str, Any]) -> AsyncConnectionPool:
    pool = AsyncConnectionPool(
        url, min_size=min(MIN_POOL_SIZE, max_size), max_size=max_size, kwargs=kwargs, open=False
    )
    await pool.open(wait=True)
    return pool


async def open_app_pool(url: str, max_size: int) -> AsyncConnectionPool:
    """Pool for repositories and the migration runner."""
    return await _open(url, max_size, {})


async def open_checkpointer_pool(url: str, max_size: int) -> AsyncConnectionPool:
    """Pool settings required by `AsyncPostgresSaver`."""
    return await _open(
        url, max_size, {"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row}
    )


def log_connection_budget(app_max: int, checkpointer_max: int, procrastinate_max: int) -> int:
    """Log and return the worst-case connections this process holds (sum of the three pools)."""
    total = app_max + checkpointer_max + procrastinate_max
    logger.info(
        "postgres connection budget: %d (app=%d, checkpointer=%d, procrastinate=%d)",
        total,
        app_max,
        checkpointer_max,
        procrastinate_max,
    )
    return total
