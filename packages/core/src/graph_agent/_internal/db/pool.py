"""psycopg async pools (design section 8, "Postgres connections").

The caller owns the returned pools and closes them (lifespan / AsyncExitStack). `db` sits below
`config` in contract C1, so sizes and the URL arrive as plain arguments.
"""

import logging

from psycopg import AsyncConnection
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

from graph_agent._internal.db.migrate import SCHEMA

logger = logging.getLogger(__name__)

MIN_POOL_SIZE = 1

type CheckpointerPool = AsyncConnectionPool[AsyncConnection[DictRow]]


async def open_app_pool(url: str, max_size: int) -> AsyncConnectionPool:
    """Pool for repositories and the migration runner."""
    pool = AsyncConnectionPool(
        url, name="graph_agent_app", min_size=MIN_POOL_SIZE, max_size=max_size, open=False
    )
    await pool.open(wait=True)
    return pool


# Pins our tables to our schema whatever the role is called (a role-named schema or `public`
# would otherwise win through "$user").
PIN_SEARCH_PATH = f"-c search_path={SCHEMA}"


def with_options(url: str, extra: str) -> str:
    """The URL's `options` plus `extra`, for the `options` connection kwarg (appended last, so
    it wins over a URL `search_path`)."""
    existing = conninfo_to_dict(url).get("options")
    return f"{existing} {extra}" if existing else extra


async def open_checkpointer_pool(url: str, max_size: int) -> CheckpointerPool:
    """Pool settings required by `AsyncPostgresSaver`."""
    pool = AsyncConnectionPool(
        url,
        connection_class=AsyncConnection[DictRow],
        name="graph_agent_checkpointer",
        min_size=MIN_POOL_SIZE,
        max_size=max_size,
        kwargs={
            "autocommit": True,
            "prepare_threshold": 0,
            "row_factory": dict_row,
            "options": with_options(url, PIN_SEARCH_PATH),
        },
        open=False,
    )
    await pool.open(wait=True)
    return pool


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
