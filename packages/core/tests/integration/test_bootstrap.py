import pytest
from pydantic import SecretStr

from graph_agent._internal.bootstrap import open_container
from graph_agent._internal.config.settings import DatabaseSettings, LimitsSettings, Settings


@pytest.fixture
def settings(database_url: str) -> Settings:
    return Settings(
        _env_file=None,
        database=DatabaseSettings(url=SecretStr(database_url)),
        limits=LimitsSettings(cpu_workers=1, shutdown_grace_s=5),
    )


async def test_container_opens_pools_and_tears_down_cleanly(settings: Settings) -> None:
    async with open_container(settings) as c:
        assert c.app_pool is not None and c.checkpointer_pool is not None
        async with c.app_pool.connection() as conn:
            row = await (await conn.execute("select 1")).fetchone()
        assert row == (1,)
        assert type(c.checkpointer).__name__ == "PooledPostgresSaver"
    assert c.app_pool.closed and c.checkpointer_pool.closed
    assert c.lag_sampler._task is None
