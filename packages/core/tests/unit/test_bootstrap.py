from contextlib import AsyncExitStack
from typing import Any

import pytest

from graph_agent._internal import bootstrap
from graph_agent._internal.config.settings import Settings
from graph_agent._internal.observability.tracing import LoggingMetrics, Observability


def _install_fakes(
    monkeypatch: pytest.MonkeyPatch, log: list[str], *, fail_at: str | None = None
) -> None:
    def step(name: str) -> None:
        log.append(f"open {name}")
        if name == fail_at:
            raise RuntimeError(f"boom {name}")

    class FakeCpuPool:
        free_slots = 0

        def __init__(self, workers: int) -> None:
            pass

        async def start(self) -> None:
            step("cpu")

        def close(self) -> None:
            log.append("close cpu")

    class FakeLlm:
        def __init__(self, settings: Any) -> None:
            step("llm")

        async def aclose(self) -> None:
            log.append("close llm")

    def fake_obs(settings: Settings) -> Observability:
        step("obs")
        return Observability(metrics=LoggingMetrics())

    async def fake_shutdown(obs: Observability, grace_s: float) -> None:
        log.append("close obs")

    async def fake_mcp(servers: Any, stack: AsyncExitStack) -> object:
        step("mcp")
        stack.callback(log.append, "close mcp")
        return object()

    monkeypatch.setattr(bootstrap, "CpuPool", FakeCpuPool)
    monkeypatch.setattr(bootstrap, "LlmRegistry", FakeLlm)
    monkeypatch.setattr(bootstrap, "setup_observability", fake_obs)
    monkeypatch.setattr(bootstrap, "_shutdown_observability", fake_shutdown)
    monkeypatch.setattr(bootstrap, "open_mcp_sources", fake_mcp)


@pytest.fixture
def settings() -> Settings:
    # Built outside the loop: the TOML source stats files (blockbuster).
    return Settings(_env_file=None)


async def test_memory_mode_opens_no_pool_and_closes_in_reverse(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    log: list[str] = []
    _install_fakes(monkeypatch, log)
    async with bootstrap.open_container(settings, memory=True) as c:
        assert c.app_pool is None and c.checkpointer_pool is None
        assert type(c.checkpointer).__name__ == "InMemorySaver"
        assert c.observers == []
        assert log == ["open obs", "open llm", "open cpu", "open mcp"]
        assert c.lag_sampler._task is not None  # started
    assert log[4:] == ["close mcp", "close cpu", "close llm", "close obs"]
    assert c.lag_sampler._task is None


async def test_failure_mid_startup_unwinds_what_was_opened(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    log: list[str] = []
    _install_fakes(monkeypatch, log, fail_at="mcp")
    with pytest.raises(RuntimeError, match="boom mcp"):
        async with bootstrap.open_container(settings, memory=True):
            pytest.fail("must not yield")
    assert log == [
        "open obs",
        "open llm",
        "open cpu",
        "open mcp",
        "close cpu",
        "close llm",
        "close obs",
    ]


async def test_cpu_warmup_failure_still_kills_workers(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    log: list[str] = []
    _install_fakes(monkeypatch, log, fail_at="cpu")
    with pytest.raises(RuntimeError, match="boom cpu"):
        async with bootstrap.open_container(settings, memory=True):
            pytest.fail("must not yield")
    assert log == ["open obs", "open llm", "open cpu", "close cpu", "close llm", "close obs"]
