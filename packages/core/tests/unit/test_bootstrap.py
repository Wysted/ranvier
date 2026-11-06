import asyncio
from contextlib import AsyncExitStack
from typing import Any

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from ranvier._internal import bootstrap
from ranvier._internal.bootstrap import ShutdownBudget
from ranvier._internal.config.manifest import (
    FlowSpec,
    LlmProfile,
    LlmSettings,
    McpServerSpec,
    McpSettings,
    ToolSpec,
)
from ranvier._internal.config.settings import LimitsSettings, Settings
from ranvier._internal.limits import HANDOVER_MARGIN_S, ShuttingDown
from ranvier._internal.observability.tracing import LoggingMetrics, Observability

LAG_TASK = "loop-lag-sampler"


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

    async def fake_shutdown(obs: Observability, budget: Any) -> None:
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
        assert isinstance(c.checkpointer, InMemorySaver)
        assert c.runs is None
        assert c.observers == []
        assert log == ["open obs", "open llm", "open cpu", "open mcp"]
        assert any(t.get_name() == LAG_TASK for t in asyncio.all_tasks())  # started
    assert log[4:] == ["close mcp", "close cpu", "close llm", "close obs"]
    assert not any(t.get_name() == LAG_TASK for t in asyncio.all_tasks())


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


async def test_real_cpu_pool_is_built_off_the_loop(settings: Settings) -> None:
    # Real CpuPool (no fake): blockbuster fails the test if the constructor blocks the loop.
    async with bootstrap.open_container(
        settings.model_copy(update={"limits": _one_worker(settings)}), memory=True
    ) as c:
        assert c.cpu_pool.free_slots >= 1


def _one_worker(settings: Settings) -> LimitsSettings:
    return settings.limits.model_copy(update={"cpu_workers": 1})


async def test_default_executor_still_works_after_close(settings: Settings) -> None:
    async with bootstrap.open_container(
        settings.model_copy(update={"limits": _one_worker(settings)}), memory=True
    ):
        pass
    assert await asyncio.to_thread(lambda: 42) == 42


async def test_shutdown_budget_starts_when_close_begins(settings: Settings) -> None:
    async with bootstrap.open_container(
        settings.model_copy(update={"limits": _one_worker(settings)}), memory=True
    ) as c:
        assert c.shutdown_budget.deadline == float("inf")
    loop = asyncio.get_running_loop()
    assert (
        loop.time() < c.shutdown_budget.deadline <= loop.time() + settings.limits.shutdown_grace_s
    )


def test_sample_publishes_every_gauge() -> None:
    seen: dict[str, float] = {}

    class Metrics(LoggingMetrics):
        def gauge(self, name: str, value: float, attributes: Any = None) -> None:
            seen[name] = value

    bootstrap._sample(Observability(metrics=Metrics()), {"a": lambda: 1, "b": lambda: 2.5})
    assert seen == {"a": 1.0, "b": 2.5}


async def test_begin_shutdown_closes_admission_starts_the_clock_and_drains(
    settings: Settings,
) -> None:
    async with bootstrap.open_container(
        settings.model_copy(update={"limits": _one_worker(settings)}), memory=True
    ) as c:
        loop = asyncio.get_running_loop()
        c.begin_shutdown()
        deadline = c.shutdown_budget.deadline
        assert loop.time() < deadline <= loop.time() + settings.limits.shutdown_grace_s
        assert c.active_runs.draining
        with pytest.raises(ShuttingDown):
            await c.limits.admit().__aenter__()
        c.begin_shutdown()  # a second signal does not move the clock
        assert c.shutdown_budget.deadline == deadline
    assert c.shutdown_budget.deadline == deadline  # nor does the close


async def test_close_waits_for_the_runs_in_flight(settings: Settings) -> None:
    gate = asyncio.Event()
    async with bootstrap.open_container(
        settings.model_copy(update={"limits": _one_worker(settings)}), memory=True
    ) as c:
        task = c.active_runs.spawn(gate.wait())
        asyncio.get_running_loop().call_later(0.05, gate.set)
    assert task.done() and c.active_runs.draining  # the close drained and awaited it


def test_short_grace_warns_naming_the_longest_timeout(caplog: pytest.LogCaptureFixture) -> None:
    short = Settings(
        _env_file=None,
        limits=LimitsSettings(shutdown_grace_s=30),
        tools={"slow": ToolSpec(target="m:t", timeout_s=40), "fast": ToolSpec(target="m:t")},
        llm=LlmSettings(
            profiles={
                "p": LlmProfile(
                    provider="openai", model="m", api_key_env="K", timeout_s=20, max_retries=2
                ),
            }
        ),
    )
    with caplog.at_level("WARNING", logger=bootstrap.__name__):
        bootstrap._warn_short_grace(short)
    assert "tool slow" in caplog.text and "40s" in caplog.text and "waits 25s" in caplog.text
    caplog.clear()
    with caplog.at_level("WARNING", logger=bootstrap.__name__):
        bootstrap._warn_short_grace(
            short.model_copy(update={"limits": LimitsSettings(shutdown_grace_s=120)})
        )
        bootstrap._warn_short_grace(Settings(_env_file=None))  # nothing declared
    assert caplog.text == ""


def test_defaults_with_a_default_profile_tool_and_mcp_server_do_not_warn(
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = Settings(
        _env_file=None,
        tools={"t": ToolSpec(target="m:t")},
        llm=LlmSettings(profiles={"p": LlmProfile(provider="openai", model="m", api_key_env="K")}),
        mcp=McpSettings(servers={"s": McpServerSpec(url="https://example.com/mcp")}),
    )
    with caplog.at_level("WARNING", logger=bootstrap.__name__):
        bootstrap._warn_short_grace(settings)
    assert caplog.text == ""


def test_flow_timeouts_do_not_count_against_the_grace(caplog: pytest.LogCaptureFixture) -> None:
    # A drain waits one superstep, not the whole flow: the defaults must start quietly.
    settings = Settings(_env_file=None, flows={"slow": FlowSpec(target="m:g", timeout_s=600)})
    with caplog.at_level("WARNING", logger=bootstrap.__name__):
        bootstrap._warn_short_grace(settings)
    assert caplog.text == ""


def test_grace_within_the_handover_margin_warns_drain_disabled(
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = Settings(_env_file=None, limits=LimitsSettings(shutdown_grace_s=HANDOVER_MARGIN_S))
    with caplog.at_level("WARNING", logger=bootstrap.__name__):
        bootstrap._warn_short_grace(settings)
    assert "drain disabled" in caplog.text


def test_shutdown_budget_start_is_idempotent() -> None:
    budget = ShutdownBudget(10)
    loop = asyncio.new_event_loop()
    try:
        budget.start(loop)
        first = budget.deadline
        loop.run_until_complete(asyncio.sleep(0.01))
        budget.start(loop)
        assert budget.deadline == first
    finally:
        loop.close()
