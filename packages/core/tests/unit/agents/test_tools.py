import asyncio
import sys
import threading
import types
from collections.abc import Iterator

import pytest
from langchain_core.tools import tool

from graph_agent._internal.agents.tools import ToolConfigError, ToolRegistry
from graph_agent._internal.config.manifest import ToolSpec
from graph_agent._internal.config.settings import LimitsSettings

TIMEOUT_S = 0.1
CONCURRENCY_CAP = 2
CALLS = 6


def _registry(**specs: ToolSpec) -> ToolRegistry:
    return ToolRegistry(specs, LimitsSettings(tool_timeout_s=TIMEOUT_S))


def _tool_call(name: str, **args: object) -> dict[str, object]:
    return {"type": "tool_call", "name": name, "args": args, "id": "1"}


@tool
async def slow(x: int) -> int:
    """Sleeps forever."""
    await asyncio.sleep(10)
    return x


@tool
async def boom(x: int) -> int:
    """Always fails."""
    raise RuntimeError("kaput")


@tool
def sync_only(x: int) -> str:
    """Reports the executing thread."""
    return threading.current_thread().name


async def test_timeout_becomes_error_message() -> None:
    (wrapped,) = _registry().resolve([slow])
    msg = await wrapped.ainvoke(_tool_call("slow", x=1))
    assert msg.status == "error"
    assert "timed out" in msg.content


async def test_exception_becomes_error_message() -> None:
    (wrapped,) = _registry().resolve([boom])
    msg = await wrapped.ainvoke(_tool_call("boom", x=1))
    assert msg.status == "error"
    assert "kaput" in msg.content


async def test_metadata_preserved() -> None:
    (wrapped,) = _registry().resolve([boom])
    assert (wrapped.name, wrapped.description) == (boom.name, boom.description)
    assert wrapped.args_schema is boom.args_schema


async def test_concurrency_cap() -> None:
    running = peak = 0

    @tool
    async def counted(x: int) -> int:
        """Tracks concurrent calls."""
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.01)
        running -= 1
        return x

    reg = ToolRegistry({}, LimitsSettings())
    wrapped = reg._wrap(
        "counted", counted, ToolSpec(target="x:x", max_concurrency=CONCURRENCY_CAP), None
    )
    await asyncio.gather(*(wrapped.ainvoke({"x": i}) for i in range(CALLS)))
    assert peak == CONCURRENCY_CAP


def test_sync_tool_rejected_with_async_isolation() -> None:
    with pytest.raises(ToolConfigError, match=r"tools\.sync_only\.isolation"):
        _registry().resolve([sync_only])


@pytest.fixture
def tools_module() -> Iterator[str]:
    mod = types.ModuleType("fake_tools_mod")
    mod.sync_only = sync_only  # type: ignore[attr-defined]
    sys.modules["fake_tools_mod"] = mod
    yield "fake_tools_mod:sync_only"
    del sys.modules["fake_tools_mod"]


async def test_thread_isolation_runs_off_loop_and_get_caches(tools_module: str) -> None:
    reg = _registry(sync_only=ToolSpec(target=tools_module, isolation="thread"))
    wrapped = reg.get("sync_only")
    assert reg.get("sync_only") is wrapped
    msg = await wrapped.ainvoke(_tool_call("sync_only", x=1))
    assert msg.status == "success"
    assert msg.content != threading.current_thread().name


def test_unknown_tool_fails_loudly() -> None:
    with pytest.raises(KeyError, match=r"tools\.nope"):
        _registry().get("nope")
