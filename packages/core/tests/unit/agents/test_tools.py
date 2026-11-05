import asyncio
import os
import sys
import textwrap
import threading
import time
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated, Any

import pytest
from blockbuster import blockbuster_ctx
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.tools import InjectedToolCallId, StructuredTool, ToolException, tool
from langgraph.graph import START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode
from langgraph.types import Command

from graph_agent._internal.agents.tools import ToolConfigError, ToolRegistry
from graph_agent._internal.config.manifest import ToolSpec
from graph_agent._internal.config.settings import LimitsSettings
from graph_agent._internal.limits import CpuPool

TIMEOUT_S = 0.1
HANG_S = 1.0
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
    assert "RuntimeError: kaput" in msg.content


async def test_tool_internal_timeout_is_a_tool_error_not_a_platform_timeout() -> None:
    @tool
    async def inner_timeout(x: int) -> int:
        """Raises its own TimeoutError."""
        raise TimeoutError("upstream")

    (wrapped,) = _registry().resolve([inner_timeout])
    msg = await wrapped.ainvoke(_tool_call("inner_timeout", x=1))
    assert "timed out" not in msg.content
    assert "TimeoutError: upstream" in msg.content


async def test_failure_is_logged_with_traceback_and_not_repr(
    caplog: pytest.LogCaptureFixture,
) -> None:
    (wrapped,) = _registry().resolve([boom])
    msg = await wrapped.ainvoke(_tool_call("boom", x=1))
    assert "RuntimeError('" not in msg.content
    assert any(r.exc_info for r in caplog.records)


async def test_non_toolcall_input_raises_tool_exception() -> None:
    (wrapped,) = _registry().resolve([boom])
    with pytest.raises(ToolException, match="kaput"):
        await wrapped.ainvoke({"x": 1})


async def _through_toolnode(wrapped: Any, args: dict[str, Any]) -> Any:
    graph = StateGraph(MessagesState)
    graph.add_node("tools", ToolNode([wrapped]))
    graph.add_edge(START, "tools")
    app = graph.compile()
    call = {"name": wrapped.name, "args": args, "id": "call-1"}
    return await app.ainvoke({"messages": [AIMessage("", tool_calls=[call])]})


async def test_toolnode_keeps_artifact() -> None:
    @tool(response_format="content_and_artifact")
    async def with_artifact(x: int) -> tuple[str, dict[str, int]]:
        """Returns an artifact."""
        return "content", {"x": x}

    (wrapped,) = _registry().resolve([with_artifact])
    out = await _through_toolnode(wrapped, {"x": 3})
    msg = out["messages"][-1]
    assert (msg.content, msg.artifact, msg.status) == ("content", {"x": 3}, "success")


async def test_toolnode_injected_tool_call_id_and_command() -> None:
    @tool
    async def cmd(x: int, tool_call_id: Annotated[str, InjectedToolCallId]) -> Command[Any]:
        """Returns a Command."""
        reply = ToolMessage(f"id={tool_call_id}", tool_call_id=tool_call_id)
        return Command(update={"messages": [reply]})

    (wrapped,) = _registry().resolve([cmd])
    out = await _through_toolnode(wrapped, {"x": 1})
    assert out["messages"][-1].content == "id=call-1"


async def test_toolnode_validation_detail_reaches_the_model() -> None:
    (wrapped,) = _registry().resolve([boom])
    out = await _through_toolnode(wrapped, {"x": "not-an-int"})
    msg = out["messages"][-1]
    assert msg.status == "error"
    assert "x" in msg.content
    assert "integer" in msg.content


async def test_inner_handle_tool_error_keeps_error_status() -> None:
    async def _handled(x: int) -> int:
        """Raises ToolException handled by the inner tool."""
        raise ToolException("bad input")

    handled = StructuredTool.from_function(
        coroutine=_handled, name="handled", handle_tool_error=True
    )

    (wrapped,) = _registry().resolve([handled])
    out = await _through_toolnode(wrapped, {"x": 1})
    msg = out["messages"][-1]
    assert (msg.status, msg.content) == ("error", "bad input")


async def test_metadata_preserved() -> None:
    (wrapped,) = _registry().resolve([boom])
    assert (wrapped.name, wrapped.description) == (boom.name, boom.description)
    assert wrapped.args_schema is boom.args_schema
    assert wrapped.return_direct == boom.return_direct


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
    wrapped = reg._wrap("counted", counted, max_concurrency=CONCURRENCY_CAP)
    await asyncio.gather(*(wrapped.ainvoke({"x": i}) for i in range(CALLS)))
    assert peak == CONCURRENCY_CAP


def test_sync_tool_rejected_with_async_isolation(tools_module: str) -> None:
    reg = _registry(sync_only=ToolSpec(target=tools_module))
    with pytest.raises(ToolConfigError, match=r"tools\.sync_only\.isolation"):
        reg.get("sync_only")
    with pytest.raises(ToolConfigError, match="tool object 'sync_only'") as info:
        _registry().resolve([sync_only])
    assert "tools." not in str(info.value)


def test_async_only_tool_rejected_with_thread_isolation() -> None:
    with pytest.raises(ToolConfigError, match=r"async-only"):
        _registry()._wrap("tools.slow", slow, isolation="thread")


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


async def test_thread_timeout_keeps_the_permit_until_the_thread_ends() -> None:
    running = peak = 0
    lock = threading.Lock()

    @tool
    def hang(x: int) -> int:
        """Blocks a thread past the timeout."""
        nonlocal running, peak
        with lock:
            running += 1
            peak = max(peak, running)
        time.sleep(HANG_S)  # blockbuster only checks the loop thread
        with lock:
            running -= 1
        return x

    reg = _registry()
    wrapped = reg._wrap("tools.hang", hang, isolation="thread", max_concurrency=1)
    msgs = [await wrapped.ainvoke(_tool_call("hang", x=1)) for _ in range(5)]
    assert all(m.status == "error" for m in msgs)
    assert peak == 1


WORKER_MODULE = textwrap.dedent(
    """
    import os
    from langchain_core.tools import tool

    @tool
    def worker_pid(x: int) -> int:
        \"\"\"Returns the worker pid.\"\"\"
        return os.getpid()
    """
)


@pytest.fixture
def _allow_pool_blocking() -> Iterator[None]:
    """Overrides conftest: starting/closing forkserver workers blocks the loop by design."""
    with blockbuster_ctx() as bb:
        for func in bb.functions.values():
            func.can_block_in("concurrent/futures/process.py", {"submit", "__init__"})
            func.can_block_in("_internal/limits.py", {"close", "_recycle"})
        yield


@pytest.fixture
def worker_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "ga_worker_tools.py").write_text(WORKER_MODULE)
    monkeypatch.syspath_prepend(tmp_path)  # before start(): workers inherit it when they spawn


@pytest.mark.usefixtures("_allow_pool_blocking", "worker_module")
async def test_process_isolation_runs_in_another_process() -> None:
    pool = CpuPool(1)
    await pool.start()
    try:
        reg = ToolRegistry(
            {
                "worker_pid": ToolSpec(
                    target="ga_worker_tools:worker_pid", isolation="process", timeout_s=20
                )
            },
            LimitsSettings(),
            pool,
        )
        msg = await reg.get("worker_pid").ainvoke(_tool_call("worker_pid", x=1))
        assert msg.status == "success"
        assert int(msg.content) != os.getpid()
    finally:
        pool.close()
