from __future__ import annotations

import asyncio
import sys
import types
from collections.abc import Iterator
from typing import Any, TypedDict

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph

from graph_agent._internal.config.manifest import FlowSpec
from graph_agent._internal.runtime.flows import (
    FLOW_BUILD_FAILED,
    FLOW_VERSION_MISMATCH,
    FlowNotFoundError,
    FlowRegistry,
    RunError,
    check_version,
)

MODULE = "t14_user_flows"


class State(TypedDict):
    x: int


def _builder() -> StateGraph[State, Any, State, State]:
    g = StateGraph(State)
    g.add_node("n", lambda s: {"x": s["x"] + 1})
    g.add_edge(START, "n")
    g.add_edge("n", END)
    return g


@pytest.fixture
def user_module() -> Iterator[Any]:
    mod = types.ModuleType(MODULE)
    sys.modules[MODULE] = mod
    yield mod
    del sys.modules[MODULE]


def _registry(**targets: str) -> FlowRegistry:
    flows = {n: FlowSpec(target=f"{MODULE}:{t}", version=3) for n, t in targets.items()}
    ctx: Any = object()  # factories under test ignore the context
    ctx_factory: Any = lambda name: ctx  # noqa: E731
    return FlowRegistry(flows, InMemorySaver(), ctx_factory)


async def test_builder_sync_and_async_factory(user_module: Any) -> None:
    async def afactory(ctx: Any) -> Any:
        return _builder()

    user_module.builder = _builder()
    user_module.sync_factory = lambda ctx: _builder()
    user_module.async_factory = afactory
    reg = _registry(a="builder", b="sync_factory", c="async_factory")
    for name in "abc":
        flow = await reg.get(name)
        assert flow.version == 3 and flow.name == name
        cfg: Any = {"configurable": {"thread_id": name}}
        assert await flow.graph.ainvoke({"x": 1}, cfg) == {"x": 2}


@pytest.mark.parametrize("kind", ["compiled", "other"])
async def test_bad_target_is_loud_and_not_retryable(user_module: Any, kind: str) -> None:
    user_module.t = _builder().compile() if kind == "compiled" else 42
    with pytest.raises(RunError, match=r"flows\.f\.target") as ei:
        await _registry(f="t").get("f")
    assert ei.value.code == FLOW_BUILD_FAILED and not ei.value.retryable


async def test_failed_build_not_cached_then_succeeds(user_module: Any) -> None:
    calls = 0

    def factory(ctx: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("boom")
        return _builder()

    user_module.t = factory
    reg = _registry(f="t")
    with pytest.raises(RunError) as ei:
        await reg.get("f")
    assert ei.value.code == FLOW_BUILD_FAILED and ei.value.retryable
    first = await reg.get("f")
    assert first is await reg.get("f") and calls == 2


async def test_missing_module_is_build_failure() -> None:
    with pytest.raises(RunError) as ei:
        await _registry(f="nope").get("f")
    assert ei.value.retryable


async def test_concurrent_get_builds_once(user_module: Any) -> None:
    calls = 0

    async def factory(ctx: Any) -> Any:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return _builder()

    user_module.t = factory
    reg = _registry(f="t")
    results = await asyncio.gather(*(reg.get("f") for _ in range(5)))
    assert calls == 1 and all(r is results[0] for r in results)


async def test_unknown_flow_and_version_check(user_module: Any) -> None:
    user_module.t = _builder()
    reg = _registry(f="t")
    with pytest.raises(FlowNotFoundError):
        await reg.get("zzz")
    flow = await reg.get("f")
    check_version(flow, 3)
    with pytest.raises(RunError) as ei:
        check_version(flow, 2)
    assert ei.value.code == FLOW_VERSION_MISMATCH
