import dataclasses
from typing import Any

import pytest

from graph_agent._internal.runtime.context import FlowContextImpl, RunContext


def _ctx(**kw: Any) -> FlowContextImpl:
    base: dict[str, Any] = {
        "flow": "f",
        "config": {"language": "es"},
        "model_fn": lambda profile: f"model:{profile}",
        "tools_fn": lambda *names: list(names),
    }
    return FlowContextImpl(**(base | kw))


def test_model_and_tools_delegate() -> None:
    ctx = _ctx()
    assert str(ctx.model("fast")) == "model:fast"
    assert list[Any](ctx.tools("a", "b")) == ["a", "b"]


async def test_agent_delegates() -> None:
    async def build(spec: object) -> str:
        return f"agent:{spec}"

    assert str(await _ctx(agent_fn=build).agent("s")) == "agent:s"


async def test_agent_without_factory_raises() -> None:
    with pytest.raises(RuntimeError, match="agent factory"):
        await _ctx().agent("s")


def test_run_context_is_frozen() -> None:
    rc = RunContext("r", "t", "f", 1, 1, None)
    with pytest.raises(dataclasses.FrozenInstanceError):
        rc.attempt = 2  # type: ignore[misc]
