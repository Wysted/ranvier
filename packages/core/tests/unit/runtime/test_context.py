import dataclasses
from typing import Any

import pytest

from graph_agent._internal.agents.spec import AgentSpec
from graph_agent._internal.runtime.context import FlowContextImpl, RunContext

SPEC = AgentSpec(name="a", profile="p")


async def _build(spec: AgentSpec) -> Any:
    return f"agent:{spec.name}"


def _ctx(**kw: Any) -> FlowContextImpl:
    base: dict[str, Any] = {
        "flow": "f",
        "config": {"language": "es"},
        "model_fn": lambda profile: f"model:{profile}",
        "tools_fn": lambda *names: list(names),
        "agent_fn": _build,
    }
    return FlowContextImpl(**(base | kw))


def test_model_and_tools_delegate() -> None:
    ctx = _ctx()
    assert str(ctx.model("fast")) == "model:fast"
    assert list[Any](ctx.tools("a", "b")) == ["a", "b"]


async def test_agent_delegates() -> None:
    assert str(await _ctx().agent(SPEC)) == "agent:a"


def test_run_context_is_frozen() -> None:
    rc = RunContext("r", "t", "f", 1, 1, None)
    with pytest.raises(dataclasses.FrozenInstanceError):
        rc.attempt = 2  # type: ignore[misc]
