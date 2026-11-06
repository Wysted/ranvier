"""Per-run context handed to LangGraph and the build-time context handed to flow factories."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    # Type-only: the public facade imports this module and must not load langgraph eagerly.
    from langchain_core.language_models import BaseChatModel
    from langchain_core.tools import BaseTool
    from langgraph.graph.state import CompiledStateGraph

    from ranvier._internal.agents.spec import AgentSpec


@dataclass(frozen=True, slots=True)
class RunContext:
    """LangGraph `Runtime.context` of every run; data only, not persisted."""

    run_id: str
    thread_id: str
    flow: str
    flow_version: int
    attempt: int  # 1 + number of orphan recoveries of this run
    tenant_id: str | None


class FlowContext(Protocol):
    """Handed to flow factories at build time."""

    @property
    def flow(self) -> str: ...
    @property
    def config(self) -> Mapping[str, Any]: ...  # [flows.<name>.config] from the manifest

    def model(self, profile: str) -> BaseChatModel: ...
    def tools(self, *names: str) -> list[BaseTool]: ...
    async def agent(self, spec: AgentSpec) -> CompiledStateGraph[Any, Any, Any, Any]: ...


@dataclass(frozen=True, slots=True)
class FlowContextImpl:
    """`FlowContext` over injected callables; the container wires the real ones."""

    flow: str
    config: Mapping[str, Any]
    model_fn: Callable[[str], BaseChatModel]
    tools_fn: Callable[..., list[BaseTool]]
    agent_fn: Callable[[AgentSpec], Awaitable[CompiledStateGraph[Any, Any, Any, Any]]]

    def model(self, profile: str) -> BaseChatModel:
        return self.model_fn(profile)

    def tools(self, *names: str) -> list[BaseTool]:
        return self.tools_fn(*names)

    async def agent(self, spec: AgentSpec) -> CompiledStateGraph[Any, Any, Any, Any]:
        return await self.agent_fn(spec)
