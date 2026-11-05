"""Public test kit (design section 11): run a flow without a database or a real model.

`run_flow` drives the same `stream_graph` core as the real runner over an `InMemorySaver`, so
events and interrupts map exactly as in production. It applies no run timeout: bound tests with
`asyncio.timeout`.
"""

from __future__ import annotations

import math
import uuid
from collections.abc import Callable, Mapping, Sequence
from contextlib import aclosing
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.tools import BaseTool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph.state import StateGraph
from langgraph.types import Command, Interrupt

from graph_agent._internal.agents.factory import build_agent
from graph_agent._internal.agents.mcp import McpToolSource
from graph_agent._internal.agents.spec import AgentSpec
from graph_agent._internal.agents.tools import Isolation, ToolRegistry, has_async
from graph_agent._internal.config.settings import LimitsSettings
from graph_agent._internal.runtime.context import FlowContext, FlowContextImpl, RunContext
from graph_agent._internal.runtime.events import (
    CustomEvent,
    EndEvent,
    ErrorEvent,
    InterruptEvent,
    InterruptItem,
    MessagesEvent,
    MetadataEvent,
    RunEvent,
    UpdatesEvent,
    UsageTotals,
    ValuesEvent,
)
from graph_agent._internal.runtime.flows import CompiledFlow, to_builder
from graph_agent._internal.runtime.offline import FakeChatModel as _OfflineFakeChatModel
from graph_agent._internal.runtime.runner import (
    CLIENT_STREAM_MODES,
    check_thread_command,
    is_resume_map,
    stream_graph,
)
from graph_agent._internal.runtime.runs import RunError

if TYPE_CHECKING:
    from langchain_core.runnables import RunnableConfig
    from langgraph.graph.state import CompiledStateGraph

__all__ = [
    "CustomEvent",
    "EndEvent",
    "ErrorEvent",
    "FakeChatModel",
    "FlowContext",
    "FlowResult",
    "InterruptEvent",
    "InterruptItem",
    "MessagesEvent",
    "MetadataEvent",
    "RunEvent",
    "UpdatesEvent",
    "UsageTotals",
    "ValuesEvent",
    "flow_context",
    "run_flow",
]

FLOW_NAME = "test"
FLOW_VERSION = 1
# Every client mode, so tests can assert on any event.
_STREAM_MODES = sorted(CLIENT_STREAM_MODES)


class FakeChatModel(_OfflineFakeChatModel):
    """Replies with `responses` in order, repeating the last one; `calls` records what it got.

    A scripted `AIMessage` may carry `tool_calls`; `bind_tools` is a no-op. Because the last reply
    repeats, end a tool-calling script with a plain reply or an agent loops on the same call.
    """

    def __init__(self, responses: Sequence[AIMessage | str]) -> None:
        super().__init__(responses=responses)


@dataclass(frozen=True, slots=True)
class FlowResult:
    values: dict[str, Any]  # final state of the thread
    interrupts: tuple[Interrupt, ...]  # pending interrupts, empty when the flow finished
    events: tuple[RunEvent, ...]


@dataclass(frozen=True, slots=True)
class _MemoryContext(FlowContextImpl):
    """`FlowContext` plus the saver that makes a second `run_flow` resume the same thread."""

    saver: InMemorySaver = field(default_factory=InMemorySaver)


def flow_context(
    *,
    models: Mapping[str, BaseChatModel] | None = None,
    tools: Mapping[str, BaseTool] | None = None,
    config: Mapping[str, Any] | None = None,
    mcp: McpToolSource | None = None,
) -> FlowContext:
    """Build-time context for a flow: `models` and `tools` are looked up by profile / name."""
    models = models or {}
    tools = tools or {}

    def model(profile: str) -> BaseChatModel:
        if profile not in models:
            raise KeyError(f"no model for profile {profile!r}; pass flow_context(models=...)")
        return models[profile]

    def lookup(*names: str) -> list[BaseTool]:
        if missing := [n for n in names if n not in tools]:
            raise KeyError(f"no tool named {missing}; pass flow_context(tools=...)")
        return [tools[n] for n in names]

    # By object identity: a same-named tool passed directly in an AgentSpec is not the kit's.
    sync_only: dict[int, Isolation] = {id(t): "thread" for t in tools.values() if not has_async(t)}

    async def agent(spec: AgentSpec) -> CompiledStateGraph[Any, Any, Any, Any]:
        # Names resolve against `tools`; the empty registry only wraps the tool objects. A sync-only
        # tool gets isolation 'thread', the same rule a user configures under `[tools.*]`.
        resolved = [
            t for item in spec.tools for t in (lookup(item) if isinstance(item, str) else [item])
        ]
        return await build_agent(
            replace(spec, tools=resolved),
            models=model,
            registry=ToolRegistry({}, LimitsSettings(), object_isolation=sync_only),
            mcp=mcp,
        )

    return _MemoryContext(FLOW_NAME, dict(config or {}), model, lookup, agent)


async def _resume_command(
    graph: CompiledStateGraph[Any, Any, Any, Any], config: RunnableConfig, resume: Any
) -> Command[Any]:
    """Same rule as the CLI and API: `check_thread_command` over the thread's pending ids."""
    pending = [i.id for i in (await graph.aget_state(config)).interrupts]
    try:
        check_thread_command(pending, input=None, resume=resume)
    except RunError as exc:
        raise ValueError(exc.message) from exc
    return Command(resume=dict(resume) if is_resume_map(resume) else resume)


async def run_flow(
    target: StateGraph[Any, Any, Any, Any] | Callable[[FlowContext], Any],
    input: Any = None,  # name fixed by the public API (design section 11)
    *,
    ctx: FlowContext | None = None,
    thread_id: str | None = None,
    resume: Any = None,
) -> FlowResult:
    """Run `target` (a `StateGraph` or a sync/async factory of one) once over an `InMemorySaver`.

    Reuse the same `ctx` and `thread_id` with `resume=` to continue an interrupted thread.
    """
    ctx = ctx or flow_context()
    if not isinstance(ctx, _MemoryContext):
        raise TypeError("ctx must come from graph_agent.testing.flow_context()")
    if input is not None and resume is not None:
        raise ValueError("pass input or resume, not both")
    builder = await to_builder(target, ctx)
    graph = builder.compile(checkpointer=ctx.saver, name=FLOW_NAME)
    thread_id = thread_id or str(uuid.uuid4())
    run_id = str(uuid.uuid4())
    config: RunnableConfig = {
        "configurable": {"thread_id": thread_id},
        "metadata": {"run_id": run_id},
    }
    payload = input if resume is None else await _resume_command(graph, config, resume)

    run_ctx = RunContext(run_id, thread_id, FLOW_NAME, FLOW_VERSION, 1, None)
    # shortcut: no run timeout, bound tests with asyncio.timeout; add one if users hang CI
    compiled = CompiledFlow(FLOW_NAME, graph, FLOW_VERSION, "sync", math.inf, None)
    stream = stream_graph(compiled, payload, config, run_ctx, _STREAM_MODES)
    async with aclosing(stream):
        events = tuple([event async for event in stream])
    state = await graph.aget_state(config)
    return FlowResult(dict(state.values), tuple(state.interrupts), events)
