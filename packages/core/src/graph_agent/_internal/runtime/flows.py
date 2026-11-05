"""Flow resolution: manifest name -> compiled graph, built lazily once per process."""

from __future__ import annotations

import asyncio
import inspect
import traceback
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from langchain_core.language_models import BaseChatModel
from langchain_core.tools import BaseTool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import START, MessagesState
from langgraph.graph.state import CompiledStateGraph, StateGraph

from graph_agent._internal.agents.mcp import McpSourceError, resolve_headers
from graph_agent._internal.agents.spec import AgentSpec
from graph_agent._internal.agents.tools import ToolConfigError, ToolRegistry
from graph_agent._internal.config.manifest import FlowSpec, TargetImportError, import_target
from graph_agent._internal.config.settings import Settings
from graph_agent._internal.limits import CpuPool
from graph_agent._internal.llm import LlmError, LlmRegistry
from graph_agent._internal.runtime.offline import FakeChatModel
from graph_agent._internal.runtime.runs import ErrorCode, RunError

if TYPE_CHECKING:
    from langgraph.checkpoint.base import BaseCheckpointSaver

    from graph_agent._internal.runtime.context import FlowContext


@dataclass(frozen=True, slots=True)
class CompiledFlow:
    name: str
    graph: CompiledStateGraph[Any, Any, Any, Any]
    version: int
    durability: Literal["sync", "async", "exit"]
    timeout_s: float
    max_concurrency: int | None


def check_version(compiled: CompiledFlow, recorded_version: int) -> None:
    """Reject a thread recorded with another flow version (persisted contract)."""
    if compiled.version != recorded_version:
        raise RunError(
            ErrorCode.FLOW_VERSION_MISMATCH,
            f"flows.{compiled.name}.version: flow is version {compiled.version}, "
            f"thread was recorded with version {recorded_version}",
        )


class FlowRegistry:
    def __init__(
        self,
        flows: Mapping[str, FlowSpec],
        checkpointer: BaseCheckpointSaver[Any],
        context_factory: Callable[[str], FlowContext],
    ) -> None:
        self._flows = flows
        self._checkpointer = checkpointer
        self._context_factory = context_factory
        self._cache: dict[str, CompiledFlow] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def get(self, name: str) -> CompiledFlow:
        if name not in self._flows:
            raise RunError(ErrorCode.FLOW_NOT_FOUND, f"flows.{name}: unknown flow", retryable=False)
        if (hit := self._cache.get(name)) is not None:
            return hit
        async with self._locks.setdefault(name, asyncio.Lock()):
            if (hit := self._cache.get(name)) is None:  # built while we waited for the lock
                hit = self._cache[name] = await self._build(name, self._flows[name])
        return hit

    async def _build(self, name: str, spec: FlowSpec) -> CompiledFlow:
        key = f"flows.{name}.target"
        try:
            # User module import is sync I/O: keep it off the event loop (blockbuster).
            target = await asyncio.to_thread(import_target, spec.target)
            builder = await self._to_builder(key, name, target)
            graph = builder.compile(checkpointer=self._checkpointer, name=name)
        except RunError:
            raise
        except Exception as exc:  # user import/factory/compile code: fail this run, not the process
            # Client message: key + exception type only (user exceptions may carry secrets).
            raise _build_failed(f"{key}: {type(exc).__name__}") from exc
        return CompiledFlow(
            name, graph, spec.version, spec.durability, spec.timeout_s, spec.max_concurrency
        )

    async def _to_builder(
        self, key: str, name: str, target: object
    ) -> StateGraph[Any, Any, Any, Any]:
        if not isinstance(target, StateGraph) and callable(target):
            if inspect.iscoroutinefunction(target):
                target = await target(self._context_factory(name))
            else:  # sync factory may do I/O: keep it off the loop (blockbuster)
                target = await asyncio.to_thread(target, self._context_factory(name))
                if inspect.isawaitable(target):  # async __call__ objects, partials
                    target = await target
        if isinstance(target, CompiledStateGraph):
            raise _build_failed(f"{key} is a compiled graph; export the uncompiled StateGraph")
        if not isinstance(target, StateGraph):
            raise _build_failed(
                f"{key} must be a StateGraph or a factory(FlowContext) returning one, "
                f"got {type(target).__name__}"
            )
        return target


def _build_failed(message: str) -> RunError:
    # Never cached and a redeploy fixes it (design §4): always retryable.
    return RunError(ErrorCode.FLOW_BUILD_FAILED, message, retryable=True)


@dataclass(frozen=True, slots=True)
class Problem:
    """One validation finding, named by its manifest key (never carries secret values)."""

    key: str
    message: str

    def __str__(self) -> str:
        return f"{self.key}: {self.message}"


@dataclass(slots=True)  # not frozen: FlowContext declares settable attributes
class _ValidationContext:
    """Build-time `FlowContext` for `validate`: cross-references are recorded, never raised, so
    one run reports every problem. Models and tools are pre-built by `validate_manifest` (their
    own failures are reported under `llm.profiles.*` / `tools.*`), so lookups here are cache reads.
    Offline, `models` is empty and every profile yields a fake."""

    flow: str
    config: Mapping[str, Any]
    settings: Settings
    problems: list[Problem]
    models: Mapping[str, BaseChatModel]
    tool_cache: Mapping[str, BaseTool]
    registry: ToolRegistry

    def _note(self, message: str) -> None:
        self.problems.append(Problem(f"flows.{self.flow}.target", message))

    def _check_profile(self, profile: str) -> bool:
        known = profile in self.settings.llm.profiles
        if not known:
            self._note(f"unknown LLM profile {profile!r}; add [llm.profiles.{profile}]")
        return known

    def model(self, profile: str) -> BaseChatModel:
        if self._check_profile(profile) and (model := self.models.get(profile)) is not None:
            return model
        return FakeChatModel()

    def tools(self, *names: str) -> list[BaseTool]:
        found: list[BaseTool] = []
        for name in names:
            if name not in self.settings.tools:
                self._note(f"unknown tool {name!r}; add [tools.{name}]")
            elif (tool := self.tool_cache.get(name)) is not None:
                found.append(tool)  # failing tools are reported under tools.<name>
        return found

    async def agent(self, spec: AgentSpec) -> CompiledStateGraph[Any, Any, Any, Any]:
        self._check_profile(spec.profile)
        self.tools(*(t for t in spec.tools if isinstance(t, str)))
        try:  # tool objects take the real wrapping path; names were checked above
            self.registry.resolve([t for t in spec.tools if not isinstance(t, str)])
        except ToolConfigError as exc:
            self._note(str(exc))
        for server in spec.mcp_servers:
            if server not in self.settings.mcp.servers:
                self._note(f"unknown MCP server {server!r}; add [mcp.servers.{server}]")
        # shortcut: a one-node stand-in, no create_agent; build the real agent offline once the
        # agent factory (T30) exists.
        builder = StateGraph(MessagesState)
        builder.add_node("agent", lambda state: {})
        builder.add_edge(START, "agent")
        return builder.compile(name=spec.name)


def _describe(exc: BaseException) -> str:
    """Safe one-liner for a user exception: type and location, never values (may be secrets)."""
    frames = traceback.extract_tb(exc.__traceback__)
    where = f" at {frames[-1].filename}:{frames[-1].lineno}" if frames else ""
    return f"{type(exc).__name__}{where}"


def _build_problem(name: str, exc: RunError) -> Problem:
    cause = exc.__cause__
    if cause is None:  # our own message (wrong kind, compiled graph)
        return Problem(f"flows.{name}", exc.message)
    key = f"flows.{name}.target"
    # TargetImportError text is ours; any other user exception is described without its values.
    return Problem(key, str(cause) if isinstance(cause, TargetImportError) else _describe(cause))


async def validate_manifest(
    settings: Settings,
    *,
    offline: bool,
    on_diagram: Callable[[str, str], None] | None = None,
) -> list[Problem]:
    """Check the whole manifest without a database or MCP connection; collect every problem.

    Names, versions, crons and URLs are already enforced by the `Settings` models. Here: imports,
    object kinds, factory builds, compile against `InMemorySaver` and cross-references. Tools go
    through the real `ToolRegistry` (not started), so its rules are never reimplemented.
    """
    problems: list[Problem] = []
    llm = None if offline else LlmRegistry(settings.llm)
    cpu_pool = CpuPool(1)  # never started: workers spawn lazily, `process` tools just need a pool
    registry = ToolRegistry(settings.tools, settings.limits, cpu_pool=cpu_pool)
    try:
        tools: dict[str, BaseTool] = {}
        for name in settings.tools:
            try:
                tools[name] = await asyncio.to_thread(registry.get, name)
            except (KeyError, ToolConfigError, TargetImportError) as exc:
                problems.append(Problem(f"tools.{name}", str(exc)))
        models: dict[str, BaseChatModel] = {}
        if llm is not None:
            for profile in settings.llm.profiles:
                try:
                    models[profile] = await asyncio.to_thread(llm.model, profile)
                except Exception as exc:  # provider constructors raise anything; report, go on
                    detail = str(exc) if isinstance(exc, LlmError) else _describe(exc)
                    problems.append(Problem(f"llm.profiles.{profile}", detail))
            for server, spec in settings.mcp.servers.items():
                try:
                    resolve_headers(server, spec)
                except McpSourceError as exc:
                    problems.append(Problem(f"mcp.servers.{server}.headers_env", str(exc)))

        def context(flow: str) -> FlowContext:
            return _ValidationContext(
                flow, settings.flows[flow].config, settings, problems, models, tools, registry
            )

        flows = FlowRegistry(settings.flows, InMemorySaver(), context)
        for name in settings.flows:
            try:
                compiled = await flows.get(name)
            except RunError as exc:
                problems.append(_build_problem(name, exc))
                continue
            if on_diagram is not None:
                on_diagram(name, compiled.graph.get_graph().draw_mermaid())
    finally:
        cpu_pool.close()
        if llm is not None:
            await llm.aclose()
    return problems
