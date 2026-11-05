"""Manifest validation for `graph-agent validate`: kept out of `flows` (the per-run hot path)."""

from __future__ import annotations

import asyncio
import traceback
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from langchain_core.language_models import BaseChatModel
from langchain_core.tools import BaseTool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph.state import CompiledStateGraph

from graph_agent._internal.agents.factory import AgentBuildError, build_agent
from graph_agent._internal.agents.mcp import McpSourceError, resolve_headers
from graph_agent._internal.agents.skills import SkillError
from graph_agent._internal.agents.spec import AgentSpec
from graph_agent._internal.agents.tools import ToolConfigError, ToolRegistry
from graph_agent._internal.config.manifest import TargetImportError
from graph_agent._internal.config.settings import Settings
from graph_agent._internal.limits import CpuPool
from graph_agent._internal.llm import LlmError, LlmRegistry
from graph_agent._internal.runtime.flows import FlowRegistry
from graph_agent._internal.runtime.offline import FakeChatModel
from graph_agent._internal.runtime.runs import RunError

if TYPE_CHECKING:
    from graph_agent._internal.runtime.context import FlowContext


@dataclass(frozen=True, slots=True)
class Problem:
    """One validation finding, named by its manifest key (never carries secret values)."""

    key: str
    message: str

    def __str__(self) -> str:
        # Build errors already lead with their key: do not repeat it.
        return self.message if self.message.startswith(self.key) else f"{self.key}: {self.message}"


class _NoMcp:
    """Validation never connects to MCP: every server offers no tools."""

    async def tools(self, server: str) -> list[BaseTool]:
        return []


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
        for server in spec.mcp_servers:
            if server not in self.settings.mcp.servers:
                self._note(f"unknown MCP server {server!r}; add [mcp.servers.{server}]")
        # Unknown names are noted and failing ones are reported under tools.<name>; the rest (and
        # tool objects) go through the real `build_agent`, which resolves them itself.
        self.tools(*(t for t in spec.tools if isinstance(t, str)))
        buildable = [t for t in spec.tools if not isinstance(t, str) or t in self.tool_cache]
        # Resolved first: `build_agent` raises on a collision before it looks at the model, which
        # would hide an unknown profile. `model` notes it and yields a fake offline; no MCP.
        model = self.model(spec.profile)
        return await build_agent(
            replace(spec, tools=buildable),
            models=lambda _p: model,
            registry=self.registry,
            mcp=_NoMcp(),
        )


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
    # These texts are ours (names and paths, no values); any other user exception is described
    # without its values.
    ours = (TargetImportError, AgentBuildError, SkillError, ToolConfigError)
    return Problem(key, str(cause) if isinstance(cause, ours) else _describe(cause))


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
