"""Flow resolution: manifest name -> compiled graph, built lazily once per process."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from langgraph.graph.state import CompiledStateGraph, StateGraph

from graph_agent._internal.config.manifest import FlowSpec, TargetImportError, import_target

if TYPE_CHECKING:
    from langgraph.checkpoint.base import BaseCheckpointSaver

    from graph_agent._internal.runtime.context import FlowContext

# shortcut: string codes mirroring design §5/§6 until T13's ErrorCode/RunError (runtime/runs.py) is
# on main; then delete RunError below, import both from runs.py and use ErrorCode members.
FLOW_BUILD_FAILED = "flow_build_failed"
FLOW_VERSION_MISMATCH = "flow_version_mismatch"


class RunError(Exception):
    """`{code, message, retryable}`; same shape as T13's `RunError`."""

    def __init__(self, code: str, message: str, retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable


class FlowNotFoundError(LookupError):
    """The flow name is not in the manifest."""


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
            FLOW_VERSION_MISMATCH,
            f"flow {compiled.name!r} is version {compiled.version}, "
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
            raise FlowNotFoundError(f"unknown flow {name!r}")
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
        except TargetImportError as exc:
            raise RunError(FLOW_BUILD_FAILED, f"{key}: {exc}", retryable=True) from exc
        except Exception as exc:  # user factory / compile code: fail this run, not the process
            raise RunError(FLOW_BUILD_FAILED, f"{key}: {exc!r}", retryable=True) from exc
        return CompiledFlow(
            name, graph, spec.version, spec.durability, spec.timeout_s, spec.max_concurrency
        )

    async def _to_builder(
        self, key: str, name: str, target: object
    ) -> StateGraph[Any, Any, Any, Any]:
        if isinstance(target, CompiledStateGraph):
            raise _bad_target(key, "is a compiled graph; export the uncompiled StateGraph")
        if not isinstance(target, StateGraph) and callable(target):
            target = target(self._context_factory(name))
            if inspect.isawaitable(target):
                target = await target
        if not isinstance(target, StateGraph):
            raise _bad_target(
                key,
                "must be a StateGraph or a factory(FlowContext) returning one, "
                f"got {type(target).__name__}",
            )
        return target


def _bad_target(key: str, detail: str) -> RunError:
    # Deterministic misconfiguration: retrying cannot help.
    return RunError(FLOW_BUILD_FAILED, f"{key} {detail}", retryable=False)
