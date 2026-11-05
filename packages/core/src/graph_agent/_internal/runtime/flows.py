"""Flow resolution: manifest name -> compiled graph, built lazily once per process."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from langgraph.graph.state import CompiledStateGraph, StateGraph

from graph_agent._internal.config.manifest import FlowSpec, import_target

if TYPE_CHECKING:
    from langgraph.checkpoint.base import BaseCheckpointSaver

    from graph_agent._internal.runtime.context import FlowContext

# shortcut: string codes mirroring design §5/§6 until T13's ErrorCode/RunError (runtime/runs.py) is
# on main (FLOW_NOT_FOUND, FLOW_BUILD_FAILED, FLOW_VERSION_MISMATCH); then delete RunError below,
# import both from runs.py and use ErrorCode members.
FLOW_NOT_FOUND = "flow_not_found"
FLOW_BUILD_FAILED = "flow_build_failed"
FLOW_VERSION_MISMATCH = "flow_version_mismatch"


class RunError(Exception):
    """`{code, message, retryable}`; same shape as T13's `RunError`."""

    def __init__(self, code: str, message: str, retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable


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
            raise RunError(FLOW_NOT_FOUND, f"flows.{name}: unknown flow", retryable=False)
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
    return RunError(FLOW_BUILD_FAILED, message, retryable=True)
