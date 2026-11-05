"""Flow resolution: manifest name -> compiled graph, built lazily once per process."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from langgraph.graph.state import CompiledStateGraph, StateGraph

from graph_agent._internal.config.manifest import FlowSpec, import_target
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


class FlowTargetError(TypeError):
    """The flow target is not a StateGraph (nor a factory returning one)."""


async def to_builder(target: object, ctx: FlowContext) -> StateGraph[Any, Any, Any, Any]:
    """Resolve a flow target (StateGraph or sync/async factory of one) to its uncompiled builder."""
    if not isinstance(target, StateGraph) and callable(target):
        try:
            inspect.signature(target).bind(ctx)
        except ValueError:  # no inspectable signature (builtins): let the call decide
            pass
        except TypeError as exc:
            raise FlowTargetError("target factory must accept one FlowContext argument") from exc
        if inspect.iscoroutinefunction(target):
            target = await target(ctx)
        else:  # sync factory may do I/O: keep it off the loop (blockbuster)
            target = await asyncio.to_thread(target, ctx)
            if inspect.isawaitable(target):  # async __call__ objects, partials
                target = await target
    if isinstance(target, CompiledStateGraph):
        raise FlowTargetError("target is a compiled graph; export the uncompiled StateGraph")
    if not isinstance(target, StateGraph):
        raise FlowTargetError(
            f"target must be a StateGraph or a factory(FlowContext) returning one, "
            f"got {type(target).__name__}"
        )
    return target


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
            builder = await to_builder(target, self._context_factory(name))
            graph = builder.compile(checkpointer=self._checkpointer, name=name)
        except RunError:
            raise
        except FlowTargetError as exc:  # wrong kind of target: our own message, safe to show
            # no cause: validate treats the message as ours
            raise _build_failed(f"flows.{name}: {exc}") from None
        except Exception as exc:  # user import/factory/compile code: fail this run, not the process
            # Client message: key + exception type only (user exceptions may carry secrets).
            raise _build_failed(f"{key}: {type(exc).__name__}") from exc
        return CompiledFlow(
            name, graph, spec.version, spec.durability, spec.timeout_s, spec.max_concurrency
        )


def _build_failed(message: str) -> RunError:
    # Never cached and a redeploy fixes it (design §4): always retryable.
    return RunError(ErrorCode.FLOW_BUILD_FAILED, message, retryable=True)
