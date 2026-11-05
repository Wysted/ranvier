"""Tool registry: `[tools.*]` imported lazily and wrapped once with timeout, semaphore, isolation.

`_LimitedTool` delegates `ainvoke` to the original tool, so injected arguments (`ToolRuntime`,
`InjectedState`, `InjectedToolCallId`), artifacts, `Command` results and the original error
handling keep working. Pydantic validation errors propagate so `ToolNode` can show the model the
detailed message. Any other failure (exception, timeout, crashed worker) becomes an error
`ToolMessage` (or a `ToolException` when not called with a `ToolCall`); it never reaches the run.

The semaphore wait does not count against `timeout_s` (same as the MCP port): the timeout bounds
the execution only. `thread` isolation holds the permit until the thread really finishes: a
timed-out thread cannot be stopped, so releasing earlier would let hung tools pile up threads.

`process` isolation sends the tool input to the worker pickled and re-imports the tool there:
injected or unpicklable arguments (state, runtime) fail and are reported as a tool error.
"""

import asyncio
import contextvars
import functools
import logging
from collections.abc import Mapping, Sequence
from typing import Any, Literal

from langchain_core.messages import ToolMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool, StructuredTool, ToolException
from langgraph.errors import GraphBubbleUp
from pydantic import PrivateAttr, ValidationError

from graph_agent._internal.config.manifest import (
    DEFAULT_TOOL_MAX_CONCURRENCY,
    ToolSpec,
    import_target,
)
from graph_agent._internal.config.settings import LimitsSettings
from graph_agent._internal.limits import CpuPool

logger = logging.getLogger(__name__)

MAX_ERROR_CHARS = 200
POOL_GRACE = 2
Isolation = Literal["async", "thread", "process"]


class ToolConfigError(ValueError):
    """Invalid tool declaration; the message names the offending tool."""


def _has_async(tool: BaseTool) -> bool:
    if isinstance(tool, StructuredTool):
        return tool.coroutine is not None
    return type(tool)._arun is not BaseTool._arun  # default _arun just wraps the sync _run


def _has_sync(tool: BaseTool) -> bool:
    # shortcut: only StructuredTool is checked; BaseTool subclasses always define `_run`
    return tool.func is not None if isinstance(tool, StructuredTool) else True


def _run_in_child(target: str, tool_input: Any) -> Any:
    """Runs inside a CpuPool worker, so it must stay module-level and importable."""
    tool = import_target(target)
    if not isinstance(tool, BaseTool):
        raise TypeError(f"{target!r} is not a langchain tool")
    return asyncio.run(tool.ainvoke(tool_input))


def _is_tool_call(tool_input: Any) -> bool:
    return isinstance(tool_input, dict) and tool_input.get("type") == "tool_call"


class _LimitedTool(BaseTool):
    inner: BaseTool
    timeout_s: float
    isolation: Isolation
    target: str | None = None
    _sem: asyncio.Semaphore = PrivateAttr()
    _cpu_pool: CpuPool | None = PrivateAttr(default=None)

    def __init__(
        self,
        inner: BaseTool,
        *,
        timeout_s: float,
        max_concurrency: int,
        isolation: Isolation,
        target: str | None,
        cpu_pool: CpuPool | None,
    ) -> None:
        super().__init__(
            name=inner.name,
            description=inner.description,
            args_schema=inner.args_schema,
            return_direct=inner.return_direct,
            response_format=inner.response_format,
            inner=inner,
            timeout_s=timeout_s,
            isolation=isolation,
            target=target,
        )
        self._cpu_pool = cpu_pool
        self._sem = asyncio.Semaphore(max_concurrency)

    def _run(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("platform tools are async only; use ainvoke")

    def _release_after_thread(self, fut: "asyncio.Future[Any]") -> None:
        self._sem.release()
        if not fut.cancelled():
            fut.exception()  # consume it: nobody awaits a timed-out thread any more

    async def ainvoke(self, input: Any, config: RunnableConfig | None = None, **kwargs: Any) -> Any:
        await self._sem.acquire()
        release = True
        try:
            async with asyncio.timeout(self.timeout_s) as cm:
                if self.isolation == "async":
                    return await self.inner.ainvoke(input, config, **kwargs)
                if self.isolation == "thread":
                    # A timed-out thread cannot be stopped: the permit is released by the
                    # done-callback, when the thread really ends, not when we stop waiting.
                    ctx = contextvars.copy_context()
                    call = functools.partial(self.inner.invoke, input, config, **kwargs)
                    fut = asyncio.get_running_loop().run_in_executor(None, ctx.run, call)
                    fut.add_done_callback(self._release_after_thread)
                    release = False
                    return await asyncio.shield(fut)
                assert self._cpu_pool is not None and self.target is not None
                # the pool's own timeout is a backstop, so ours always fires first
                return await self._cpu_pool.run(
                    _run_in_child, self.target, input, timeout_s=self.timeout_s * POOL_GRACE
                )
        except TimeoutError as exc:
            if cm.expired():  # a tool's own internal TimeoutError is a plain tool error
                err = f"tool {self.name!r} timed out after {self.timeout_s}s"
                logger.warning(err)
                return self._error(input, err)
            return self._failed(input, exc)
        except (ValidationError, GraphBubbleUp):
            raise  # ToolNode builds the model-facing validation error; interrupts must bubble
        except Exception as exc:  # error isolation boundary: the model sees it, the run lives
            return self._failed(input, exc)
        finally:
            if release:
                self._sem.release()

    def _failed(self, tool_input: Any, exc: Exception) -> Any:
        logger.warning("tool %r failed", self.name, exc_info=exc)
        # never repr(exc): it may carry URLs or tokens
        detail = str(exc)[:MAX_ERROR_CHARS]
        return self._error(tool_input, f"tool {self.name!r} failed: {type(exc).__name__}: {detail}")

    def _error(self, tool_input: Any, message: str) -> Any:
        if _is_tool_call(tool_input):
            return ToolMessage(
                message, tool_call_id=tool_input["id"], name=self.name, status="error"
            )
        raise ToolException(message)


class ToolRegistry:
    def __init__(
        self,
        tools_settings: Mapping[str, ToolSpec],
        limits: LimitsSettings,
        cpu_pool: CpuPool | None = None,
    ) -> None:
        self._settings = tools_settings
        self._limits = limits
        self._cpu_pool = cpu_pool
        self._by_name: dict[str, BaseTool] = {}
        # Keeps the source object alive so id() stays unique; objects are wrapped once too.
        self._by_object: dict[int, tuple[BaseTool, BaseTool]] = {}

    def get(self, name: str) -> BaseTool:
        if (wrapped := self._by_name.get(name)) is not None:
            return wrapped
        spec = self._settings.get(name)
        if spec is None:
            raise KeyError(f"unknown tool {name!r}: not declared under [tools.{name}]")
        tool = import_target(spec.target)
        if not isinstance(tool, BaseTool):
            raise ToolConfigError(f"tools.{name}.target {spec.target!r} is not a langchain tool")
        wrapped = self._wrap(
            f"tools.{name}",
            tool,
            isolation=spec.isolation,
            timeout_s=spec.timeout_s,
            max_concurrency=spec.max_concurrency,
            target=spec.target,
        )
        self._by_name[name] = wrapped
        return wrapped

    def resolve(self, items: Sequence[str | BaseTool]) -> list[BaseTool]:
        out: list[BaseTool] = []
        for item in items:
            if isinstance(item, str):
                out.append(self.get(item))
                continue
            cached = self._by_object.get(id(item))
            if cached is None:
                cached = (item, self._wrap(f"tool object {item.name!r}", item))
                self._by_object[id(item)] = cached
            out.append(cached[1])
        return out

    def _wrap(
        self,
        where: str,
        tool: BaseTool,
        *,
        isolation: Isolation = "async",
        timeout_s: float | None = None,
        max_concurrency: int = DEFAULT_TOOL_MAX_CONCURRENCY,
        target: str | None = None,
    ) -> BaseTool:
        """`where` labels the tool in errors: `tools.<name>` for manifest tools, else the object."""
        key = f"{where}.isolation" if target else where
        if isolation == "async" and not _has_async(tool):
            raise ToolConfigError(f"{key}: a sync-only tool must declare isolation = 'thread'")
        if isolation == "thread" and not _has_sync(tool):
            raise ToolConfigError(f"{key}: an async-only tool cannot use isolation = 'thread'")
        if isolation == "process" and (target is None or self._cpu_pool is None):
            raise ToolConfigError(f"{key}: 'process' needs a manifest tool and a CPU pool")
        return _LimitedTool(
            tool,
            timeout_s=timeout_s or self._limits.tool_timeout_s,
            max_concurrency=max_concurrency,
            isolation=isolation,
            target=target,
            cpu_pool=self._cpu_pool,
        )
