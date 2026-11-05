"""Tool registry: `[tools.*]` imported lazily and wrapped once with timeout, semaphore, isolation.

Failures (exceptions, timeouts, crashed workers) become `ToolException`s, which the wrapper turns
into error `ToolMessage`s for the model; they never propagate into the run.
"""

import asyncio
from collections.abc import Mapping, Sequence
from typing import Any

from langchain_core.tools import BaseTool, StructuredTool, ToolException

from graph_agent._internal.config.manifest import ToolSpec, import_target
from graph_agent._internal.config.settings import LimitsSettings
from graph_agent._internal.limits import CpuPool


class ToolConfigError(ValueError):
    """Invalid tool declaration; the message names the offending `tools.<name>` key."""


def _has_async(tool: BaseTool) -> bool:
    if isinstance(tool, StructuredTool):
        return tool.coroutine is not None
    return type(tool)._arun is not BaseTool._arun  # default _arun just wraps the sync _run


def _run_in_child(target: str, kwargs: dict[str, Any]) -> Any:
    """Runs inside a CpuPool worker, so it must stay module-level and importable."""
    tool = import_target(target)
    if not isinstance(tool, BaseTool):
        raise TypeError(f"{target!r} is not a langchain tool")
    return asyncio.run(tool.ainvoke(kwargs))


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
        wrapped = self._wrap(name, tool, spec, spec.target)
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
                cached = (item, self._wrap(item.name, item, ToolSpec(target="x:x"), None))
                self._by_object[id(item)] = cached
            out.append(cached[1])
        return out

    def _wrap(self, key: str, tool: BaseTool, spec: ToolSpec, target: str | None) -> BaseTool:
        isolation = spec.isolation
        if isolation == "async" and not _has_async(tool):
            raise ToolConfigError(
                f"tools.{key}.isolation: sync-only tool must declare isolation = 'thread'"
            )
        if isolation == "process" and (target is None or self._cpu_pool is None):
            raise ToolConfigError(
                f"tools.{key}.isolation: 'process' needs a manifest tool and a CPU pool"
            )
        if tool.args_schema is None:
            raise ToolConfigError(f"tools.{key}: tool has no args_schema")
        timeout_s = spec.timeout_s or self._limits.tool_timeout_s
        sem = asyncio.Semaphore(spec.max_concurrency)
        cpu_pool = self._cpu_pool

        async def _call(**kwargs: Any) -> Any:
            try:
                async with sem, asyncio.timeout(timeout_s):
                    if isolation == "async":
                        return await tool.ainvoke(kwargs)
                    if isolation == "thread":  # default executor is the bounded one (T09)
                        return await asyncio.to_thread(tool.invoke, kwargs)
                    assert cpu_pool is not None and target is not None
                    return await cpu_pool.run(_run_in_child, target, kwargs, timeout_s=timeout_s)
            except TimeoutError:
                raise ToolException(f"tool {tool.name!r} timed out after {timeout_s}s") from None
            except ToolException:
                raise
            except Exception as exc:  # error isolation boundary: the model sees it, the run lives
                raise ToolException(f"tool {tool.name!r} failed: {exc!r}") from exc

        return StructuredTool(
            name=tool.name,
            description=tool.description,
            args_schema=tool.args_schema,
            coroutine=_call,
            handle_tool_error=True,
            handle_validation_error=True,
            return_direct=tool.return_direct,
            response_format=tool.response_format,
        )
