"""MCP port and its adapter over the `mcp` SDK v2 (Streamable HTTP only; design §7, S2).

One `mcp.Client` per server, entered and held by its own owner task: a transport error cancels
the task that entered the Client (python-sdk #1393), so it must never be a caller or the
composition root. The root only registers `close` on its `AsyncExitStack`.
"""

import asyncio
import importlib
import logging
import os
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import AsyncExitStack
from http import HTTPStatus
from typing import Any, Protocol

import httpx2  # noqa: TID251
from langchain_core.messages.content import (
    create_audio_block,
    create_file_block,
    create_image_block,
    create_text_block,
)
from langchain_core.tools import BaseTool, StructuredTool, ToolException
from mcp import Client, MCPError  # noqa: TID251
from mcp.client import InputRequiredRoundsExceededError  # noqa: TID251
from mcp.client.streamable_http import streamable_http_client  # noqa: TID251
from mcp_types import (  # noqa: TID251
    INVALID_REQUEST,
    AudioContent,
    CallToolResult,
    EmbeddedResource,
    ImageContent,
    ResourceLink,
    TextContent,
    TextResourceContents,
)
from mcp_types.version import MODERN_PROTOCOL_VERSIONS  # noqa: TID251
from pydantic import ValidationError

from graph_agent._internal.config.manifest import McpServerSpec

_log = logging.getLogger(__name__)

# shortcut: eager import because mcp imports jsonschema lazily on the first call_tool (sync I/O
# inside the loop); remove when mcp imports it at load time.
importlib.import_module("jsonschema")

SSE_READ_TIMEOUT_S = 300.0  # same as the SDK's own client: idle server-to-client streams
REAP_TIMEOUT_S = 5.0  # grace for an owner task to exit before it is cancelled
DEFAULT_MIME = "application/octet-stream"


class McpSourceError(Exception):
    """Misconfiguration or collision detected by the MCP adapter (never carries secret values)."""


class McpToolSource(Protocol):
    async def tools(self, server: str) -> Sequence[BaseTool]: ...


class _ConnectErrorAs502(httpx2.AsyncBaseTransport):
    """Turns a failure before any response into a synthetic 502.

    The SDK maps non-2xx to a request-scoped `MCPError`, which keeps a connect failure out of the
    transport task group (S2). It cannot help once a response has started; the owner task
    covers that.
    """

    def __init__(self, inner: httpx2.AsyncBaseTransport) -> None:
        self._inner = inner

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        try:
            return await self._inner.handle_async_request(request)
        except httpx2.TransportError as exc:
            return httpx2.Response(HTTPStatus.BAD_GATEWAY, request=request, text=type(exc).__name__)

    async def aclose(self) -> None:
        await self._inner.aclose()


class _Server:
    """One MCP server: owner task + Client, reconnect under a lock, semaphore, tool cache."""

    def __init__(
        self,
        name: str,
        spec: McpServerSpec,
        headers: dict[str, str],
        transport: httpx2.AsyncBaseTransport | None,
    ) -> None:
        self.name = name
        self._spec = spec
        self._headers = headers
        self._closed = False
        self._inner_transport = transport
        self._http: httpx2.AsyncClient | None = None
        self._client: Client | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._lock = asyncio.Lock()  # serializes restarts so siblings share one new Client
        self.sem = asyncio.Semaphore(spec.max_concurrency)
        self.tools: list[BaseTool] | None = None

    # ---- lifecycle (all under self._lock except close)

    async def _http_client(self) -> httpx2.AsyncClient:
        if self._http is None:
            # Lazy imports inside httpx2 read package metadata (sync I/O): keep them off the loop.
            inner = self._inner_transport
            if inner is None:
                inner = await asyncio.to_thread(httpx2.AsyncHTTPTransport)
            self._http = httpx2.AsyncClient(
                headers=self._headers,
                timeout=httpx2.Timeout(
                    self._spec.timeout_s, read=max(self._spec.timeout_s, SSE_READ_TIMEOUT_S)
                ),
                transport=_ConnectErrorAs502(inner),
            )
        return self._http

    def _alive(self) -> bool:
        return self._client is not None and self._task is not None and not self._task.done()

    async def _start(self) -> None:
        http = await self._http_client()
        ready: asyncio.Future[Client] = asyncio.get_running_loop().create_future()
        stop = self._stop = asyncio.Event()
        task = self._task = asyncio.create_task(
            self._own(http, ready, stop), name=f"mcp-owner-{self.name}"
        )
        try:
            self._client = await ready
        except BaseException:
            task.cancel()  # a hung connect must not leak its owner
            await self._reap()
            raise

    async def _own(
        self, http: httpx2.AsyncClient, ready: asyncio.Future[Client], stop: asyncio.Event
    ) -> None:
        try:
            async with Client(
                streamable_http_client(self._spec.url, http_client=http), mode="auto"
            ) as client:
                ready.set_result(client)
                await stop.wait()
        except BaseException as exc:  # tell the starter how connecting died, then die
            if not ready.done():
                ready.set_exception(exc)
            raise

    async def _reap(self) -> None:
        """Stop the owner (if any) and swallow how it died: the caller decides what to do."""
        task, self._task, self._client = self._task, None, None
        if task is None:
            return
        self._stop.set()
        _, pending = await asyncio.wait({task}, timeout=REAP_TIMEOUT_S)
        if pending:  # stuck in teardown: never let it hang shutdown
            task.cancel()
            await asyncio.wait({task}, timeout=REAP_TIMEOUT_S)
        if task.done() and not task.cancelled() and (exc := task.exception()) is not None:
            _log.warning("MCP owner for %r died: %s", self.name, _describe(exc))

    async def _live_client(self) -> Client:
        async with self._lock:
            if self._closed:
                raise McpSourceError(f"MCP server {self.name!r} is closed")
            if not self._alive():
                await self._reap()
                await self._start()
            assert self._client is not None
            return self._client

    async def _drop(self, client: Client) -> None:
        async with self._lock:
            if self._client is client:  # a sibling may already have replaced it
                await self._reap()

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            await self._reap()
            if self._http is not None:
                await self._http.aclose()
                self._http = None

    # ---- operations

    async def run[T](self, op: Callable[[Client], Awaitable[T]]) -> T:
        """Run `op` on a live Client. The failed call is never retried (may not be idempotent)."""
        client = await self._live_client()
        legacy = client.protocol_version not in MODERN_PROTOCOL_VERSIONS
        try:
            return await op(client)
        except MCPError as exc:
            # Handshake-era servers answer a stale session id with INVALID_REQUEST: reconnect next.
            if legacy and exc.code == INVALID_REQUEST:
                await self._drop(client)
            raise

    async def list_tools(self) -> list[BaseTool]:
        """Paginated listing, cached after the first success."""
        if self.tools is None:
            async with asyncio.timeout(self._spec.timeout_s):  # includes connect and handshake
                self.tools = await self.run(self._list_all)
        return self.tools

    async def _list_all(self, client: Client) -> list[BaseTool]:
        out: list[BaseTool] = []
        cursor: str | None = None
        while True:
            page = await client.list_tools(cursor=cursor)
            out.extend(
                self._tool(t.name, t.description or t.name, t.input_schema) for t in page.tools
            )
            if page.next_cursor is None:
                return out
            cursor = page.next_cursor

    def _tool(self, name: str, description: str, schema: dict[str, Any]) -> StructuredTool:
        async def call(**arguments: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
            try:
                async with self.sem, asyncio.timeout(self._spec.timeout_s):
                    result = await self.run(lambda c: c.call_tool(name, arguments))
            except TimeoutError:
                raise ToolException(
                    f"MCP tool {name!r} on {self.name!r} timed out after {self._spec.timeout_s}s"
                ) from None
            except (
                MCPError,
                InputRequiredRoundsExceededError,
                ExceptionGroup,
                httpx2.HTTPError,
                RuntimeError,  # SDK: structuredContent violates the tool's outputSchema
                ValidationError,
            ) as exc:
                raise ToolException(
                    f"MCP tool {name!r} on {self.name!r} failed: {_describe(exc)}"
                ) from None
            return _map_result(name, result)

        return StructuredTool(
            name=name,
            description=description,
            args_schema=schema,
            coroutine=call,
            response_format="content_and_artifact",
            handle_tool_error=True,  # ToolException -> error ToolMessage, not a graph failure
        )


def _describe(exc: BaseException) -> str:
    """Error text without echoing request data: server-provided MCP message, else the type name."""
    if isinstance(exc, MCPError):
        return f"{exc.error.message} (code {exc.code})"
    if isinstance(exc, ExceptionGroup):
        return ", ".join(_describe(e) for e in exc.exceptions)
    if isinstance(exc, InputRequiredRoundsExceededError):
        return "server requested input (InputRequiredResult), unsupported in v0.1"
    return type(exc).__name__


def _meta(kind: str, **fields: Any) -> dict[str, Any]:
    return {"type": kind, **{k: v for k, v in fields.items() if v is not None}}


def _map_result(tool: str, result: CallToolResult) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Design §7 'MCP results': media go to content blocks; the artifact keeps metadata only."""
    blocks: list[dict[str, Any]] = []
    resources: list[dict[str, Any]] = []
    for block in result.content:
        match block:
            case TextContent():
                blocks.append(dict(create_text_block(block.text)))
            case ImageContent():
                blocks.append(
                    dict(create_image_block(base64=block.data, mime_type=block.mime_type))
                )
                resources.append(_meta("image", mime_type=block.mime_type))
            case AudioContent():
                blocks.append(
                    dict(create_audio_block(base64=block.data, mime_type=block.mime_type))
                )
                resources.append(_meta("audio", mime_type=block.mime_type))
            case ResourceLink():
                blocks.append(dict(create_text_block(f"{block.name}: {block.uri}")))
                resources.append(
                    _meta(
                        "resource_link",
                        uri=block.uri,
                        name=block.name,
                        mime_type=block.mime_type,
                        size=block.size,
                    )
                )
            case EmbeddedResource():
                res = block.resource
                if isinstance(res, TextResourceContents):
                    blocks.append(dict(create_text_block(res.text)))
                else:
                    mime = res.mime_type or DEFAULT_MIME
                    make = create_image_block if mime.startswith("image/") else create_file_block
                    blocks.append(dict(make(base64=res.blob, mime_type=mime)))
                resources.append(_meta("resource", uri=res.uri, mime_type=res.mime_type))
    if result.is_error:
        text = " ".join(b["text"] for b in blocks if b["type"] == "text")
        raise ToolException(text or f"MCP tool {tool!r} reported an error")
    return blocks, {"structured_content": result.structured_content, "resources": resources}


class _Source:
    def __init__(self, servers: dict[str, _Server]) -> None:
        self._servers = servers

    async def tools(self, server: str) -> Sequence[BaseTool]:
        try:
            srv = self._servers[server]
        except KeyError:
            raise McpSourceError(f"unknown MCP server {server!r}") from None
        # Cross-server name collisions are checked per agent by T30, not here (order-dependent).
        try:
            return await srv.list_tools()
        except (MCPError, ExceptionGroup, httpx2.HTTPError, TimeoutError) as exc:
            # No mcp/httpx2 type crosses the port.
            raise McpSourceError(f"MCP server {server!r} unavailable: {_describe(exc)}") from None

    async def close(self) -> None:
        for srv in self._servers.values():
            await srv.close()


def _resolve_headers(name: str, spec: McpServerSpec) -> dict[str, str]:
    """Header values from the environment; never echoes a value, only the variable name."""
    headers: dict[str, str] = {}
    for header, env_name in spec.headers_env.items():
        value = os.environ.get(env_name)
        if value is None or not value.strip():
            raise McpSourceError(
                f"MCP server {name!r}: env var {env_name} (header {header}) is missing or empty"
            )
        headers[header] = value
    return headers


async def open_mcp_sources(
    servers: Mapping[str, McpServerSpec],
    stack: AsyncExitStack,
    *,
    transport: httpx2.AsyncBaseTransport | None = None,
) -> McpToolSource:
    """Build the source; headers are resolved now, connections open lazily on first use.

    `stack` closes them. `transport` is a test seam (in-process ASGI instead of the network).
    """
    source = _Source(
        {
            name: _Server(name, spec, _resolve_headers(name, spec), transport)
            for name, spec in servers.items()
        }
    )
    stack.push_async_callback(source.close)
    return source
