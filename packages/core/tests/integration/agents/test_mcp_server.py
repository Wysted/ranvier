"""Adapter against an in-process MCPServer from the same SDK; loopback only, no external network."""

import asyncio
import socket
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any

import anyio
import httpx2
import pytest
import uvicorn
from langchain_core.messages import ToolMessage
from mcp import Client, MCPError
from mcp.server.mcpserver import MCPServer
from mcp_types import INVALID_REQUEST, CallToolResult

from ranvier._internal.agents import mcp as mcp_module
from ranvier._internal.agents.mcp import McpSourceError, open_mcp_sources
from ranvier._internal.config.manifest import McpServerSpec

# Explicit port: DNS-rebinding protection rejects a Host header without one (S2).
ASGI_URL = "http://127.0.0.1:8000/mcp"
TOKEN_ENV = "T29_TEST_TOKEN"
SECRET = "s3cr3t-token-value"


def make_server(name: str = "t") -> MCPServer:
    srv = MCPServer(name, log_level="CRITICAL")

    @srv.tool(name="echo")
    async def echo(text: str) -> str:
        return text

    @srv.tool(name="slow")
    async def slow(seconds: float) -> str:
        await anyio.sleep(seconds)
        return "done"

    @srv.tool(name="boom")
    async def boom() -> str:
        raise RuntimeError("boom from tool")

    @srv.tool(name="bad_output")
    async def bad_output() -> dict[str, int]:
        # Bypasses server-side conversion: structuredContent violates the advertised outputSchema.
        return CallToolResult(content=[], structured_content={"n": "x"})  # type: ignore[return-value]

    return srv


def call(tool_name: str, args: dict[str, Any]) -> dict[str, Any]:
    return {"type": "tool_call", "id": "c1", "name": tool_name, "args": args}


def spec(url: str = ASGI_URL, **kw: Any) -> McpServerSpec:
    return McpServerSpec(url=url, **kw)


@asynccontextmanager
async def asgi(srv: MCPServer) -> AsyncIterator[httpx2.ASGITransport]:
    app = srv.streamable_http_app()  # must exist before session_manager
    async with srv.session_manager.run():  # ASGITransport sends no lifespan events
        yield httpx2.ASGITransport(app=app)


async def test_list_call_error_and_concurrent_failure() -> None:
    async with asgi(make_server()) as transport, AsyncExitStack() as stack:
        source = await open_mcp_sources({"t": spec()}, stack, transport=transport)
        tools = {t.name: t for t in await source.tools("t")}
        assert set(tools) == {"echo", "slow", "boom", "bad_output"}
        assert await source.tools("t") is await source.tools("t")  # cached

        ok = await tools["echo"].ainvoke(call("echo", {"text": "hi"}))
        assert isinstance(ok, ToolMessage)
        assert ok.status == "success"
        assert ok.content_blocks[0]["type"] == "text"

        results = await asyncio.gather(
            *(tools["slow"].ainvoke(call("slow", {"seconds": 0.2})) for _ in range(5)),
            tools["boom"].ainvoke(call("boom", {})),
        )
        assert [r.status for r in results] == ["success"] * 5 + ["error"]


async def test_timeout_becomes_error_message() -> None:
    async with asgi(make_server()) as transport, AsyncExitStack() as stack:
        source = await open_mcp_sources({"t": spec(timeout_s=0.3)}, stack, transport=transport)
        slow = next(t for t in await source.tools("t") if t.name == "slow")
        msg = await slow.ainvoke(call("slow", {"seconds": 5}))
        assert msg.status == "error"
        assert "timed out" in msg.content


async def test_invalid_structured_content_becomes_error_message() -> None:
    async with asgi(make_server()) as transport, AsyncExitStack() as stack:
        source = await open_mcp_sources({"t": spec()}, stack, transport=transport)
        bad = next(t for t in await source.tools("t") if t.name == "bad_output")
        msg = await bad.ainvoke(call("bad_output", {}))
        assert msg.status == "error"
        assert "bad_output" in msg.content


async def test_call_after_close_fails_without_reconnect_and_leaks_no_tasks() -> None:
    before = asyncio.all_tasks()
    async with asgi(make_server()) as transport:
        async with AsyncExitStack() as stack:
            source = await open_mcp_sources({"t": spec()}, stack, transport=transport)
            echo = next(t for t in await source.tools("t") if t.name == "echo")
            assert (await echo.ainvoke(call("echo", {"text": "a"}))).status == "success"
        with pytest.raises(McpSourceError, match="closed"):
            await echo.ainvoke(call("echo", {"text": "b"}))
    leaked = [t for t in asyncio.all_tasks() - before if t is not asyncio.current_task()]
    assert [t.get_name() for t in leaked if t.get_name().startswith("mcp-owner")] == []


async def test_unreachable_server_listing_raises_port_error() -> None:
    async with AsyncExitStack() as stack:
        source = await open_mcp_sources(
            {"t": spec(f"http://127.0.0.1:{free_port()}/mcp", timeout_s=2)}, stack
        )
        with pytest.raises(McpSourceError, match="'t' unavailable"):
            await source.tools("t")


async def test_legacy_stale_session_burst_causes_exactly_one_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with asgi(make_server()) as transport, AsyncExitStack() as stack:
        source = await open_mcp_sources({"t": spec(max_concurrency=6)}, stack, transport=transport)
        echo = next(t for t in await source.tools("t") if t.name == "echo")
        srv = source._servers["t"]  # type: ignore[attr-defined]
        starts = 0
        real_start = srv._start

        async def counting_start() -> None:
            nonlocal starts
            starts += 1
            await real_start()

        async def stale(self: Any, *args: Any, **kwargs: Any) -> None:
            await asyncio.sleep(0.05)  # let the whole burst obtain the stale client first
            raise MCPError(INVALID_REQUEST, "stale session")

        monkeypatch.setattr(srv, "_start", counting_start)
        monkeypatch.setattr(mcp_module, "MODERN_PROTOCOL_VERSIONS", frozenset())  # force legacy
        with monkeypatch.context() as m:
            m.setattr(Client, "call_tool", stale)
            burst = await asyncio.gather(
                *(echo.ainvoke(call("echo", {"text": str(i)})) for i in range(6))
            )
        assert [r.status for r in burst] == ["error"] * 6  # failed calls are never retried
        assert starts == 0  # the drop is lazy: nothing reconnected yet
        after = await asyncio.gather(
            *(echo.ainvoke(call("echo", {"text": str(i)})) for i in range(6))
        )
        assert [r.status for r in after] == ["success"] * 6
        assert starts == 1


async def test_unknown_server_and_missing_env_fail_naming_the_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(TOKEN_ENV, raising=False)
    async with AsyncExitStack() as stack:
        source = await open_mcp_sources({"t": spec()}, stack)
        with pytest.raises(McpSourceError, match="unknown MCP server"):
            await source.tools("nope")


@pytest.mark.parametrize("value", [None, "", "   "])
async def test_missing_or_blank_header_env_fails_at_open(
    monkeypatch: pytest.MonkeyPatch, value: str | None
) -> None:
    if value is None:
        monkeypatch.delenv(TOKEN_ENV, raising=False)
    else:
        monkeypatch.setenv(TOKEN_ENV, value)
    async with AsyncExitStack() as stack:
        with pytest.raises(McpSourceError, match=TOKEN_ENV) as err:
            await open_mcp_sources({"t": spec(headers_env={"Authorization": TOKEN_ENV})}, stack)
        assert repr(value) not in str(err.value) or value is None


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@asynccontextmanager
async def uvicorn_server(port: int) -> AsyncIterator[None]:
    config = uvicorn.Config(
        make_server().streamable_http_app(),
        host="127.0.0.1",
        port=port,
        log_level="critical",
        timeout_graceful_shutdown=1,
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:
        if task.done():
            task.result()
        await asyncio.sleep(0.01)
    try:
        yield
    finally:
        server.should_exit = True
        await task


async def test_reconnects_after_server_restart_without_leaking_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(TOKEN_ENV, SECRET)
    port = free_port()
    url = f"http://127.0.0.1:{port}/mcp"
    async with AsyncExitStack() as stack:
        source = await open_mcp_sources(
            {"t": spec(url, headers_env={"Authorization": TOKEN_ENV})}, stack
        )
        async with uvicorn_server(port):
            echo = next(t for t in await source.tools("t") if t.name == "echo")
            assert (await echo.ainvoke(call("echo", {"text": "a"}))).status == "success"
        # Server down: an error message, not an exception; this task survives.
        down = await echo.ainvoke(call("echo", {"text": "b"}))
        assert down.status == "error"
        assert SECRET not in down.content
        async with uvicorn_server(port):
            after = await echo.ainvoke(call("echo", {"text": "c"}))
            assert after.status == "success"
