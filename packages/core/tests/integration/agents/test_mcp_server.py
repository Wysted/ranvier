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
from mcp.server.mcpserver import MCPServer

from graph_agent._internal.agents.mcp import McpSourceError, open_mcp_sources
from graph_agent._internal.config.manifest import McpServerSpec

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
        assert set(tools) == {"echo", "slow", "boom"}
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


async def test_name_collision_across_servers_fails() -> None:
    # One shared transport: both specs point at the same in-process server, so names collide.
    async with asgi(make_server()) as transport, AsyncExitStack() as stack:
        source = await open_mcp_sources({"a": spec(), "b": spec()}, stack, transport=transport)
        await source.tools("a")
        with pytest.raises(McpSourceError, match="both 'a' and 'b'"):
            await source.tools("b")


async def test_unknown_server_and_missing_env_fail_naming_the_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(TOKEN_ENV, raising=False)
    async with AsyncExitStack() as stack:
        source = await open_mcp_sources(
            {"t": spec(headers_env={"Authorization": TOKEN_ENV})}, stack
        )
        with pytest.raises(McpSourceError, match="unknown MCP server"):
            await source.tools("nope")
        with pytest.raises(McpSourceError, match=TOKEN_ENV):
            await source.tools("t")


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
