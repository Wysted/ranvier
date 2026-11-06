"""S2 spike: mcp SDK v2 Client session reuse, reconnect, failure isolation, content blocks.

Throwaway. Run from the repo root:
    uv run python bench/spikes/S2/s2.py [reuse|reconnect|isolation|hosting|content ...]
No argument runs every group (about 40 s).
Everything is local: an MCPServer from the same SDK, served by uvicorn on 127.0.0.1
or through httpx2.ASGITransport. No external servers, no secrets.
"""

# ruff: noqa: BLE001  # broad catches are deliberate probes: every failure shape is a finding

from __future__ import annotations

import asyncio
import base64
import collections
import json
import socket
import sys
import time
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from http import HTTPStatus
from importlib.metadata import version
from typing import Any

import anyio
import httpx2
import uvicorn
from mcp import Client, MCPError
from mcp.client.streamable_http import streamable_http_client
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.utilities.types import Image
from mcp_types import (
    INVALID_REQUEST,
    BlobResourceContents,
    CallToolResult,
    EmbeddedResource,
    ImageContent,
    TextContent,
    TextResourceContents,
)
from mcp_types.version import MODERN_PROTOCOL_VERSIONS

MODES = ("auto", "legacy")
CALL_GUARD_S = 5.0  # every client call is bounded so a hang shows up as a finding, not a stuck script
N_SEQ = 50
N_CONC = 50
N_SIBLINGS = 8
SIBLING_SLEEP_S = 1.0
PNG_1PX_B64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
HTTP_FAIL_TOOL = "http_fail"  # the middleware answers this tool call with a non-2xx status (python-sdk #2604)
TRUNCATED = -1  # http_fail status meaning "200 with a truncated body"
# The app auto-enables DNS-rebinding protection for host 127.0.0.1 with allowed_hosts "127.0.0.1:*":
# the Host header needs an explicit port (httpx2 drops a default :80), else 421 Invalid Host header.
ASGI_BASE = "http://127.0.0.1:8000"


# ---------------------------------------------------------------- server side
@dataclass
class Stats:
    tool_calls: int = 0
    rpc_methods: list[str] = field(default_factory=list)
    http_methods: list[str] = field(default_factory=list)
    session_ids: set[str] = field(default_factory=set)
    client_ports: set[int] = field(default_factory=set)
    auth_headers: set[str] = field(default_factory=set)
    protocol_versions: set[str] = field(default_factory=set)
    errors: list[str] = field(
        default_factory=list
    )  # server responses >= 400 (status + body head)

    def handshakes(self) -> int:
        return sum(m in ("initialize", "server/discover") for m in self.rpc_methods)

    def summary(self) -> str:
        return (
            f"tool_calls={self.tool_calls} handshakes={self.handshakes()} "
            f"session_ids={len(self.session_ids)} tcp_conns={len(self.client_ports)} "
            f"protocol={sorted(self.protocol_versions)} http={dict(collections.Counter(self.http_methods))} "
            f"rpc={dict(collections.Counter(self.rpc_methods))}"
            + (f" server_errors={self.errors[:3]}" if self.errors else "")
        )


def make_server(stats: Stats) -> MCPServer:
    srv = MCPServer(
        "s2", log_level="CRITICAL"
    )  # tool errors are expected here; keep the output readable

    @srv.tool()
    async def echo(text: str) -> str:
        stats.tool_calls += 1
        return text

    @srv.tool()
    async def slow(seconds: float) -> str:
        stats.tool_calls += 1
        await anyio.sleep(seconds)
        return f"slept {seconds}"

    @srv.tool()
    async def boom() -> str:
        stats.tool_calls += 1
        raise RuntimeError("boom from tool")

    @srv.tool(name=HTTP_FAIL_TOOL)
    async def http_fail(
        status: int,
    ) -> str:  # never reached: the middleware answers first
        return "unreachable"

    @srv.tool()
    def media() -> list[Any]:
        return [
            TextContent(text="caption"),
            # A bare Image helper inside a returned list raised PydanticSerializationError in 2.3.0: convert it.
            Image(data=base64.b64decode(PNG_1PX_B64), format="png").to_image_content(),
            EmbeddedResource(
                resource=TextResourceContents(
                    uri="file:///notes.txt", text="note", mime_type="text/plain"
                )
            ),
            EmbeddedResource(
                resource=BlobResourceContents(
                    uri="file:///px.png", blob=PNG_1PX_B64, mime_type="image/png"
                )
            ),
        ]

    @srv.tool()
    def structured(n: int) -> dict[str, int]:
        return {"n": n, "double": n * 2}

    return srv


def instrument(app: Any, stats: Stats) -> Any:
    """ASGI wrapper: records each HTTP request and answers HTTP_FAIL_TOOL calls with a non-2xx status."""

    async def wrapped(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await app(scope, receive, send)
            return
        stats.http_methods.append(scope["method"])
        headers = {
            k.decode("latin-1"): v.decode("latin-1") for k, v in scope["headers"]
        }
        if (sid := headers.get("mcp-session-id")) is not None:
            stats.session_ids.add(sid)
        if (pv := headers.get("mcp-protocol-version")) is not None:
            stats.protocol_versions.add(pv)
        if (auth := headers.get("authorization")) is not None:
            stats.auth_headers.add(auth)
        if scope.get("client"):
            stats.client_ports.add(scope["client"][1])
        if scope["method"] != "POST":
            await app(scope, receive, send)
            return
        chunks: list[bytes] = []
        more = True
        while more:
            msg = await receive()
            chunks.append(msg.get("body", b""))
            more = msg.get("more_body", False)
        body = b"".join(chunks)
        status = None
        rpc = json.loads(body)
        for item in rpc if isinstance(rpc, list) else [rpc]:
            if "method" in item:
                stats.rpc_methods.append(item["method"])
                params = item.get("params") or {}
                if (
                    item["method"] == "tools/call"
                    and params.get("name") == HTTP_FAIL_TOOL
                ):
                    status = params["arguments"]["status"]
        if status == TRUNCATED:
            # 200 + declared length larger than the body: uvicorn drops the connection, the client
            # sees a transport-level error (not an HTTP status) on this one request.
            hdrs = [
                (b"content-type", b"application/json"),
                (b"content-length", b"1000"),
            ]
            await send({"type": "http.response.start", "status": 200, "headers": hdrs})
            await send({"type": "http.response.body", "body": b'{"jsonrpc": "2.0"'})
            return
        if status is not None:
            await send(
                {
                    "type": "http.response.start",
                    "status": status,
                    "headers": [(b"content-type", b"text/plain")],
                }
            )
            await send({"type": "http.response.body", "body": b"injected failure"})
            return
        replayed = False

        async def replay() -> dict[str, Any]:
            nonlocal replayed
            if replayed:
                return await receive()
            replayed = True
            return {"type": "http.request", "body": body, "more_body": False}

        failed: list[int] = []

        async def tap(msg: dict[str, Any]) -> None:
            if (
                msg["type"] == "http.response.start"
                and msg["status"] >= HTTPStatus.BAD_REQUEST
            ):
                failed.append(msg["status"])
            elif msg["type"] == "http.response.body" and failed:
                stats.errors.append(f"{failed[0]} {msg.get('body', b'')[:160]!r}")
            await send(msg)

        await app(scope, replay, tap)

    return wrapped


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@asynccontextmanager
async def uvicorn_server(
    port: int, stats: Stats, **app_kw: Any
) -> AsyncIterator[uvicorn.Server]:
    app = instrument(make_server(stats).streamable_http_app(**app_kw), stats)
    config = uvicorn.Config(
        app,
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
        yield server
    finally:
        server.should_exit = True
        await task


# ---------------------------------------------------------------- client side
def http_client(**kw: Any) -> httpx2.AsyncClient:
    # Same timeouts as the SDK's own client (create_mcp_http_client); httpx2's default is 5 s.
    return httpx2.AsyncClient(timeout=httpx2.Timeout(30.0, read=300.0), **kw)


def new_client(url: str, mode: str, hc: httpx2.AsyncClient) -> Client:
    return Client(streamable_http_client(url, http_client=hc), mode=mode)


async def guarded[T](aw: Awaitable[T], limit: float = CALL_GUARD_S) -> T:
    with anyio.fail_after(limit):
        return await aw


def describe(exc: BaseException) -> str:
    if isinstance(exc, MCPError):
        return f"MCPError(code={exc.code}, message={exc.error.message!r})"
    if isinstance(exc, BaseExceptionGroup):
        return f"{type(exc).__name__}[{', '.join(describe(e) for e in exc.exceptions)}]"
    return f"{type(exc).__module__}.{type(exc).__name__}({str(exc)[:100]!r})"


async def outcome(aw: Awaitable[Any]) -> str:
    try:
        r = await aw
    except (
        BaseException
    ) as exc:  # spike probe: report every failure shape, including cancellation
        if isinstance(exc, KeyboardInterrupt):
            raise
        return "raised " + describe(exc)
    if isinstance(r, CallToolResult):
        texts = " ".join(b.text for b in r.content if isinstance(b, TextContent))
        return f"ok is_error={r.is_error} text={texts[:60]!r}"
    return f"ok {type(r).__name__}"


class Log(list[str]):
    """Prints each line as it is recorded, so a crash still shows the steps that ran."""

    def append(self, s: str) -> None:
        print("    .. " + s, flush=True)
        super().append(s)


RESULTS: list[tuple[str, str]] = []


def record(check: str, verdict: str, detail: str) -> None:
    RESULTS.append((check, verdict))
    print(f"[{verdict}] {check}\n    {detail}")


# ---------------------------------------------------------------- checks
async def check_reuse(mode: str) -> None:
    """One Client entered once: many sequential and concurrent calls, one handshake, at most one session."""
    stats, port = Stats(), free_port()
    url = f"http://127.0.0.1:{port}/mcp"
    async with (
        uvicorn_server(port, stats),
        http_client() as hc,
        new_client(url, mode, hc) as client,
    ):
        handshakes_after_enter = stats.handshakes()
        await client.list_tools()
        for i in range(N_SEQ):
            assert text_ok(
                await guarded(client.call_tool("echo", {"text": str(i)}))
            ) == str(i)
        t0 = time.perf_counter()
        conc = await asyncio.gather(
            *(
                guarded(client.call_tool("slow", {"seconds": 0.2}))
                for _ in range(N_CONC)
            )
        )
        wall = time.perf_counter() - t0
        proto = client.protocol_version
    ok = (
        stats.tool_calls == N_SEQ + N_CONC
        and all(not r.is_error for r in conc)
        and stats.handshakes() == handshakes_after_enter == 1
        and len(stats.session_ids) <= 1
        and wall < 1.0  # 50 x 0.2 s ran concurrently, not serialized
    )
    record(
        f"reuse mode={mode}",
        "PASS" if ok else "FAIL",
        f"negotiated={proto} concurrent_wall={wall:.2f}s {stats.summary()}",
    )


def text_ok(r: CallToolResult) -> str:
    assert not r.is_error, r
    return " ".join(b.text for b in r.content if isinstance(b, TextContent))


async def check_host_cancel(mode: str) -> None:
    """A transport error cancels the task that entered the Client (here: a stand-in composition root)."""
    port = free_port()
    url = f"http://127.0.0.1:{port}/mcp"
    lines: list[str] = Log()
    async with http_client() as hc:
        srv = uvicorn_server(port, Stats())
        await srv.__aenter__()
        ready: asyncio.Future[Client] = asyncio.get_running_loop().create_future()

        async def root() -> str:
            t0 = time.perf_counter()
            try:
                async with new_client(url, mode, hc) as client:
                    ready.set_result(client)
                    await asyncio.sleep(10)  # "serving"
            except BaseException as exc:  # spike probe: report how the root died
                return f"died after {time.perf_counter() - t0:.2f}s of a 10s sleep: {describe(exc)}"
            return "finished normally"

        host = asyncio.create_task(root())
        client = await ready
        lines.append(
            "call before stop: "
            + await outcome(guarded(client.call_tool("echo", {"text": "a"})))
        )
        await srv.__aexit__(None, None, None)
        lines.append(
            "call from another task while down: "
            + await outcome(guarded(client.call_tool("echo", {"text": "b"})))
        )
        lines.append("root task: " + await guarded(host, CALL_GUARD_S * 3))
    # PASS = the hazard is reproduced (the entering task dies), which is what T29 must design around.
    record(
        f"host-task cancel mode={mode}",
        "PASS" if "died" in lines[-1] else "FAIL",
        "\n    ".join(lines),
    )


class OwnedClient:
    """One mcp.Client per server, entered in its own task (T29 shape; spike-only).

    The transport's task group belongs to the task that enters the Client, and an uncaught
    transport error in it cancels that task. A dedicated owner task keeps that away from callers
    and the composition root. A dead owner (or a terminated legacy session) is replaced on the
    next call; the failed call is not retried because tool calls are not assumed idempotent.
    shortcut: no lock around concurrent restarts, T29 adds one (asyncio.Lock per server).
    """

    def __init__(self, url: str, mode: str, hc: httpx2.AsyncClient) -> None:
        self.url, self.mode, self.hc = url, mode, hc
        self.client: Client | None = None
        self.restarts = 0
        self.deaths: list[str] = []
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        ready: asyncio.Future[Client] = asyncio.get_running_loop().create_future()
        self._stop = asyncio.Event()
        self._task = asyncio.create_task(self._own(ready, self._stop))
        self.client = await ready

    async def _own(self, ready: asyncio.Future[Client], stop: asyncio.Event) -> None:
        try:
            async with new_client(self.url, self.mode, self.hc) as client:
                ready.set_result(client)
                await stop.wait()
        except (
            BaseException
        ) as exc:  # the owner reports how it died; the caller decides
            if not ready.done():
                ready.set_exception(exc)
            raise

    @property
    def alive(self) -> bool:
        return self._task is not None and not self._task.done()

    async def close(self) -> None:
        if self._task is None:
            return
        self._stop.set()
        await asyncio.wait({self._task})
        task, self._task, self.client = self._task, None, None
        if task.cancelled():
            self.deaths.append("owner cancelled")
        elif (exc := task.exception()) is not None:
            self.deaths.append(describe(exc))

    async def call_tool(
        self, name: str, args: dict[str, Any] | None = None, **kw: Any
    ) -> CallToolResult:
        if not self.alive:
            await self.close()
            await self.start()
            self.restarts += 1
        assert self.client is not None
        legacy = self.client.protocol_version not in MODERN_PROTOCOL_VERSIONS
        try:
            return await guarded(self.client.call_tool(name, args, **kw))
        except MCPError as exc:
            # A legacy server answers a stale mcp-session-id with 404: the transport maps it to
            # INVALID_REQUEST ("Session terminated", or the server's own body, e.g. "Session not
            # found"). No dedicated error class, so match on the code; next call opens a new session.
            if legacy and exc.code == INVALID_REQUEST:
                await self.close()
            raise


async def check_reconnect(mode: str) -> None:
    """Server restart on the same port: what surfaces, does the same Client recover, minimal recovery."""
    port = free_port()
    url = f"http://127.0.0.1:{port}/mcp"
    s2 = Stats()
    lines: list[str] = Log()
    async with http_client() as hc:
        oc = OwnedClient(url, mode, hc)
        srv1 = uvicorn_server(port, Stats())
        await srv1.__aenter__()
        await oc.start()
        lines.append(
            "before restart: " + await outcome(oc.call_tool("echo", {"text": "a"}))
        )
        inflight = asyncio.create_task(outcome(oc.call_tool("slow", {"seconds": 3})))
        await asyncio.sleep(0.3)
        await srv1.__aexit__(None, None, None)
        lines.append("in-flight during shutdown: " + await inflight)
        await asyncio.sleep(0.2)
        lines.append(f"owner alive after shutdown: {oc.alive}")
        lines.append(
            "call while down: " + await outcome(oc.call_tool("echo", {"text": "b"}))
        )
        await asyncio.sleep(0.2)
        lines.append(f"owner alive after that call: {oc.alive}")
        lines.append(
            "call while down #2: " + await outcome(oc.call_tool("echo", {"text": "b2"}))
        )
        srv2 = uvicorn_server(port, s2)
        await srv2.__aenter__()
        after = [
            await outcome(oc.call_tool("echo", {"text": f"c{i}"})) for i in range(3)
        ]
        lines.append("after restart: " + " | ".join(after))
        # Quick restart: no call while the server is down, so the transport never sees a ConnectError.
        await srv2.__aexit__(None, None, None)
        srv2 = uvicorn_server(port, Stats())
        await srv2.__aenter__()
        quick = [
            await outcome(oc.call_tool("echo", {"text": f"q{i}"})) for i in range(3)
        ]
        lines.append(
            "quick restart, same owner: "
            + " | ".join(quick)
            + f" restarts={oc.restarts}"
        )
        await oc.close()
        lines.append(f"restarts={oc.restarts} deaths={oc.deaths}")
        # A Client cannot be re-entered and its transport (an @asynccontextmanager) is single-use.
        reused = Client(streamable_http_client(url, http_client=hc), mode=mode)
        async with reused:
            pass
        lines.append("re-enter an exited Client: " + await outcome(reused.__aenter__()))
        await srv2.__aexit__(None, None, None)
    verdict = (
        "PASS"
        if all(o[-1].startswith("ok is_error=False") for o in (after, quick))
        else "FAIL"
    )
    record(
        f"reconnect mode={mode}",
        verdict,
        "\n    ".join(lines) + f"\n    server2: {s2.summary()}",
    )


async def check_transport_isolation(mode: str) -> None:
    """Server up, one request dies at the HTTP transport layer (truncated body): do siblings survive?"""
    port = free_port()
    url = f"http://127.0.0.1:{port}/mcp"
    async with uvicorn_server(port, Stats()), http_client() as hc:
        oc = OwnedClient(url, mode, hc)
        await oc.start()
        await oc.client.list_tools()  # type: ignore[union-attr]
        jobs = [
            oc.call_tool("slow", {"seconds": SIBLING_SLEEP_S})
            for _ in range(N_SIBLINGS)
        ]
        jobs.append(oc.call_tool(HTTP_FAIL_TOOL, {"status": TRUNCATED}))
        outs = await asyncio.gather(*(outcome(j) for j in jobs))
        await asyncio.sleep(0.2)
        alive = oc.alive
        after = await outcome(oc.call_tool("echo", {"text": "next"}))
        await oc.close()
    siblings = collections.Counter(o.split("(")[0] for o in outs[:-1])
    ok = all(o.startswith("ok is_error=False") for o in outs[:-1])
    record(
        f"transport-error isolation mode={mode}",
        "PASS" if ok else "FAIL",
        f"failing: {outs[-1]}\n    siblings: {dict(siblings)}\n    owner alive after: {alive}"
        f"\n    next call: {after} restarts={oc.restarts}\n    deaths={oc.deaths}",
    )


class ConnectErrorAs502(httpx2.AsyncBaseTransport):
    """Turns a failure before any response (connect/write) into a synthetic 502.

    The SDK maps non-2xx to a request-scoped MCPError (the python-sdk #2604 fix), so this keeps a
    connection failure out of the transport task group. It cannot help once a response has
    started (a body cut mid-stream is raised while the SDK reads it).
    """

    def __init__(self) -> None:
        self.inner = httpx2.AsyncHTTPTransport()

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        try:
            return await self.inner.handle_async_request(request)
        except httpx2.TransportError as exc:
            return httpx2.Response(
                HTTPStatus.BAD_GATEWAY,
                request=request,
                text=f"{type(exc).__name__}: {exc}",
            )

    async def aclose(self) -> None:
        await self.inner.aclose()


async def check_connect_error_wrapper(mode: str) -> None:
    """Same Client across a restart with ConnectErrorAs502: is the root task spared, does it self-recover?"""
    port = free_port()
    url = f"http://127.0.0.1:{port}/mcp"
    lines: list[str] = Log()
    async with http_client(transport=ConnectErrorAs502()) as hc:
        srv = uvicorn_server(port, Stats())
        await srv.__aenter__()
        ready: asyncio.Future[Client] = asyncio.get_running_loop().create_future()
        stop = asyncio.Event()

        async def root() -> str:
            try:
                async with new_client(url, mode, hc) as client:
                    ready.set_result(client)
                    await stop.wait()
            except BaseException as exc:  # spike probe
                return "died: " + describe(exc)
            return "finished normally"

        host = asyncio.create_task(root())
        client = await ready
        lines.append(
            "before: " + await outcome(guarded(client.call_tool("echo", {"text": "a"})))
        )
        await srv.__aexit__(None, None, None)
        lines.append(
            "while down: "
            + await outcome(guarded(client.call_tool("echo", {"text": "b"})))
        )
        await asyncio.sleep(0.2)
        lines.append(f"root alive while down: {not host.done()}")
        srv = uvicorn_server(port, Stats())
        await srv.__aenter__()
        after = [
            await outcome(guarded(client.call_tool("echo", {"text": f"c{i}"})))
            for i in range(2)
        ]
        lines.append("same client after restart: " + " | ".join(after))
        stop.set()
        lines.append("root: " + await guarded(host))
        await srv.__aexit__(None, None, None)
    ok = (
        "root alive while down: True" in lines
        and lines[-1] == "root: finished normally"
    )
    record(
        f"connect-error->502 wrapper mode={mode}",
        "PASS" if ok else "FAIL",
        "\n    ".join(lines),
    )


async def check_isolation(mode: str) -> None:
    """python-sdk #2604: one failing request (tool raise, non-2xx, timeout, cancel) must not kill siblings."""
    stats, port = Stats(), free_port()
    url = f"http://127.0.0.1:{port}/mcp"
    async with (
        uvicorn_server(port, stats),
        http_client() as hc,
        new_client(url, mode, hc) as client,
    ):
        await client.list_tools()
        jobs: dict[str, Awaitable[Any]] = {
            f"sibling{i}": client.call_tool("slow", {"seconds": SIBLING_SLEEP_S})
            for i in range(N_SIBLINGS)
        }
        jobs |= {
            "tool_raises": client.call_tool("boom"),
            "http_400": client.call_tool(HTTP_FAIL_TOOL, {"status": 400}),
            "http_404": client.call_tool(HTTP_FAIL_TOOL, {"status": 404}),
            "http_500": client.call_tool(HTTP_FAIL_TOOL, {"status": 500}),
            "read_timeout": client.call_tool(
                "slow", {"seconds": 5}, read_timeout_seconds=0.3
            ),
            "caller_cancel": asyncio.wait_for(
                client.call_tool("slow", {"seconds": 5}), 0.3
            ),
        }
        names = list(jobs)
        outs = await asyncio.gather(*(outcome(guarded(aw)) for aw in jobs.values()))
        after = await outcome(guarded(client.call_tool("echo", {"text": "alive"})))
    by_name = dict(zip(names, outs, strict=True))
    siblings_ok = all(
        by_name[f"sibling{i}"].startswith("ok is_error=False")
        for i in range(N_SIBLINGS)
    )
    ok = siblings_ok and after.startswith("ok is_error=False")
    detail = "\n    ".join(
        f"{n}: {o}"
        for n, o in by_name.items()
        if not n.startswith("sibling") or "ok" not in o
    )
    record(
        f"isolation mode={mode}",
        "PASS" if ok else "FAIL",
        f"siblings_ok={siblings_ok}\n    {detail}\n    after: {after}",
    )


async def check_asgi(mode: str, json_response: bool) -> None:
    """In-process Streamable HTTP via httpx2.ASGITransport (no port), headers on the httpx2 client."""
    stats = Stats()
    srv = make_server(stats)
    app = instrument(srv.streamable_http_app(json_response=json_response), stats)
    transport = httpx2.ASGITransport(app=app)
    lines: list[str] = Log()
    ok = False
    # ASGITransport sends no lifespan events: run the session manager ourselves.
    async with srv.session_manager.run():
        hc = http_client(
            transport=transport,
            base_url=ASGI_BASE,
            headers={"Authorization": "Bearer asgi"},
        )
        async with hc:
            try:
                async with asyncio.timeout(CALL_GUARD_S * 2):
                    async with new_client(f"{ASGI_BASE}/mcp", mode, hc) as client:
                        lines.append(
                            "echo: "
                            + await outcome(
                                guarded(client.call_tool("echo", {"text": "hi"}))
                            )
                        )
                        t0 = time.perf_counter()
                        conc = await asyncio.gather(
                            *(
                                outcome(
                                    guarded(client.call_tool("slow", {"seconds": 0.2}))
                                )
                                for _ in range(10)
                            )
                        )
                        wall = time.perf_counter() - t0
                        lines.append(
                            f"10 concurrent: wall={wall:.2f}s all_ok={all(c.startswith('ok') for c in conc)}"
                        )
                        ok = (
                            all(c.startswith("ok is_error=False") for c in conc)
                            and wall < 1.0
                        )
                lines.append("exit: ok")
            except BaseException as exc:  # spike probe
                if isinstance(exc, KeyboardInterrupt):
                    raise
                lines.append("raised " + describe(exc))
                ok = False
    ok = ok and stats.auth_headers == {"Bearer asgi"}
    record(
        f"asgi mode={mode} json_response={json_response}",
        "PASS" if ok else "FAIL",
        "\n    ".join(lines)
        + f"\n    auth_seen={sorted(stats.auth_headers)} {stats.summary()}",
    )


async def check_headers_port() -> None:
    """Headers set on httpx2.AsyncClient reach the server over a real local port."""
    stats, port = Stats(), free_port()
    url = f"http://127.0.0.1:{port}/mcp"
    async with (
        uvicorn_server(port, stats),
        http_client(headers={"Authorization": "Bearer port"}) as hc,
        new_client(url, "auto", hc) as client,
    ):
        await client.call_tool("echo", {"text": "h"})
    ok = stats.auth_headers == {"Bearer port"}
    record(
        "headers over local port",
        "PASS" if ok else "FAIL",
        f"auth_seen={sorted(stats.auth_headers)}",
    )


async def check_inproc() -> None:
    """Client(MCPServer) direct in-process (no HTTP at all)."""
    stats = Stats()
    async with Client(make_server(stats)) as client:
        out = await outcome(guarded(client.call_tool("echo", {"text": "direct"})))
        proto = client.protocol_version
    record(
        "in-process Client(MCPServer)",
        "PASS" if out.startswith("ok is_error=False") else "FAIL",
        f"{proto} {out}",
    )


def to_lc_blocks(result: CallToolResult) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Mapping proposed in design.md 'MCP results': text -> content; image/resource -> block + artifact."""
    from langchain_core.messages.content import (
        create_file_block,
        create_image_block,
        create_text_block,
    )

    blocks: list[dict[str, Any]] = []
    artifact: dict[str, Any] = {
        "structured_content": result.structured_content,
        "resources": [],
    }
    for b in result.content:
        if isinstance(b, TextContent):
            blocks.append(dict(create_text_block(b.text)))
        elif isinstance(b, ImageContent):
            blocks.append(
                dict(create_image_block(base64=b.data, mime_type=b.mime_type))
            )
        elif isinstance(b, EmbeddedResource):
            res = b.resource
            artifact["resources"].append(res.model_dump(mode="json", by_alias=True))
            if isinstance(res, TextResourceContents):
                blocks.append(dict(create_text_block(res.text)))
            else:
                mime = res.mime_type or "application/octet-stream"
                make = (
                    create_image_block
                    if mime.startswith("image/")
                    else create_file_block
                )
                blocks.append(dict(make(base64=res.blob, mime_type=mime)))
        else:
            raise TypeError(f"unmapped MCP block {b.type}")
    return blocks, artifact


async def check_content() -> None:
    """Optional: do image/resource blocks survive to a LangChain ToolMessage and to provider payloads."""
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
    from langchain_core.tools import StructuredTool

    stats, port = Stats(), free_port()
    url = f"http://127.0.0.1:{port}/mcp"
    async with (
        uvicorn_server(port, stats),
        http_client() as hc,
        new_client(url, "auto", hc) as client,
    ):
        listing = await client.list_tools()
        media = await client.call_tool("media")
        structured = await client.call_tool("structured", {"n": 21})
    wire_types = [b.type for b in media.content]
    spec = next(t for t in listing.tools if t.name == "structured")

    async def run_media() -> tuple[list[dict[str, Any]], dict[str, Any]]:
        return to_lc_blocks(media)

    tool = StructuredTool(
        name="media",
        description="media",
        args_schema=spec.input_schema,  # JSON Schema dict straight from tools/list
        coroutine=lambda **_: run_media(),
        response_format="content_and_artifact",
    )
    msg = await tool.ainvoke(
        {"type": "tool_call", "id": "call_1", "name": "media", "args": {}}
    )
    assert isinstance(msg, ToolMessage)
    std = [b["type"] for b in msg.content_blocks]
    lines = [
        f"wire blocks={wire_types}",
        f"structured_content={structured.structured_content}",
        f"ToolMessage.content types={[b['type'] for b in msg.content]} content_blocks={std}",
        f"artifact keys={sorted(msg.artifact)} resources={len(msg.artifact['resources'])}",
    ]
    history = [
        HumanMessage("hi"),
        AIMessage("", tool_calls=[{"id": "call_1", "name": "media", "args": {}}]),
        msg,
    ]
    lines += provider_payloads(history)
    ok = "image" in std and msg.artifact["resources"]
    record(
        "content blocks -> ToolMessage", "PASS" if ok else "FAIL", "\n    ".join(lines)
    )


def provider_payloads(history: list[Any]) -> list[str]:
    """Offline conversion through each installed provider's private formatter (no network)."""
    out: list[str] = []
    try:
        from langchain_anthropic.chat_models import _format_messages

        _, msgs = _format_messages(history, model="anthropic-model")
        tr = msgs[-1]["content"][0]
        out.append(
            f"anthropic: role={msgs[-1]['role']} {tr['type']} inner={[c['type'] for c in tr['content']]}"
        )
    except Exception as exc:  # spike probe: a provider formatter failing is a finding
        out.append("anthropic: raised " + describe(exc))
    try:
        from langchain_openai.chat_models.base import _convert_message_to_dict

        d = _convert_message_to_dict(history[-1])
        content = d["content"]
        shape = (
            [c.get("type") for c in content]
            if isinstance(content, list)
            else type(content).__name__
        )
        out.append(f"openai chat/completions: role={d['role']} content={shape}")
    except Exception as exc:  # spike probe
        out.append("openai chat/completions: raised " + describe(exc))
    try:
        from langchain_ollama import ChatOllama

        msgs = ChatOllama(model="x")._convert_messages_to_ollama_messages(history)
        last = msgs[-1]
        out.append(
            f"ollama: role={last['role']} images={len(last.get('images') or [])} content={str(last.get('content'))[:60]!r}"
        )
    except Exception as exc:  # spike probe
        out.append("ollama: raised " + describe(exc))
    return out


async def main(only: set[str]) -> None:
    pkgs = (
        "mcp",
        "mcp-types",
        "httpx2",
        "anyio",
        "starlette",
        "uvicorn",
        "langchain-core",
    )
    print(
        f"python {sys.version.split()[0]} | "
        + " ".join(f"{p}=={version(p)}" for p in pkgs)
    )
    checks: dict[str, list[Awaitable[None]]] = {
        "reuse": [check_reuse(m) for m in MODES],
        "reconnect": [check_host_cancel(m) for m in MODES]
        + [check_reconnect(m) for m in MODES],
        "isolation": [check_isolation(m) for m in MODES]
        + [check_transport_isolation(m) for m in MODES]
        + [check_connect_error_wrapper(m) for m in MODES],
        "hosting": [
            check_headers_port(),
            *(check_asgi(m, jr) for m in MODES for jr in (False, True)),
            check_inproc(),
        ],
        "content": [check_content()],
    }
    for name, coros in checks.items():
        for coro in coros:
            if only and name not in only:
                coro.close()  # type: ignore[attr-defined]
                continue
            await coro
    print("\nsummary: " + ", ".join(f"{c}={v}" for c, v in RESULTS))


if __name__ == "__main__":
    asyncio.run(main(set(sys.argv[1:])))
