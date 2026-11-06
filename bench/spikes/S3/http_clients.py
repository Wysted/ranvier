"""S3 check: shared httpx.AsyncClient with Limits per provider via init_chat_model, and stream_usage.

Run: uv run python -W ignore::DeprecationWarning bench/spikes/S3/run.py
Talks only to a local stub server; dummy keys; no network.
"""

import asyncio
import json
import os
import warnings
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from importlib.metadata import version
from typing import Any

import anthropic
import httpx
import httpx2
from langchain.chat_models import init_chat_model
from langchain_core.language_models import BaseChatModel

MAX_CONNECTIONS = 2
CONCURRENT_CALLS = 8
DELAY_S = 0.3
TOKENS_IN, TOKENS_OUT = 3, 1


# ---------------------------------------------------------------- stub server


def _sse(events: list[tuple[str | None, Any]]) -> bytes:
    out = []
    for event, data in events:
        head = f"event: {event}\n" if event else ""
        body = data if isinstance(data, str) else json.dumps(data)
        out.append(f"{head}data: {body}\n\n")
    return "".join(out).encode()


def openai_reply(req: dict[str, Any]) -> tuple[str, bytes]:
    usage = {
        "prompt_tokens": TOKENS_IN,
        "completion_tokens": TOKENS_OUT,
        "total_tokens": 4,
    }
    base = {"id": "c1", "created": 0, "model": req["model"]}
    if not req.get("stream"):
        msg = {"role": "assistant", "content": "hi"}
        choice = {"index": 0, "message": msg, "finish_reason": "stop"}
        body = {
            **base,
            "object": "chat.completion",
            "choices": [choice],
            "usage": usage,
        }
        return "application/json", json.dumps(body).encode()
    chunk = {**base, "object": "chat.completion.chunk"}
    delta = {"role": "assistant", "content": "hi"}
    events: list[tuple[str | None, Any]] = [
        (
            None,
            {**chunk, "choices": [{"index": 0, "delta": delta, "finish_reason": None}]},
        ),
        (
            None,
            {**chunk, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
        ),
    ]
    # Like the real API: usage only when stream_options.include_usage is requested.
    if (req.get("stream_options") or {}).get("include_usage"):
        events.append((None, {**chunk, "choices": [], "usage": usage}))
    events.append((None, "[DONE]"))
    return "text/event-stream", _sse(events)


def anthropic_reply(req: dict[str, Any]) -> tuple[str, bytes]:
    msg = {"id": "m1", "type": "message", "role": "assistant", "model": req["model"]}
    if not req.get("stream"):
        body = {
            **msg,
            "content": [{"type": "text", "text": "hi"}],
            "stop_reason": "end_turn",
            "stop_sequence": None,
            "usage": {"input_tokens": TOKENS_IN, "output_tokens": TOKENS_OUT},
        }
        return "application/json", json.dumps(body).encode()
    start = {
        **msg,
        "content": [],
        "stop_reason": None,
        "stop_sequence": None,
        "usage": {"input_tokens": TOKENS_IN, "output_tokens": 0},
    }
    events: list[tuple[str | None, Any]] = [
        ("message_start", {"type": "message_start", "message": start}),
        (
            "content_block_start",
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""},
            },
        ),
        (
            "content_block_delta",
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": "hi"},
            },
        ),
        ("content_block_stop", {"type": "content_block_stop", "index": 0}),
        (
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                # Cumulative, as in anthropic/types/message_delta_usage.py.
                "usage": {"input_tokens": TOKENS_IN, "output_tokens": TOKENS_OUT},
            },
        ),
        ("message_stop", {"type": "message_stop"}),
    ]
    return "text/event-stream", _sse(events)


def ollama_reply(req: dict[str, Any]) -> tuple[str, bytes]:
    base = {"model": req["model"], "created_at": "2026-01-01T00:00:00Z"}
    final = {
        **base,
        "message": {
            "role": "assistant",
            "content": "" if req.get("stream", True) else "hi",
        },
        "done": True,
        "done_reason": "stop",
        "total_duration": 1,
        "load_duration": 1,
        "prompt_eval_count": TOKENS_IN,
        "prompt_eval_duration": 1,
        "eval_count": TOKENS_OUT,
        "eval_duration": 1,
    }
    if not req.get("stream", True):
        return "application/json", json.dumps(final).encode()
    first = {**base, "message": {"role": "assistant", "content": "hi"}, "done": False}
    return (
        "application/x-ndjson",
        f"{json.dumps(first)}\n{json.dumps(final)}\n".encode(),
    )


ROUTES: dict[str, Callable[[dict[str, Any]], tuple[str, bytes]]] = {
    "/chat/completions": openai_reply,
    "/v1/messages": anthropic_reply,
    "/api/chat": ollama_reply,
}


class Stub:
    """HTTP/1.1 keep-alive server that counts concurrent TCP connections."""

    def __init__(self) -> None:
        self.open = self.max_open = self.inflight = self.max_inflight = 0
        self.requests: list[tuple[str, dict[str, Any]]] = []

    async def handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self.open += 1
        self.max_open = max(self.max_open, self.open)
        try:
            while line := await reader.readline():
                path = line.decode().split(" ")[1]
                headers: dict[str, str] = {}
                while (h := await reader.readline()) not in (b"\r\n", b""):
                    k, v = h.decode().split(":", 1)
                    headers[k.strip().lower()] = v.strip()
                raw = await reader.readexactly(int(headers.get("content-length", "0")))
                req = json.loads(raw) if raw else {}
                self.requests.append((path, req))
                self.inflight += 1
                self.max_inflight = max(self.max_inflight, self.inflight)
                await asyncio.sleep(DELAY_S)
                self.inflight -= 1
                route = next(
                    (fn for suffix, fn in ROUTES.items() if path.endswith(suffix)), None
                )
                if route is None:
                    writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                else:
                    ctype, body = route(req)
                    head = f"HTTP/1.1 200 OK\r\nContent-Type: {ctype}\r\nContent-Length: {len(body)}\r\n\r\n"
                    writer.write(head.encode() + body)
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass  # client closed the socket mid-request; nothing to answer
        finally:
            self.open -= 1
            writer.close()


@asynccontextmanager
async def stub_server() -> AsyncIterator[tuple[Stub, str]]:
    stub = Stub()
    server = await asyncio.start_server(stub.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield stub, f"http://127.0.0.1:{port}"
    finally:
        server.close()
        server.close_clients()
        await server.wait_closed()


# ---------------------------------------------------------------- helpers


async def burst(
    stub: Stub, models: list[BaseChatModel], *, stream: bool = False
) -> str:
    """CONCURRENT_CALLS calls spread over the given model instances; report server-side counts."""

    async def one(model: BaseChatModel) -> None:
        if stream:
            async for _ in model.astream("hi"):
                pass
        else:
            await model.ainvoke("hi")

    await asyncio.gather(
        *(one(models[i % len(models)]) for i in range(CONCURRENT_CALLS))
    )
    return (
        f"calls={CONCURRENT_CALLS} instances={len(models)} "
        f"max_open_tcp={stub.max_open} max_inflight={stub.max_inflight}"
    )


async def streamed_usage(stub: Stub, model: BaseChatModel) -> str:
    stub.requests.clear()
    total = None
    async for chunk in model.astream("hi"):
        total = chunk if total is None else total + chunk
    req = stub.requests[-1][1]
    usage = getattr(total, "usage_metadata", None)
    return f"stream_options_sent={req.get('stream_options')} usage_metadata={usage}"


def build(*args: Any, **kwargs: Any) -> tuple[BaseChatModel | None, list[str]]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            model = init_chat_model(*args, **kwargs)
        except (TypeError, ValueError) as exc:  # spike: report the rejection
            return None, [f"{type(exc).__name__}: {str(exc)[:200]}"]
    return model, [f"{w.category.__name__}: {str(w.message)[:200]}" for w in caught]


def limited_client(lib: Any = httpx2) -> Any:
    """openai>=3 and anthropic 2.x run on httpx2; ollama on httpx."""
    return lib.AsyncClient(limits=lib.Limits(max_connections=MAX_CONNECTIONS))


# ---------------------------------------------------------------- checks


async def check_openai() -> None:
    print("== openai (default base URL, no network call)")
    print(f"  OPENAI_BASE_URL set in env: {'OPENAI_BASE_URL' in os.environ}")
    plain, _ = build("gpt-4o-mini", model_provider="openai", api_key="sk-dummy")
    print(f"  no client: stream_usage={plain.stream_usage}")  # type: ignore[union-attr]
    shared = limited_client()
    m, notes = build(
        "gpt-4o-mini",
        model_provider="openai",
        api_key="sk-dummy",
        http_async_client=shared,
        timeout=60,
        max_retries=2,
    )
    assert m is not None, notes
    sdk = m.root_async_client  # type: ignore[attr-defined]
    print(
        f"  http_async_client: warnings={notes} same_instance={sdk._client is shared}"
    )
    print(
        f"  sdk timeout={sdk.timeout} max_retries={sdk.max_retries} stream_usage={m.stream_usage}"
    )  # type: ignore[attr-defined]
    copy = m.model_copy(update={"metadata": {"ranvier_agent": "x"}})
    print(f"  model_copy keeps client: {copy.root_async_client._client is shared}")  # type: ignore[attr-defined]
    await shared.aclose()


async def check_openai_compatible() -> None:
    print("== openai-compatible base_url (stub)")
    async with stub_server() as (stub, url):
        common = {
            "model_provider": "openai",
            "api_key": "sk-dummy",
            "base_url": f"{url}/v1",
        }
        a, _ = build("local-model", **common)
        b, _ = build("local-model", **common)
        print(f"  baseline, own clients: {await burst(stub, [a, b])}")  # type: ignore[list-item]
    for lib in (httpx2, httpx):
        async with stub_server() as (stub, url):
            shared = limited_client(lib)
            kw = {**common, "base_url": f"{url}/v1", "http_async_client": shared}
            a, notes = build("local-model", **kw)
            b, _ = build("other-model", **kw)
            same = (
                a.root_async_client._client is shared
                and b.root_async_client._client is shared
            )  # type: ignore[union-attr]
            print(
                f"  shared {lib.__name__}.AsyncClient: warnings={notes} same_instance={same}"
            )
            print(f"    Limits({MAX_CONNECTIONS}): {await burst(stub, [a, b])}")  # type: ignore[list-item]
            print(f"    streaming: {await burst(stub, [a, b], stream=True)}")  # type: ignore[list-item]
            print(
                f"    stream_usage default={a.stream_usage}: {await streamed_usage(stub, a)}"
            )  # type: ignore[union-attr, arg-type]
            on, _ = build("local-model", **kw, stream_usage=True)
            print(f"    stream_usage=True: {await streamed_usage(stub, on)}")  # type: ignore[arg-type]
            await shared.aclose()


def seed_anthropic_client(model: Any, client: httpx.AsyncClient) -> None:
    """Pre-fill ChatAnthropic's cached `_async_client` with an SDK client over the shared httpx client."""
    model.__dict__["_async_client"] = anthropic.AsyncClient(
        **model._client_params, http_client=client
    )


async def check_anthropic() -> None:
    print("== anthropic (stub)")
    async with stub_server() as (stub, url):
        shared = limited_client()
        common = {"model_provider": "anthropic", "api_key": "sk-dummy", "base_url": url}
        m, notes = build("anthropic-model", **common, http_async_client=shared)
        print(f"  http_async_client kwarg: warnings={notes}")
        if m is not None:
            print(
                f"  kwarg landed in model_kwargs: {'http_async_client' in m.model_kwargs}"
            )  # type: ignore[attr-defined]
            try:
                await m.ainvoke("hi")
                print(
                    f"  ainvoke ok; same_instance={m._async_client._client is shared}"
                )  # type: ignore[attr-defined]
            except TypeError as exc:  # spike: show the failure mode
                print(f"  ainvoke fails: {type(exc).__name__}: {str(exc)[:160]}")
        a, _ = build("anthropic-model", **common)
        b, _ = build("anthropic-model", **common)
        default_shared = a._async_client._client is b._async_client._client  # type: ignore[union-attr]
        limits = a._async_client._client._transport._pool._max_connections  # type: ignore[union-attr]
        print(
            f"  default: lru-cached httpx client shared across instances={default_shared} pool max_connections={limits}"
        )
        print(f"  baseline, default client: {await burst(stub, [a, b])}")  # type: ignore[list-item]
    async with stub_server() as (stub, url):
        common = {**common, "base_url": url, "timeout": 60, "max_retries": 2}
        a, _ = build("anthropic-model", **common)
        b, _ = build("anthropic-model-small", **common)
        try:
            seed_anthropic_client(a, limited_client(httpx))
        except TypeError as exc:
            print(
                f"  seeding with an httpx (not httpx2) client: TypeError: {str(exc)[:120]}"
            )
        seed_anthropic_client(a, shared)
        seed_anthropic_client(b, shared)
        same = a._async_client._client is shared and b._async_client._client is shared  # type: ignore[union-attr]
        sdk = a._async_client  # type: ignore[union-attr]
        print(
            f"  seeded _async_client: same_instance={same} sdk timeout={sdk.timeout} max_retries={sdk.max_retries}"
        )
        copy = a.model_copy(update={"metadata": {"ranvier_agent": "x"}})  # type: ignore[union-attr]
        print(f"  model_copy keeps client: {copy._async_client._client is shared}")
        print(f"  seeded Limits({MAX_CONNECTIONS}): {await burst(stub, [a, b])}")  # type: ignore[list-item]
        print(f"  seeded Limits, streaming: {await burst(stub, [a, b], stream=True)}")  # type: ignore[list-item]
        print(
            f"  stream_usage default={a.stream_usage}: {await streamed_usage(stub, a)}"
        )  # type: ignore[union-attr, arg-type]
        await shared.aclose()


async def check_ollama() -> None:
    print("== ollama (stub)")
    async with stub_server() as (stub, url):
        shared = limited_client(httpx)
        m, notes = build(
            "llama3.2", model_provider="ollama", base_url=url, http_async_client=shared
        )
        print(f"  http_async_client kwarg: built={m is not None} notes={notes}")
        if m is not None:
            inner = m._async_client._client  # type: ignore[attr-defined]
            print(f"  same_instance={inner is shared}")
        a, _ = build("llama3.2", model_provider="ollama", base_url=url)
        b, _ = build("llama3.2", model_provider="ollama", base_url=url)
        print(f"  baseline, own clients: {await burst(stub, [a, b])}")  # type: ignore[list-item]
        await shared.aclose()
    async with stub_server() as (stub, url):
        per_instance = {"limits": httpx.Limits(max_connections=MAX_CONNECTIONS)}
        a, _ = build(
            "llama3.2",
            model_provider="ollama",
            base_url=url,
            async_client_kwargs=per_instance,
        )
        b, _ = build(
            "llama3.2",
            model_provider="ollama",
            base_url=url,
            async_client_kwargs=per_instance,
        )
        print(
            f"  async_client_kwargs limits (per instance): {await burst(stub, [a, b])}"
        )  # type: ignore[list-item]
    async with stub_server() as (stub, url):
        transport = httpx.AsyncHTTPTransport(
            limits=httpx.Limits(max_connections=MAX_CONNECTIONS)
        )
        kw = {"transport": transport, "timeout": 60}
        a, notes = build(
            "llama3.2", model_provider="ollama", base_url=url, async_client_kwargs=kw
        )
        b, _ = build(
            "qwen3", model_provider="ollama", base_url=url, async_client_kwargs=kw
        )
        same = (
            a._async_client._client._transport is transport
            and b._async_client._client._transport is transport
        )  # type: ignore[union-attr]
        print(
            f"  shared transport: warnings={notes} same_transport={same} timeout={a._async_client._client.timeout}"
        )  # type: ignore[union-attr]
        print(
            f"  shared transport Limits({MAX_CONNECTIONS}): {await burst(stub, [a, b])}"
        )  # type: ignore[list-item]
        print(
            f"  shared transport, streaming: {await burst(stub, [a, b], stream=True)}"
        )  # type: ignore[list-item]
        stub.requests.clear()
        total = None
        async for chunk in a.astream("hi"):  # type: ignore[union-attr]
            total = chunk if total is None else total + chunk
        print(f"  streamed usage_metadata={total.usage_metadata}")  # type: ignore[union-attr]
        res = await a.ainvoke("hi")  # type: ignore[union-attr]
        print(
            f"  ainvoke usage_metadata={res.usage_metadata} request stream={stub.requests[-1][1].get('stream')}"
        )
        await transport.aclose()


async def main() -> None:
    pkgs = [
        "langchain",
        "langchain-core",
        "langchain-openai",
        "openai",
        "langchain-anthropic",
        "anthropic",
        "langchain-ollama",
        "ollama",
        "httpx",
    ]
    print("versions: " + ", ".join(f"{p}=={version(p)}" for p in pkgs))
    await check_openai()
    await check_openai_compatible()
    await check_anthropic()
    await check_ollama()


if __name__ == "__main__":
    asyncio.run(main())
