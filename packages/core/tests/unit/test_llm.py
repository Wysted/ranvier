import asyncio
import functools
import inspect
from typing import Any

import httpx2
import pytest

from graph_agent._internal.config.manifest import LlmSettings
from graph_agent._internal.llm import LlmError, LlmRegistry

MAX_CONN = 3
KEYED = {"api_key_env": "FAKE_KEY"}


def _registry(monkeypatch: pytest.MonkeyPatch) -> LlmRegistry:
    monkeypatch.setenv("FAKE_KEY", "sk-fake")
    settings = LlmSettings.model_validate(
        {
            "providers": {
                p: {"max_connections": MAX_CONN} for p in ("openai", "anthropic", "ollama")
            },
            "profiles": {
                "oa": {"provider": "openai", "model": "gpt-x", "timeout_s": 7, "max_retries": 4}
                | KEYED,
                "oa2": {"provider": "openai", "model": "gpt-y"} | KEYED,
                "an": {
                    "provider": "anthropic",
                    "model": "claude-x",
                    "timeout_s": 9,
                    "max_retries": 6,
                }
                | KEYED,
                "an2": {"provider": "anthropic", "model": "claude-y"} | KEYED,
                "ol": {"provider": "ollama", "model": "llama", "timeout_s": 5},
                "ol2": {"provider": "ollama", "model": "llama2"},
            },
        }
    )
    return LlmRegistry(settings)


def _pool_limit(owner: Any) -> int:
    # httpx2.AsyncClient and httpx.AsyncHTTPTransport both keep an httpcore pool.
    pool = getattr(owner, "_transport", owner)._pool
    return int(pool._max_connections)


def test_openai_shares_client_and_applies_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry(monkeypatch)
    a, b = reg.model("oa"), reg.model("oa2")
    shared = a.root_async_client._client  # type: ignore[attr-defined]
    assert shared is b.root_async_client._client  # type: ignore[attr-defined]
    assert shared is reg._clients["openai"]
    assert _pool_limit(shared) == MAX_CONN
    assert a.stream_usage is True  # type: ignore[attr-defined]
    assert a.root_async_client.timeout == 7  # type: ignore[attr-defined]
    assert a.root_async_client.max_retries == 4  # type: ignore[attr-defined]
    asyncio.run(reg.aclose())


def test_anthropic_shares_pool_client(monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry(monkeypatch)
    a, b = reg.model("an"), reg.model("an2")
    shared = a._async_client._client  # type: ignore[attr-defined]
    assert shared is b._async_client._client  # type: ignore[attr-defined]
    assert shared is reg._clients["anthropic"]
    assert _pool_limit(shared) == MAX_CONN
    assert a._async_client.timeout == 9  # type: ignore[attr-defined]
    assert a._async_client.max_retries == 6  # type: ignore[attr-defined]
    # The seeding relies on this being a cached_property (see llm.py shortcut).
    static = inspect.getattr_static(type(a), "_async_client")
    assert isinstance(static, functools.cached_property)
    asyncio.run(reg.aclose())


def test_ollama_shares_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry(monkeypatch)
    a, b = reg.model("ol"), reg.model("ol2")
    transport = reg._transports["ollama"]
    assert a._async_client._client._transport is transport  # type: ignore[attr-defined]
    assert b._async_client._client._transport is transport  # type: ignore[attr-defined]
    assert _pool_limit(transport) == MAX_CONN
    assert a._async_client._client.timeout.read == 5  # type: ignore[attr-defined]
    asyncio.run(reg.aclose())
    assert not reg._transports


def test_unknown_profile_names_key(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(LlmError, match="'nope'"):
        _registry(monkeypatch).model("nope")


def test_missing_env_names_variable_only(monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry(monkeypatch)
    monkeypatch.delenv("FAKE_KEY")
    with pytest.raises(LlmError, match="FAKE_KEY"):
        reg.model("oa")


def test_missing_extra_names_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry(monkeypatch)
    monkeypatch.setattr("graph_agent._internal.llm.find_spec", lambda _name: None)
    with pytest.raises(LlmError, match=r"graph-agent\[ollama\]"):
        reg.model("ol")


def test_anthropic_requests_go_through_shared_client(monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry(monkeypatch)
    seen: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        seen.append(request)
        return httpx2.Response(
            200,
            json={
                "id": "msg_1",
                "type": "message",
                "role": "assistant",
                "model": "claude-x",
                "content": [{"type": "text", "text": "hi"}],
                "stop_reason": "end_turn",
                "stop_sequence": None,
                "usage": {"input_tokens": 1, "output_tokens": 1},
            },
        )

    reg._clients["anthropic"] = httpx2.AsyncClient(transport=httpx2.MockTransport(handler))
    model = reg.model("an")
    result = asyncio.run(model.ainvoke("hello"))
    assert result.content == "hi"
    assert len(seen) == 1
    asyncio.run(reg.aclose())


def test_model_is_cached_per_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry(monkeypatch)
    first = reg.model("oa")

    def boom(*_a: object, **_k: object) -> None:
        raise AssertionError("must not rebuild a cached model")

    monkeypatch.setattr("graph_agent._internal.llm.find_spec", boom)
    monkeypatch.setattr("graph_agent._internal.llm.init_chat_model", boom)
    assert reg.model("oa") is first
    asyncio.run(reg.aclose())


def test_aclose_closes_all_pools_and_blocks_reuse(monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry(monkeypatch)
    reg.model("oa")
    reg.model("ol")
    client, transport = reg._clients["openai"], reg._transports["ollama"]

    async def fail() -> None:
        raise RuntimeError("close failed")

    closed: list[bool] = []

    async def mark() -> None:
        closed.append(True)

    monkeypatch.setattr(client, "aclose", fail)
    monkeypatch.setattr(transport, "aclose", mark)
    with pytest.raises(RuntimeError, match="close failed"):
        asyncio.run(reg.aclose())
    assert closed == [True]  # the transport was closed despite the client failing
    assert not reg._clients
    assert not reg._transports
    with pytest.raises(LlmError, match="closed"):
        reg.model("oa")
