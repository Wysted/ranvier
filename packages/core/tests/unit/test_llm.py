import asyncio
from typing import Any

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
                "an": {"provider": "anthropic", "model": "claude-x", "timeout_s": 9} | KEYED,
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
