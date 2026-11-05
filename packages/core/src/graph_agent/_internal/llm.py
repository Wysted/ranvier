"""LLM profiles to chat models, one shared connection pool per provider (design sections 8, 9).

Provider packages are optional extras: they are only imported when a profile needs them.
Wiring per provider follows docs/spikes/S3.md.
"""

import os
from importlib.util import find_spec
from typing import Any

import httpx
import httpx2  # noqa: TID251 - llm.py builds the shared pools (design section 2)
from langchain.chat_models import init_chat_model
from langchain_core.language_models import BaseChatModel

from graph_agent._internal.config.manifest import LlmProfile, LlmSettings, ProviderSettings

# provider -> (integration package, extra that installs it)
_INTEGRATIONS = {
    "openai": ("langchain_openai", "openai"),
    "anthropic": ("langchain_anthropic", "anthropic"),
    "ollama": ("langchain_ollama", "ollama"),
}


class LlmError(Exception):
    """A profile cannot be turned into a chat model; the message never contains secrets."""


class LlmRegistry:
    """Builds chat models from profiles; owns the shared HTTP pools (close with `aclose`)."""

    def __init__(self, settings: LlmSettings) -> None:
        self._settings = settings
        # httpx2 clients (openai, anthropic) and httpx transports (ollama), one per provider.
        self._clients: dict[str, httpx2.AsyncClient] = {}
        self._transports: dict[str, httpx.AsyncHTTPTransport] = {}

    def model(self, profile: str) -> BaseChatModel:
        spec = self._settings.profiles.get(profile)
        if spec is None:
            known = ", ".join(sorted(self._settings.profiles)) or "none"
            raise LlmError(f"unknown LLM profile {profile!r} (configured: {known})")
        package, extra = _INTEGRATIONS[spec.provider]
        if find_spec(package) is None:
            raise LlmError(
                f"profile {profile!r} needs provider {spec.provider!r}: "
                f"install the extra 'graph-agent[{extra}]'"
            )
        kwargs = self._kwargs(profile, spec)
        model: Any = init_chat_model(spec.model, model_provider=spec.provider, **kwargs)
        if spec.provider == "anthropic":
            self._seed_anthropic(model, self._client("anthropic"))
        return model  # type: ignore[no-any-return]

    async def aclose(self) -> None:
        """Close the shared pools; models are never closed individually (S3)."""
        clients, transports = list(self._clients.values()), list(self._transports.values())
        self._clients.clear()
        self._transports.clear()
        for client in clients:
            await client.aclose()
        for transport in transports:
            await transport.aclose()

    def _max_connections(self, provider: str) -> int:
        settings = self._settings.providers.get(provider) or ProviderSettings()  # type: ignore[call-overload]
        return settings.max_connections

    def _client(self, provider: str) -> httpx2.AsyncClient:
        if provider not in self._clients:
            limits = httpx2.Limits(max_connections=self._max_connections(provider))
            self._clients[provider] = httpx2.AsyncClient(limits=limits)
        return self._clients[provider]

    def _transport(self, provider: str) -> httpx.AsyncHTTPTransport:
        if provider not in self._transports:
            limits = httpx.Limits(max_connections=self._max_connections(provider))
            self._transports[provider] = httpx.AsyncHTTPTransport(limits=limits)
        return self._transports[provider]

    def _kwargs(self, name: str, spec: LlmProfile) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if spec.base_url is not None:
            kwargs["base_url"] = spec.base_url
        if spec.temperature is not None:
            kwargs["temperature"] = spec.temperature
        if spec.api_key_env is not None:
            # Fail naming the variable, never its value.
            key = os.environ.get(spec.api_key_env)
            if not key:
                raise LlmError(
                    f"profile {name!r}: environment variable {spec.api_key_env} is not set"
                )
            kwargs["api_key"] = key
        match spec.provider:
            case "openai":
                # stream_usage=True always: a shared http_async_client turns the default off (S3).
                kwargs |= {
                    "timeout": spec.timeout_s,
                    "max_retries": spec.max_retries,
                    "http_async_client": self._client("openai"),
                    "stream_usage": True,
                }
            case "anthropic":
                kwargs |= {"timeout": spec.timeout_s, "max_retries": spec.max_retries}
            case "ollama":
                # No timeout/max_retries fields in ChatOllama: the timeout rides in the client.
                kwargs["async_client_kwargs"] = {
                    "transport": self._transport("ollama"),
                    "timeout": httpx.Timeout(timeout=spec.timeout_s),
                }
        return kwargs

    @staticmethod
    def _seed_anthropic(model: Any, client: httpx2.AsyncClient) -> None:
        import anthropic  # transitive of langchain-anthropic

        # shortcut: langchain-anthropic 1.7.x has no client kwarg, so we pre-fill the private
        # cached_property `_async_client` from `_client_params`; revisit when it grows one
        # (test_anthropic_shares_pool_client fails loudly if either name changes).
        model.__dict__["_async_client"] = anthropic.AsyncClient(
            **model._client_params, http_client=client
        )
