"""Principal and the three authenticators (OD-2): none, static API keys, module hook."""

import hmac
import importlib
import inspect
import ipaddress
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from fastapi import Request

from graph_agent._internal.config.settings import AUTH_API_KEYS, AUTH_NONE, ApiSettings

UNAUTHORIZED = "unauthorized"
_BEARER_PREFIX = "bearer "
_LOCALHOST = "localhost"


@dataclass(frozen=True)
class Principal:
    subject: str
    tenant_id: str | None = None


class AuthError(Exception):
    """Authentication failed; T23 maps it to the `{code, message, retryable}` body (401)."""

    def __init__(
        self, code: str = UNAUTHORIZED, message: str = "invalid or missing credentials"
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class Authenticator(Protocol):
    async def authenticate(self, request: Request) -> Principal: ...


type _Hook = Callable[[Request], Principal | Awaitable[Principal]]

_ANONYMOUS = Principal(subject="anonymous")


def _is_loopback(host: str) -> bool:
    if host == _LOCALHOST:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class _NoAuth:
    async def authenticate(self, request: Request) -> Principal:
        return _ANONYMOUS


class _ApiKeys:
    def __init__(self, keys: list[tuple[bytes, Principal]]) -> None:
        self._keys = keys

    async def authenticate(self, request: Request) -> Principal:
        header = request.headers.get("authorization", "")
        if not header.lower().startswith(_BEARER_PREFIX):
            raise AuthError()
        token = header[len(_BEARER_PREFIX) :].strip().encode()
        found: Principal | None = None
        for key, principal in self._keys:  # no early exit: timing does not reveal which key matched
            if hmac.compare_digest(token, key):
                found = principal
        if found is None:
            raise AuthError()
        return found


class _Hooked:
    def __init__(self, hook: _Hook) -> None:
        self._hook = hook

    async def authenticate(self, request: Request) -> Principal:
        result = self._hook(request)
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, Principal):
            raise TypeError(f"auth hook must return a Principal, got {type(result).__name__}")
        return result


def _load_hook(target: str) -> _Hook:
    module_name, _, attr = target.partition(":")
    try:
        hook = getattr(importlib.import_module(module_name), attr)
    except (ImportError, AttributeError) as exc:
        raise ValueError(f"cannot import auth hook {target!r}: {exc}") from exc
    if not callable(hook):
        raise ValueError(f"auth hook {target!r} is not callable")
    # shortcut: only callability is checked at build time, the return type is checked per call.
    return hook  # type: ignore[no-any-return]


def build_authenticator(settings: ApiSettings) -> Authenticator:
    """Build the authenticator for `settings.auth`; misconfiguration fails here, at startup."""
    if settings.auth == AUTH_NONE:
        if not _is_loopback(settings.host):
            raise ValueError(
                "api.auth = 'none' is only allowed on a loopback host, "
                f"got api.host = {settings.host!r}; "
                f"set api.auth to '{AUTH_API_KEYS}' or a hook, or bind to 127.0.0.1"
            )
        return _NoAuth()
    if settings.auth == AUTH_API_KEYS:
        keys: list[tuple[bytes, Principal]] = []
        for subject, ref in settings.keys.items():
            value = os.environ.get(ref.key_env)
            if not value:
                raise ValueError(f"api.keys.{subject}: env var {ref.key_env!r} is unset or empty")
            keys.append((value.encode(), Principal(subject, ref.tenant_id)))
        return _ApiKeys(keys)
    return _Hooked(_load_hook(settings.auth))


async def require_principal(request: Request) -> Principal:
    """FastAPI dependency; T23 stores the authenticator in `app.state.authenticator`."""
    authenticator: Authenticator = request.app.state.authenticator
    return await authenticator.authenticate(request)
