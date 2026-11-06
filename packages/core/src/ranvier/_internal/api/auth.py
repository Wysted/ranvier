"""Principal and the three authenticators (OD-2): none, static API keys, module hook."""

import hashlib
import hmac
import importlib
import inspect
import ipaddress
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol

import anyio.to_thread
from fastapi import Request

from ranvier._internal.config.settings import AUTH_API_KEYS, AUTH_NONE, ApiSettings

UNAUTHORIZED = "unauthorized"
_BEARER_PREFIX = "bearer "
_LOCALHOST = "localhost"


@dataclass(frozen=True, slots=True)
class Principal:
    """The caller. `tenant_id=None` is an admin identity: it sees the threads of every tenant."""

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


def _digest(value: str) -> bytes:
    return hashlib.sha256(value.encode()).digest()


class _ApiKeys:
    def __init__(self, keys: list[tuple[bytes, Principal]]) -> None:
        self._keys = keys  # (sha256 digest, principal): raw keys are not kept in memory

    async def authenticate(self, request: Request) -> Principal:
        header = request.headers.get("authorization", "")
        if not header.lower().startswith(_BEARER_PREFIX):
            raise AuthError()
        token = _digest(header[len(_BEARER_PREFIX) :].strip())
        found: Principal | None = None
        for digest, principal in self._keys:  # no early exit: timing does not reveal the match
            if hmac.compare_digest(token, digest):
                found = principal
        if found is None:
            raise AuthError()
        return found


class _Hooked:
    def __init__(self, hook: _Hook) -> None:
        self._hook = hook

    async def authenticate(self, request: Request) -> Principal:
        result: Any
        if inspect.iscoroutinefunction(self._hook):
            result = await self._hook(request)
        else:  # a sync hook may do I/O: keep it off the event loop
            result = await anyio.to_thread.run_sync(self._hook, request)
            if inspect.isawaitable(result):  # callable object with an async __call__
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
    # shortcut: return type checked per call (a build-time check would need a request),
    # upgrade if hooks gain a typed protocol
    checked: _Hook = hook
    return checked


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
        owners: dict[bytes, str] = {}
        for subject, ref in settings.keys.items():
            value = os.environ.get(ref.key_env, "").strip()
            if not value:
                raise ValueError(f"api.keys.{subject}: env var {ref.key_env!r} is unset or empty")
            digest = _digest(value)
            if digest in owners:
                raise ValueError(f"api.keys: {owners[digest]!r} and {subject!r} share a key value")
            owners[digest] = subject
            keys.append((digest, Principal(subject, ref.tenant_id)))
        return _ApiKeys(keys)
    return _Hooked(_load_hook(settings.auth))


async def require_principal(request: Request) -> Principal:
    """FastAPI dependency; T23 stores the authenticator in `app.state.authenticator`."""
    authenticator: Authenticator = request.app.state.authenticator
    return await authenticator.authenticate(request)
