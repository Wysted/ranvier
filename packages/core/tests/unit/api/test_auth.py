import pytest
from fastapi import Request

from graph_agent._internal.api.auth import AuthError, Principal, build_authenticator
from graph_agent._internal.config.settings import ApiKeyRef, ApiSettings


def _request(authorization: str | None = None) -> Request:
    headers = [(b"authorization", authorization.encode())] if authorization else []
    return Request({"type": "http", "headers": headers})


async def good_hook(request: Request) -> Principal:
    return Principal("hooked", "t1")


def bad_hook(request: Request) -> str:
    return "nope"


not_callable = 1


async def test_none_on_loopback() -> None:
    auth = build_authenticator(ApiSettings())
    assert (await auth.authenticate(_request())).subject == "anonymous"


@pytest.mark.parametrize("host", ["0.0.0.0", "10.0.0.5", "example.com"])
def test_none_refused_off_loopback(host: str) -> None:
    with pytest.raises(ValueError, match="loopback"):
        build_authenticator(ApiSettings(host=host))


@pytest.mark.parametrize("host", ["127.0.0.1", "::1", "localhost"])
def test_none_allowed_on_loopback_hosts(host: str) -> None:
    build_authenticator(ApiSettings(host=host))


def _keys_settings() -> ApiSettings:
    return ApiSettings(
        host="0.0.0.0",
        auth="api_keys",
        keys={
            "alice": ApiKeyRef(key_env="TEST_KEY_A", tenant_id="t1"),
            "bob": ApiKeyRef(key_env="TEST_KEY_B"),
        },
    )


async def test_api_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_KEY_A", "secret-a")
    monkeypatch.setenv("TEST_KEY_B", "secret-b")
    auth = build_authenticator(_keys_settings())
    assert await auth.authenticate(_request("Bearer secret-a")) == Principal("alice", "t1")
    assert await auth.authenticate(_request("bearer secret-b")) == Principal("bob", None)
    for header in (None, "Bearer wrong", "Basic secret-a", "Bearer ", "Bearer sécret"):
        with pytest.raises(AuthError) as exc:
            await auth.authenticate(_request(header))
        assert exc.value.code == "unauthorized"


def test_api_keys_missing_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEST_KEY_A", raising=False)
    monkeypatch.setenv("TEST_KEY_B", "x")
    with pytest.raises(ValueError, match="TEST_KEY_A"):
        build_authenticator(_keys_settings())


async def test_hook() -> None:
    auth = build_authenticator(ApiSettings(auth=f"{__name__}:good_hook"))
    assert await auth.authenticate(_request()) == Principal("hooked", "t1")


async def test_hook_wrong_return_type() -> None:
    auth = build_authenticator(ApiSettings(auth=f"{__name__}:bad_hook"))
    with pytest.raises(TypeError, match="Principal"):
        await auth.authenticate(_request())


@pytest.mark.parametrize(
    "target", ["no_such_module_xyz:f", f"{__name__}:missing", f"{__name__}:not_callable"]
)
def test_hook_build_failures(target: str) -> None:
    with pytest.raises(ValueError, match="auth hook"):
        build_authenticator(ApiSettings(auth=target))
