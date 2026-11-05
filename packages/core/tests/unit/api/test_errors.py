from http import HTTPStatus
from typing import Any

import httpx
import pytest
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from graph_agent._internal.api.app import register_error_handlers
from graph_agent._internal.api.auth import AuthError
from graph_agent._internal.runtime.runs import ErrorCode, RunError


class Body(BaseModel):
    secret: str


def _app() -> FastAPI:
    app = FastAPI()
    register_error_handlers(app)

    @app.post("/body")
    async def body(item: Body) -> None: ...

    @app.get("/run-error")
    async def run_error() -> None:
        raise RunError(ErrorCode.THREAD_BUSY, "busy", retryable=True)

    @app.get("/auth")
    async def auth() -> None:
        raise AuthError()

    @app.get("/queue")
    async def queue() -> None:
        raise HTTPException(HTTPStatus.SERVICE_UNAVAILABLE, "queue_unavailable")

    @app.get("/boom")
    async def boom() -> None:
        raise ValueError("password=hunter2")

    return app


async def _get(method: str, path: str, **kwargs: Any) -> httpx.Response:
    transport = httpx.ASGITransport(app=_app(), raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        return await client.request(method, path, **kwargs)


@pytest.mark.parametrize(
    ("path", "status", "code", "retryable"),
    [
        ("/run-error", HTTPStatus.CONFLICT, "thread_busy", True),
        ("/auth", HTTPStatus.UNAUTHORIZED, "unauthorized", False),
        ("/queue", HTTPStatus.SERVICE_UNAVAILABLE, "queue_unavailable", True),
        ("/missing", HTTPStatus.NOT_FOUND, "not_found", False),
    ],
)
async def test_every_error_has_the_one_body(
    path: str, status: HTTPStatus, code: str, retryable: bool
) -> None:
    response = await _get("GET", path)
    assert response.status_code == status
    body = response.json()
    assert set(body) == {"code", "message", "retryable"}
    assert (body["code"], body["retryable"]) == (code, retryable)


async def test_validation_error_does_not_echo_the_input() -> None:
    response = await _get("POST", "/body", json={"secret": 1, "token": "s3cr3t-value"})
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json()["code"] == "invalid_request"
    assert "s3cr3t-value" not in response.text


async def test_unexpected_error_shows_the_type_only() -> None:
    response = await _get("GET", "/boom")
    assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
    assert response.json() == {
        "code": "internal_error",
        "message": "ValueError",
        "retryable": False,
    }
