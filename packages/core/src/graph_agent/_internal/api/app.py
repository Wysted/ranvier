"""FastAPI app factory: lifespan, the one error handler, routers (design section 6)."""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import Final

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from graph_agent._internal.api.auth import AuthError, build_authenticator
from graph_agent._internal.api.dto import ERROR_STATUS
from graph_agent._internal.api.routes import health, router
from graph_agent._internal.bootstrap import open_container
from graph_agent._internal.config.settings import Settings
from graph_agent._internal.runtime.runs import ErrorCode, RunError

logger = logging.getLogger(__name__)

_RETRYABLE_STATUS: Final = frozenset(
    {
        HTTPStatus.TOO_MANY_REQUESTS,
        HTTPStatus.BAD_GATEWAY,
        HTTPStatus.SERVICE_UNAVAILABLE,
        HTTPStatus.GATEWAY_TIMEOUT,
    }
)


def error_response(
    status: HTTPStatus, code: str, message: str, retryable: bool = False
) -> JSONResponse:
    """The one error body: `{code, message, retryable}`."""
    body = {"code": code, "message": message, "retryable": retryable}
    return JSONResponse(body, status_code=status)


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(RunError)
    async def _run_error(request: Request, exc: RunError) -> JSONResponse:
        return error_response(ERROR_STATUS[exc.code], exc.code.value, exc.message, exc.retryable)

    @app.exception_handler(AuthError)
    async def _auth_error(request: Request, exc: AuthError) -> JSONResponse:
        return error_response(HTTPStatus.UNAUTHORIZED, exc.code, exc.message)

    @app.exception_handler(RequestValidationError)
    async def _invalid(request: Request, exc: RequestValidationError) -> JSONResponse:
        # loc + msg only: pydantic's `input` would echo the whole request body.
        message = "; ".join(
            f"{'.'.join(str(part) for part in err['loc'])}: {err['msg']}" for err in exc.errors()
        )
        return error_response(
            ERROR_STATUS[ErrorCode.INVALID_REQUEST], ErrorCode.INVALID_REQUEST.value, message
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        status = HTTPStatus(exc.status_code)
        # `detail` is the code ("queue_unavailable") or Starlette's phrase ("Not Found").
        code = str(exc.detail).lower().replace(" ", "_")
        return error_response(status, code, status.phrase, status in _RETRYABLE_STATUS)

    @app.exception_handler(Exception)
    async def _unexpected(request: Request, exc: Exception) -> JSONResponse:
        logger.error("unhandled error on %s %s", request.method, request.url.path, exc_info=exc)
        # Type only: user exceptions may carry secrets.
        return error_response(
            HTTPStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR.value, type(exc).__name__
        )


def create_app(settings: Settings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        async with open_container(settings) as container:
            # May import the auth hook module (blocking I/O) and fails fast on misconfiguration.
            app.state.authenticator = await asyncio.to_thread(build_authenticator, settings.api)
            app.state.container = container
            yield

    # No OpenAPI/docs routes: they would be served without auth.
    app = FastAPI(
        title="graph-agent", lifespan=lifespan, openapi_url=None, docs_url=None, redoc_url=None
    )
    register_error_handlers(app)
    app.include_router(health)
    app.include_router(router)
    return app


def app_from_env() -> FastAPI:
    """Factory for `uvicorn --factory` (`graph-agent serve`): settings from manifest and env."""
    return create_app(Settings())
