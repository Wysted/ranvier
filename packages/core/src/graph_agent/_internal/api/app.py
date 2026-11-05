"""FastAPI app factory: lifespan, the one error handler, routers (design section 6)."""

import asyncio
import logging
import signal
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from http import HTTPStatus
from types import FrameType
from typing import Final

import uvicorn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from graph_agent._internal.api.auth import AuthError, build_authenticator
from graph_agent._internal.api.dto import ERROR_STATUS, error_response
from graph_agent._internal.api.routes import health, router
from graph_agent._internal.bootstrap import Container, open_container
from graph_agent._internal.config.settings import Settings
from graph_agent._internal.limits import graceful_wait_s
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
        code = status.phrase.lower().replace(" ", "_")  # "Not Found" -> "not_found"
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
        # First: may import the auth hook module (blocking I/O) and fails fast on a bad config
        # before any pool is opened.
        app.state.authenticator = await asyncio.to_thread(build_authenticator, settings.api)
        async with open_container(settings) as container:
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


class DrainingServer(uvicorn.Server):
    """uvicorn that begins the platform shutdown before it waits for connections.

    Left alone, uvicorn waits for every open stream first and the lifespan only ends afterwards,
    so inline runs would not hear about the shutdown until then. Here the first thing `shutdown`
    does is `Container.begin_shutdown`: admission closes and the runs in flight drain (their
    streams end with `run_handed_over`), then uvicorn closes the listener and waits.
    """

    def __init__(self, config: uvicorn.Config, app: FastAPI) -> None:
        super().__init__(config)
        self._app = app

    async def shutdown(self, sockets: list[socket.socket] | None = None) -> None:
        container: Container | None = getattr(self._app.state, "container", None)
        if container is not None:  # None: the lifespan never got that far
            container.begin_shutdown()
        await super().shutdown(sockets)

    def handle_exit(self, sig: int, frame: FrameType | None) -> None:
        # Upstream records the signal and re-raises it with the original handler once the server is
        # done: SIGTERM then kills the interpreter before its cleanup (leaked semaphores) and
        # SIGINT raises KeyboardInterrupt. Exiting normally is the point of a graceful stop.
        if self.should_exit and sig == signal.SIGINT:
            self.force_exit = True
        self.should_exit = True


def serve(settings: Settings) -> None:
    """`graph-agent serve`: run the API until SIGTERM/SIGINT, then drain and exit 0."""
    app = create_app(settings)
    config = uvicorn.Config(
        app,
        host=settings.api.host,
        port=settings.api.port,
        timeout_graceful_shutdown=int(graceful_wait_s(settings.limits.shutdown_grace_s)),
    )
    DrainingServer(config, app).run()
