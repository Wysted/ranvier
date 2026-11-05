"""Wire DTOs of the HTTP API: requests, responses, the error body and the SSE mapping."""

import logging
import uuid
from datetime import datetime
from http import HTTPStatus
from typing import Any, Final, Literal, Self, assert_never

from fastapi.responses import JSONResponse
from fastapi.sse import ServerSentEvent
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_core import PydanticSerializationError, to_json

from graph_agent._internal.runtime.events import (
    CustomEvent,
    EndEvent,
    ErrorEvent,
    InterruptEvent,
    MessagesEvent,
    MetadataEvent,
    RunEvent,
    UpdatesEvent,
    ValuesEvent,
)
from graph_agent._internal.runtime.runner import DEFAULT_STREAM_MODES
from graph_agent._internal.runtime.runs import ErrorCode, Run, RunError, RunStatus

logger = logging.getLogger(__name__)

StreamMode = Literal["updates", "values", "messages", "custom"]

# The one place an error code becomes an HTTP status.
ERROR_STATUS: Final[dict[ErrorCode, HTTPStatus]] = {
    ErrorCode.UNAUTHORIZED: HTTPStatus.UNAUTHORIZED,
    ErrorCode.NOT_FOUND: HTTPStatus.NOT_FOUND,
    ErrorCode.FLOW_NOT_FOUND: HTTPStatus.NOT_FOUND,
    ErrorCode.INVALID_REQUEST: HTTPStatus.UNPROCESSABLE_ENTITY,
    ErrorCode.THREAD_BUSY: HTTPStatus.CONFLICT,
    ErrorCode.THREAD_INTERRUPTED: HTTPStatus.CONFLICT,
    ErrorCode.FLOW_VERSION_MISMATCH: HTTPStatus.CONFLICT,
    ErrorCode.TOO_MANY_RUNS: HTTPStatus.TOO_MANY_REQUESTS,
    ErrorCode.SHUTTING_DOWN: HTTPStatus.SERVICE_UNAVAILABLE,
    # Canonical DEADLINE_EXCEEDED mapping; 408 would blame the client.
    ErrorCode.RUN_TIMEOUT: HTTPStatus.GATEWAY_TIMEOUT,
    ErrorCode.FLOW_BUILD_FAILED: HTTPStatus.SERVICE_UNAVAILABLE,
    ErrorCode.ORPHAN_RETRIES_EXHAUSTED: HTTPStatus.INTERNAL_SERVER_ERROR,
    ErrorCode.INTERNAL_ERROR: HTTPStatus.INTERNAL_SERVER_ERROR,
    ErrorCode.QUEUE_UNAVAILABLE: HTTPStatus.SERVICE_UNAVAILABLE,
    # Only ever an SSE `error` frame (the response is already 200); 503 is for completeness.
    ErrorCode.RUN_HANDED_OVER: HTTPStatus.SERVICE_UNAVAILABLE,
}

RETRY_AFTER_S: Final = 1  # a 429 clears when an admission slot frees up; one second is plenty


def error_response(
    status: HTTPStatus, code: str, message: str, retryable: bool = False
) -> JSONResponse:
    """The one error body: `{code, message, retryable}`; 401/429 carry their RFC headers."""
    headers: dict[str, str] = {}
    if status == HTTPStatus.UNAUTHORIZED:
        headers["WWW-Authenticate"] = "Bearer"
    elif status == HTTPStatus.TOO_MANY_REQUESTS:
        headers["Retry-After"] = str(RETRY_AFTER_S)
    body = {"code": code, "message": message, "retryable": retryable}
    return JSONResponse(body, status_code=status, headers=headers)


class _Dto(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ErrorBody(_Dto):
    code: ErrorCode
    message: str
    retryable: bool

    @classmethod
    def from_error(cls, error: RunError) -> Self:
        return cls.model_validate(error.as_dict())


class ResumeCommand(_Dto):
    # A bare `resume: {}` is an empty interrupt map in LangGraph, not a plain value.
    resume: Any

    @field_validator("resume")
    @classmethod
    def _resume_not_null(cls, value: Any) -> Any:
        # LangGraph raises EmptyInputError on a None resume.
        if value is None:
            raise ValueError("'resume' must not be null")
        return value


class RunRequest(_Dto):
    flow: str
    input: Any = None
    command: ResumeCommand | None = None
    stream_mode: list[StreamMode] = Field(
        default_factory=lambda: list[StreamMode](DEFAULT_STREAM_MODES)
    )
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _exactly_one_of_input_command(self) -> Self:
        # JSON null counts as absent, consistent with the DB CHECK.
        if (self.input is None) == (self.command is None):
            raise ValueError("exactly one of 'input' or 'command' is required")
        return self


class RunAccepted(_Dto):
    run_id: uuid.UUID
    thread_id: uuid.UUID


class RunOut(_Dto):
    run_id: uuid.UUID
    thread_id: uuid.UUID
    flow: str
    flow_version: int
    status: RunStatus
    error: ErrorBody | None
    attempt: int
    cancel_requested: bool
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    metadata: dict[str, Any]

    @classmethod
    def from_run(cls, run: Run) -> Self:
        return cls(
            run_id=run.run_id,
            thread_id=run.thread_id,
            flow=run.flow,
            flow_version=run.flow_version,
            status=run.status,
            error=ErrorBody.from_error(run.error) if run.error else None,
            attempt=run.attempt,
            cancel_requested=run.cancel_requested,
            created_at=run.created_at,
            started_at=run.started_at,
            finished_at=run.finished_at,
            metadata=run.metadata,
        )


class FlowItem(_Dto):
    name: str
    version: int
    description: str


class InterruptOut(_Dto):
    id: str
    value: Any


class ThreadState(_Dto):
    values: Any
    next: list[str]
    interrupts: list[InterruptOut]
    checkpoint_id: str | None


def _event_data(event: RunEvent) -> tuple[str, dict[str, Any]]:
    """Event name and `data` DTO per the design section 6 table."""
    match event:
        case MetadataEvent():
            return "metadata", {
                "run_id": event.run_id,
                "thread_id": event.thread_id,
                "flow": event.flow,
                "flow_version": event.flow_version,
                "attempt": event.attempt,
            }
        case UpdatesEvent():
            return "updates", {"ns": event.ns, "node": event.node, "update": event.update}
        case ValuesEvent():
            return "values", {"ns": event.ns, "values": event.values}
        case MessagesEvent():
            return "messages", {
                "ns": event.ns,
                "node": event.node,
                "id": event.id,
                "type": event.type,
                "content": event.content,
                "tool_call_chunks": event.tool_call_chunks,
            }
        case CustomEvent():
            return "custom", {"ns": event.ns, "node": event.node, "data": event.data}
        case InterruptEvent():
            return "interrupt", {
                "interrupts": [{"id": item.id, "value": item.value} for item in event.interrupts]
            }
        case EndEvent():
            return "end", {
                "status": event.status,
                "usage": {
                    "input_tokens": event.usage.input_tokens,
                    "output_tokens": event.usage.output_tokens,
                    "cached_input_tokens": event.usage.cached_input_tokens,
                },
            }
        case ErrorEvent():
            return "error", {
                "code": event.code,
                "message": event.message,
                "retryable": event.retryable,
            }
        case _:
            assert_never(event)


class SseEncoder:
    """Maps run events to SSE frames with ids `{run_id}:{seq}`, `seq` counting from 1."""

    def __init__(self, run_id: uuid.UUID | str) -> None:
        self._run_id = str(run_id)
        self._seq = 0
        self._failed = False

    def handed_over(self) -> ServerSentEvent:
        """Terminal frame of an inline run that continues elsewhere (drained for shutdown)."""
        self._seq += 1
        body = {
            "code": ErrorCode.RUN_HANDED_OVER.value,
            "message": "run handed over to another executor; poll the run for its result",
            "retryable": True,
            "run_id": self._run_id,
        }
        return ServerSentEvent(
            event="error", id=f"{self._run_id}:{self._seq}", raw_data=to_json(body).decode()
        )

    def encode(self, event: RunEvent) -> ServerSentEvent:
        """Raises after an `error` frame was emitted: the caller must stop and cancel the run."""
        if self._failed:
            raise RuntimeError("SSE stream already failed; stop and cancel/mark the run")
        name, data = _event_data(event)
        self._seq += 1
        try:
            # Serialize here: the framework would fail mid-stream, after the frame was accepted.
            payload = to_json(data, inf_nan_mode="null").decode()
        except PydanticSerializationError as exc:
            # Terminal `error` frame instead of a silent drop; the caller ends the stream.
            logger.error("SSE %r payload not serializable: %s", name, exc)
            self._failed = True
            payload = to_json(
                {
                    "code": ErrorCode.INTERNAL_ERROR.value,
                    "message": f"{name!r} payload is not JSON-serializable ({type(exc).__name__})",
                    "retryable": False,
                }
            ).decode()
            name = "error"
        return ServerSentEvent(event=name, id=f"{self._run_id}:{self._seq}", raw_data=payload)
