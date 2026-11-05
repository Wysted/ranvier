"""Observability port: span attribute names, foreign-tracing guard, metrics sink, run spans.

Nothing here imports opentelemetry/openinference (contract C6). The OTLP adapter in `otlp.py`
is imported lazily by `setup_observability`, only when `observability.otel_enabled` is on, and
needs the `[otel]` extra.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    # Annotation only: the facade imports this module (AGENT_METADATA_KEY) and must stay light.
    from graph_agent._internal.config.settings import Settings


logger = logging.getLogger(__name__)

ATTR_FLOW = "graph_agent.flow"
ATTR_RUN_ID = "graph_agent.run_id"
ATTR_THREAD_ID = "graph_agent.thread_id"
# Callback metadata key carrying AgentSpec.name; the usage ledger reads it (S3).
AGENT_METADATA_KEY = "graph_agent_agent"

OTEL_EXTRA = "otel"
_OTEL_PACKAGES = frozenset({"opentelemetry", "openinference"})

# Environment variables that switch LangChain's own (LangSmith) tracing on.
FOREIGN_TRACING_VARIABLES = (
    "LANGSMITH_TRACING",
    "LANGSMITH_TRACING_V2",
    "LANGCHAIN_TRACING_V2",
    "LANGCHAIN_TRACING",
)
_TRUTHY = frozenset({"1", "true", "yes", "on"})

type Attributes = Mapping[str, str]


class TracingError(RuntimeError):
    """Tracing is misconfigured: the app must not start."""


def guard_foreign_tracing(environ: Mapping[str, str] | None = None) -> None:
    """Fail closed when LangChain's own tracing is on (strict on purpose: 1/true/yes/on)."""
    env = os.environ if environ is None else environ
    active = [
        name for name in FOREIGN_TRACING_VARIABLES if env.get(name, "").strip().lower() in _TRUTHY
    ]
    if active:
        raise TracingError(
            f"{', '.join(active)} enables LangChain's own tracing, which would bypass the "
            "observability port. Unset it and enable observability.otel_enabled instead "
            "(LangSmith accepts OTLP traces)."
        )


class Metrics(Protocol):
    def gauge(self, name: str, value: float, attributes: Attributes | None = None) -> None: ...

    def counter(
        self, name: str, value: float = 1, attributes: Attributes | None = None
    ) -> None: ...


class LoggingMetrics:
    """Default sink when OTel is off: one log line per sample."""

    def gauge(self, name: str, value: float, attributes: Attributes | None = None) -> None:
        logger.debug("gauge %s=%s %s", name, value, dict(attributes or {}))

    def counter(self, name: str, value: float = 1, attributes: Attributes | None = None) -> None:
        logger.debug("counter %s+=%s %s", name, value, dict(attributes or {}))


class OtelBackend(Protocol):
    """What `otlp.OtlpBackend` provides to the handle."""

    @property
    def metrics(self) -> Metrics: ...

    def run_span(self, attributes: Attributes) -> AbstractContextManager[object]: ...

    def shutdown(self) -> None: ...


@dataclass(frozen=True, slots=True)
class Observability:
    """Handle returned by `setup_observability`; `backend` is None when OTel is off."""

    metrics: Metrics
    backend: OtelBackend | None = None

    def run_span(self, *, flow: str, run_id: str, thread_id: str) -> AbstractContextManager[object]:
        """Span around one run; a no-op when tracing is disabled."""
        if self.backend is None:
            return nullcontext()
        return self.backend.run_span(
            {ATTR_FLOW: flow, ATTR_RUN_ID: run_id, ATTR_THREAD_ID: thread_id}
        )

    def shutdown(self) -> None:
        """Flush and release the exporters (blocking); idempotent."""
        if self.backend is not None:
            self.backend.shutdown()


def setup_observability(settings: Settings) -> Observability:
    """Run the foreign-tracing guard, then build the handle the settings ask for."""
    guard_foreign_tracing()
    obs = settings.observability
    if not obs.otel_enabled:
        return Observability(metrics=LoggingMetrics())
    try:
        from graph_agent._internal.observability.otlp import OtlpBackend
    except ImportError as exc:
        if (exc.name or "").partition(".")[0] not in _OTEL_PACKAGES:
            raise  # a bug in the adapter, not a missing extra
        raise TracingError(
            "observability.otel_enabled needs the OpenTelemetry packages: install the "
            f"'{OTEL_EXTRA}' extra (uv sync --extra {OTEL_EXTRA}). Missing: {exc.name}"
        ) from exc
    backend = OtlpBackend(obs.otel_endpoint)
    return Observability(metrics=backend.metrics, backend=backend)
