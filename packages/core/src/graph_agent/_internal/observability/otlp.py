"""OTLP/HTTP adapter: OpenInference LangChain instrumentation, run spans and OTel metrics.

Imported only by `tracing.setup_observability` when OTel is enabled (needs the `[otel]` extra).
The providers are ours and never installed as the global ones; the LangChain instrumentor is a
process singleton, so a second backend without a `shutdown()` in between is refused.
"""

import logging
from contextlib import AbstractContextManager

from openinference.instrumentation.langchain import LangChainInstrumentor
from opentelemetry import metrics as otel_metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from graph_agent._internal.observability.tracing import Attributes, TracingError

logger = logging.getLogger(__name__)

SERVICE = "graph-agent"
PROJECT_NAME = "openinference.project.name"  # the project Phoenix files traces under
RUN_SPAN_NAME = "graph_agent.run"
_TRACES_PATH = "/v1/traces"
_METRICS_PATH = "/v1/metrics"


class OtelMetrics:
    """`Metrics` over an OTel meter; instruments are created on first use."""

    def __init__(self, meter: otel_metrics.Meter) -> None:
        self._meter = meter
        # shortcut: private _Gauge (no public type in the pinned API), switch to Gauge when exported
        self._gauges: dict[str, otel_metrics._Gauge] = {}
        self._counters: dict[str, otel_metrics.Counter] = {}

    def gauge(self, name: str, value: float, attributes: Attributes | None = None) -> None:
        if name not in self._gauges:
            self._gauges[name] = self._meter.create_gauge(name)
        self._gauges[name].set(value, dict(attributes or {}))

    def counter(self, name: str, value: float = 1, attributes: Attributes | None = None) -> None:
        if name not in self._counters:
            self._counters[name] = self._meter.create_counter(name)
        self._counters[name].add(value, dict(attributes or {}))


class OtlpBackend:
    """`endpoint` is an OTLP/HTTP base URL; None leaves it to the SDK defaults / OTEL_* env."""

    def __init__(self, endpoint: str | None) -> None:
        instrumentor = LangChainInstrumentor()
        if instrumentor.is_instrumented_by_opentelemetry:
            raise TracingError(
                "LangChain is already instrumented in this process: shut the previous "
                "observability handle down first"
            )
        base = endpoint.rstrip("/") if endpoint else None
        resource = Resource.create({SERVICE_NAME: SERVICE, PROJECT_NAME: SERVICE})
        self._tracer_provider = TracerProvider(resource=resource)
        self._tracer_provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=base and base + _TRACES_PATH))
        )
        self._meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[
                PeriodicExportingMetricReader(
                    OTLPMetricExporter(endpoint=base and base + _METRICS_PATH)
                )
            ],
        )
        try:
            instrumentor.instrument(
                tracer_provider=self._tracer_provider, raise_exception_on_conflict=True
            )
        except Exception as exc:
            self._release()
            raise TracingError(f"Cannot instrument LangChain: {exc}") from exc
        self._tracer = self._tracer_provider.get_tracer(SERVICE)
        self.metrics = OtelMetrics(self._meter_provider.get_meter(SERVICE))
        self._closed = False
        logger.info("OpenTelemetry on: OTLP/HTTP to %s", base or "SDK default endpoint")

    def run_span(self, attributes: Attributes) -> AbstractContextManager[object]:
        return self._tracer.start_as_current_span(RUN_SPAN_NAME, attributes=dict(attributes))

    def _release(self) -> None:
        self._tracer_provider.shutdown()
        self._meter_provider.shutdown()

    def shutdown(self) -> None:
        if self._closed:
            return
        self._closed = True
        LangChainInstrumentor().uninstrument()
        self._release()
