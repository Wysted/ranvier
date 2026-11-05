import logging
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest

from graph_agent._internal.config.settings import Settings
from graph_agent._internal.observability import tracing
from graph_agent._internal.observability.tracing import (
    LoggingMetrics,
    Observability,
    TracingError,
    guard_foreign_tracing,
    setup_observability,
)

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

_OTLP = "graph_agent._internal.observability.otlp"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in tracing.FOREIGN_TRACING_VARIABLES:
        monkeypatch.delenv(name, raising=False)


def _settings(**observability: object) -> Settings:
    return Settings.model_validate({"observability": observability})


@pytest.mark.parametrize("value", ["1", "true", "TRUE", " yes ", "on"])
def test_guard_rejects_truthy(value: str) -> None:
    with pytest.raises(TracingError, match="LANGSMITH_TRACING"):
        guard_foreign_tracing({"LANGSMITH_TRACING": value})


@pytest.mark.parametrize(
    "environ", [{}, {"LANGSMITH_TRACING": "false"}, {"LANGCHAIN_TRACING_V2": ""}]
)
def test_guard_passes_when_off(environ: dict[str, str]) -> None:
    guard_foreign_tracing(environ)


def test_setup_runs_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LANGCHAIN_TRACING_V2", "true")
    with pytest.raises(TracingError):
        setup_observability(_settings())


def test_disabled_never_imports_otel() -> None:
    code = (
        "import sys\n"
        "from graph_agent._internal.config.settings import Settings\n"
        "from graph_agent._internal.observability.tracing import setup_observability\n"
        "obs = setup_observability(Settings(_env_file=None))\n"
        "with obs.run_span(flow='f', run_id='r', thread_id='t'):\n"
        "    pass\n"
        "bad = [m for m in sys.modules if m.split('.')[0] in ('opentelemetry', 'openinference')"
        " or m.endswith('observability.otlp')]\n"
        "assert not bad, bad\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_disabled_metrics_log(caplog: pytest.LogCaptureFixture) -> None:
    obs = setup_observability(_settings())
    assert isinstance(obs.metrics, LoggingMetrics)
    assert obs.backend is None
    with caplog.at_level(logging.DEBUG, logger=tracing.__name__):
        obs.metrics.gauge("active_runs", 3, {"flow": "f"})
        obs.metrics.counter("runs", 1)
    assert "active_runs=3" in caplog.text
    assert "runs+=1" in caplog.text
    obs.shutdown()


def test_missing_extra_names_it(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, _OTLP, raising=False)
    monkeypatch.setitem(sys.modules, "opentelemetry", None)  # makes `import opentelemetry` fail
    with pytest.raises(TracingError, match="'otel' extra"):
        setup_observability(_settings(otel_enabled=True))


def _enabled(
    monkeypatch: pytest.MonkeyPatch, **observability: object
) -> tuple[Observability, "InMemorySpanExporter", "InMemoryMetricReader"]:
    pytest.importorskip("opentelemetry.sdk")
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    from graph_agent._internal.observability import otlp

    spans = InMemorySpanExporter()
    reader = InMemoryMetricReader()
    monkeypatch.setattr(otlp, "OTLPSpanExporter", lambda **_: spans)
    monkeypatch.setattr(otlp, "BatchSpanProcessor", SimpleSpanProcessor)
    monkeypatch.setattr(otlp, "OTLPMetricExporter", lambda **_: None)
    monkeypatch.setattr(otlp, "PeriodicExportingMetricReader", lambda _e: reader)
    return setup_observability(_settings(otel_enabled=True, **observability)), spans, reader


def test_enabled_run_span_children_and_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    from langchain_core.runnables import RunnableLambda
    from opentelemetry import trace

    obs, spans, reader = _enabled(monkeypatch)
    try:
        with obs.run_span(flow="f", run_id="r", thread_id="t"):
            RunnableLambda(lambda x: x).invoke(1)
        obs.metrics.counter("runs")
        finished = spans.get_finished_spans()
        run = next(s for s in finished if s.name == "graph_agent.run")
        assert dict(run.attributes or {}) == {
            "graph_agent.flow": "f",
            "graph_agent.run_id": "r",
            "graph_agent.thread_id": "t",
            "session.id": "t",
        }
        child = next(s for s in finished if s is not run)
        assert child.parent is not None and child.parent.span_id == run.context.span_id
        assert (child.attributes or {}).get("session.id") == "t"
        assert isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider)
        data = reader.get_metrics_data()
        assert data is not None
        names = [
            m.name for rm in data.resource_metrics for sm in rm.scope_metrics for m in sm.metrics
        ]
        assert names == ["runs"]
    finally:
        obs.shutdown()
        obs.shutdown()  # idempotent
    obs2, _, _ = _enabled(monkeypatch)  # setup again after shutdown
    obs2.shutdown()


@pytest.mark.parametrize("suffix", ["/v1/traces", "/v1/metrics/"])
def test_endpoint_with_signal_path_rejected(monkeypatch: pytest.MonkeyPatch, suffix: str) -> None:
    pytest.importorskip("opentelemetry.sdk")
    with pytest.raises(TracingError, match="base URL") as err:
        setup_observability(
            _settings(otel_enabled=True, otel_endpoint=f"http://user:secret@h:4318{suffix}")
        )
    assert "secret" not in str(err.value)


def test_endpoint_for_log_strips_credentials() -> None:
    pytest.importorskip("opentelemetry.sdk")
    from graph_agent._internal.observability.otlp import endpoint_for_log

    assert endpoint_for_log("https://u:p@host:4318/x?k=1") == "https://host:4318"
