import logging
import subprocess
import sys

import pytest

from graph_agent._internal.config.settings import Settings
from graph_agent._internal.observability import tracing
from graph_agent._internal.observability.tracing import (
    LoggingMetrics,
    TracingError,
    guard_foreign_tracing,
    setup_observability,
)

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
    assert not obs.enabled
    with caplog.at_level(logging.INFO, logger=tracing.__name__):
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


def test_enabled_exports_run_span() -> None:
    pytest.importorskip("opentelemetry.sdk")
    obs = setup_observability(_settings(otel_enabled=True, otel_endpoint="http://127.0.0.1:1"))
    try:
        assert obs.enabled
        with obs.run_span(flow="f", run_id="r", thread_id="t"):
            pass
        obs.metrics.counter("runs")
    finally:
        obs.shutdown()
        obs.shutdown()  # idempotent
