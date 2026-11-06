---
id: T10
title: Tracing port and lazy OTLP adapter
part: 3
wave: D
depends_on: [T04]
unblocks: [T15, T33, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T10-tracing-port
status: done
---

## Goal
Observability behind a port: span attribute names, foreign-tracing guard, a metrics sink that logs by default, and a lazily imported OTLP + OpenInference adapter.

## Design references
- design.md §9 "Observability"
- design.md §2 contract C6 and layout rows `observability/tracing.py`, `otlp.py`
- design.md §13 row "Tracing"

## Scope: files this task may create or edit
- packages/core/src/ranvier/_internal/observability/tracing.py
- packages/core/src/ranvier/_internal/observability/otlp.py
- packages/core/tests/unit/observability/test_tracing.py
Shared files: none.

## Out of scope
- Using the port from the runner and publishing runtime gauges (T33)
- Container wiring (T15)

## Acceptance criteria
- [ ] `tracing.py`: constants `ranvier.flow`, `ranvier.run_id`, `ranvier.thread_id`; `guard_foreign_tracing(environ)` raising when LangSmith tracing variables are on; a `Metrics` protocol (`gauge`, `counter`) with a logging implementation; `run_span(...)` context manager that is a no-op when tracing is disabled; `setup_observability(settings)` returning the handle
- [ ] `otlp.py` is imported only from `tracing.py`, only when `observability` enables OTel, and only the `[otel]` extra imports opentelemetry/openinference; missing extra gives an error naming it
- [ ] OpenInference LangChain instrumentation exported via OTLP; metrics via OTel meter when enabled
- [ ] Tests: guard on/off; disabled mode never imports `otlp` or `opentelemetry` (assert on `sys.modules`); metrics fall back to logs
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Port from `/home/wysted/code/agintis/src/agintis/platform/observability/tracing.py`, `otlp.py` and `/home/wysted/code/agintis/src/agintis/bootstrap/observability.py` (attributes renamed).
- Contract C6: opentelemetry/openinference only in `observability.otlp`.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
