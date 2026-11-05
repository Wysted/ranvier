---
id: T33
title: Runner spans and runtime metrics
part: 10
wave: H
depends_on: [T10, T16]
unblocks: [T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T33-otel-runner
status: done
---

## Goal
Put the run/thread/flow attributes on the run span and publish the runner-level metrics through the metrics port.

## Design references
- design.md §9 "Observability" (attributes and metrics list)
- design.md §8 row "Loop health"
- design.md §2 contract C6

## Scope: files this task may create or edit
- packages/core/tests/unit/runtime/test_runner_observability.py
Shared files (append-only, keep edits minimal to avoid merge conflicts):
- packages/core/src/graph_agent/_internal/runtime/runner.py (T16, append: wrap the run in `run_span`, active-runs gauge, run duration/outcome counters)
- packages/core/src/graph_agent/_internal/bootstrap.py (append only: register any remaining gauge sources; T24 appends queue wiring in the same wave)

## Out of scope
- OTel SDK code (stays in `observability.otlp`, T10)
- Usage ledger (T32)

## Acceptance criteria
- [ ] Every run is wrapped by `run_span` with `graph_agent.flow/run_id/thread_id`; no `opentelemetry` import outside `observability.otlp` (C6)
- [ ] Gauges/counters published through the `Metrics` port: loop lag, executor occupancy, pool stats, semaphore waiters, active runs; with OTel off they go to logs
- [ ] Tests with a recording `Metrics` double: attributes set, active-runs gauge moves, outcome counted
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Gauge sources for limits and pools are registered in T15; here you add the runner ones and verify the whole set reaches the port.
- No new dependencies.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
