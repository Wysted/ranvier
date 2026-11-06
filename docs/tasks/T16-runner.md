---
id: T16
title: execute_run: claim, stream, heartbeat, timeout, cancel
part: 5
wave: G
depends_on: [T13, T14, T15, S1]
unblocks: [T18, T19, T23, T24, T33, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T16-runner
status: done
---

## Goal
The single place a flow runs: a DB-free `stream_graph` core plus `execute_run` that claims, heartbeats, times out, cancels and finishes a run row.

## Design references
- design.md §5 "Single runner" (steps 1-5 and the paragraph on input choice)
- design.md §5 "Transitions" table
- design.md §5 "Interrupts, cancellation, idempotency"
- design.md §8 rows "Run timeout" and "Node timeout"
- design.md §13 row "Runner"

## Scope: files this task may create or edit
- packages/core/src/ranvier/_internal/runtime/runner.py
- packages/core/tests/unit/runtime/test_runner.py
- packages/core/tests/integration/runtime/test_runner.py
Shared files (append-only, keep edits minimal to avoid merge conflicts):
- packages/core/src/ranvier/_internal/bootstrap.py (append only: add `flows` (FlowRegistry), `queue: RunQueue | None`; T32 appends the usage observer wiring in the same wave)

## Out of scope
- CLI commands (T17, T18)
- HTTP/SSE layer and admission (T21, T23)
- Procrastinate implementation of `RunQueue` (T24)
- Usage ledger (T32), OTel spans/metrics (T33)
- SIGTERM drain (T26 appends)

## Acceptance criteria
- [ ] `stream_graph(compiled, input_or_command, config, ctx, stream_modes, observers) -> AsyncIterator[RunEvent]`: no DB, uses `astream(..., context=RunContext, subgraphs=True, version="v2", durability=flow.durability)`, maps parts with `map_stream_part`; reused by `run --memory` (T18) and `testing.run_flow` (T19)
- [ ] `execute_run(container, run_id)` async generator of `RunEvent`: claim (lost -> returns; busy -> typed outcome), resolve flow + version check, `stream_graph` inside `asyncio.timeout(flow.timeout_s)`, heartbeat task every `runs.heartbeat_s` that extends the lease and cancels the stream on `cancel_requested` or a lost lease, terminal write via fenced `finish`; closing the generator (client disconnect) cancels the run
- [ ] Outcomes: success, `interrupted` (stream reports interrupts; `interrupt` event then `end`), `error` (`run_timeout`, `flow_build_failed`, `flow_version_mismatch`, `internal_error` with `retryable`), `cancelled`; the `end` event carries usage totals from observers
- [ ] Recovery attempt input rule: `None` if the latest checkpoint already carries this `run_id` in metadata, else the original input (per S1); first attempt uses input or `Command(resume=...)`
- [ ] `RunQueue` Protocol (`async defer(run_id, thread_id)`) defined here for T23/T24/T26
- [ ] Tests: contract test with `InMemorySaver` and a toy flow (success, interrupt, timeout); integration test with Postgres (claim, cancel mid-run via flag, lost lease)
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- design.md §5 says `execute_run` is the only way a flow runs, but §6 says `run --memory` uses no database; splitting out `stream_graph` resolves this. Mention it in the task report so design.md can say so.
- Read `docs/spikes/S1.md` (RunControl, context without `context_schema`, `metadata.run_id`).
- Port the stream mapping and interrupt-by-id resume from `/home/wysted/code/agintis/src/agintis/agent_kit/runtime/langgraph_runner.py`.
- Sync/blocking work must not run on the loop (blockbuster is on in tests).
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
- From the T13 review (orchestrator decision, design §5): use the lease token returned by `claim` for heartbeat/finish; a claim may return the run as `cancelled` when `cancel_requested` was set.
- From the T21 review (orchestrator): always request the `updates` stream mode from `astream` (interrupts are detected via `updates.__interrupt__`) and filter to the client's requested modes afterwards; when merging request `metadata` into `config["metadata"]`, platform keys (`run_id`, `thread_id`) always win so a client cannot spoof the S1 recovery check.
- From the T15 review (orchestrator decision): `Container.runs` is None in memory mode; `execute_run` raises loudly when `runs is None` (no fallback, no in-memory repository); `run --memory` goes through `stream_graph` (R2) with InMemorySaver and reports usage only in the `end` event. The Container exposes the shared shutdown deadline (T26 drain uses it).
- REVIEW 1 RESULT (2026-11-04, changes_requested) — fix pass in existing worktree .claude/worktrees/agent-a7ecac2895542df68 (branch task/T16-runner), `git rebase main` first; T16 merges BEFORE T32. Plan (all approved, incl. scope extension to runtime/context.py, usage.py and events.py):
  B1/B2/M2: one try/except BaseException covering everything after the claim (including flows.get and _initial_input); heartbeat starts right after the claim; cleanup (cancel pump+heartbeat, then finish CANCELLED) runs in a separate task awaited via asyncio.shield, strong ref in a module set, bounded by the shutdown budget (anyio re-cancels every await in a cancelled scope). Tests: consumer task cancelled twice while waiting -> finished == [(CANCELLED, None)]; cancellation during flows.get.
  M1: track last successful heartbeat; if loop.time() - last >= lease_s treat as LOST (cancel pump, write nothing). Test.
  Usage (single source, replaces T32 review item 1): delete UsageCollector; runner owns a T32 UsageCallback per attempt passed to stream_graph; EndEvent.usage from callback.totals(); RunOutcome gains usage_by_key (snapshot); UsageObserver becomes stateless (callbacks() -> (), on_finish -> repo.add(ctx, outcome.usage_by_key)); observers called once per streamed attempt incl. drain/lease-lost (status running); memory mode registers no ledger observer.
  T26 registry: `ActiveRuns` (set of RunControl, track() context manager, request_drain(), draining flag) in runner.py, held by Container.active_runs and RunnerResources; drain test stops monkeypatching RunControl.
  Bounded queue: asyncio.Queue(RUN_EVENT_BUFFER=256) with blocking put (backpressure), pump finally -> queue.shutdown(); delete _Done sentinel. Note for T23: EventSourceResponse(send_timeout=...).
  context.py: FlowContext `flow`/`config` as read-only @property; drop the bootstrap type: ignore + shortcut.
  bootstrap: agent_fn=partial(build_agent, models=llm.model, registry=tools, mcp=mcp); pre-resolve configured tools in a thread like _prime_models.
  Minors: validate stream modes before the claim; CANCELLED claim path emits metadata first; replace the assert on run.command with an explicit error; _OWNER = hostname:pid; docstring: inline THREAD_BUSY callers must request_cancel the pending row; drop the FlowRunError alias (T17 made flows.py raise T13 RunError).
  Open for design: a drained inline run leaves the client without a terminal event (document or add a retryable code); stream_graph does not apply the run timeout — T18 must reuse the pump pattern.
- DONE after fix loop 2 (approved). Follow-ups W1-W3, L1-L3 of the final review moved to T32's fix pass.
