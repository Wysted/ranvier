---
id: T32
title: Usage ledger, repository and usage report
part: 10
wave: G
depends_on: [T06, T15]
unblocks: [T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T32-usage-ledger
status: done
---

## Goal
Aggregate `usage_metadata` per `(model, agent)` for each run, upsert it into `graph_agent.usage`, and report tokens and indicative cost with `genai-prices` at read time.

## Design references
- design.md §9 "Usage ledger" (paragraph and column table)
- design.md §6 "CLI" row `usage`
- design.md §6 SSE `end` event (usage totals)
- design.md §13 row "Usage ledger"

## Scope: files this task may create or edit
- packages/core/src/graph_agent/_internal/usage.py
- packages/core/tests/unit/test_usage.py
- packages/core/tests/integration/test_usage_repo.py
Shared files (append-only, keep edits minimal to avoid merge conflicts):
- packages/core/src/graph_agent/_internal/cli.py (append `_add_usage` and handler; T17 appends `validate` in the same wave)
- packages/core/src/graph_agent/_internal/bootstrap.py (append only: build the repository and register the `UsageObserver`; T16 appends other fields)

## Out of scope
- The runner itself (T16 already calls observers)
- Changing the `usage` table (T06 owns the migration; ask if a column is missing)
- Storing money (never)

## Acceptance criteria
- [ ] `UsageCallback` (LangChain async callback) accumulates counts per `(model, agent)` from `usage_metadata`, agent name from callback metadata as S3 documents (`""` for direct flow calls); thread-safe under concurrent tool calls
- [ ] `UsageObserver` implements `RunObserver` (T12): returns the callback, upserts at run end including failed/cancelled runs, accumulating over attempts (ON CONFLICT add); exposes `UsageTotals` for the `end` event
- [ ] `usage [--since] [--by flow|model|agent]` prints a table with tokens and indicative cost; models unknown to `genai-prices` are reported as unpriced, never as 0
- [ ] Unit tests for aggregation and pricing fallback; integration test for the upsert across two attempts
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Port from `/home/wysted/code/agintis/src/agintis/platform/usage/` (`repository.py`, `report.py`, `rows.py`) and `/home/wysted/code/agintis/src/agintis/agent_kit/usage.py`, `model_usage.py`; SQLAlchemy/Alembic and own `prices.yaml` go away.
- Raw SQL via psycopg; langchain allowed in `usage` (C5).
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
- REVIEW 1 RESULT (2026-11-04, changes_requested) — fix pass to run in the existing worktree .claude/worktrees/agent-a59470f72d958886f (branch task/T32-usage-ledger), rebase on main first (conflict in cli.py with T17's validate). Orchestrator decisions: (1) typed field `Container.usage: UsageObserver` (memory mode too); `observers` back to []; runner order: totals = usage.totals(ctx) -> RunOutcome(usage) -> EndEvent -> on_finish of [usage, *observers] in `finally` on every exit, never retried. (2) `AGENT_METADATA_KEY` lives in usage.py (C1: usage and observability are independent siblings); approved one-line edits in agents/factory.py and its test; delete it from tracing.py. (3) `usage` CLI: sync handler, lazy imports, shared `_load_settings()` with validate's ValidationError formatting (include_input=False), only DB fetch inside asyncio.run, build/format report outside the loop. Also: `_active` keyed by (run_id, attempt); on_llm_error counts `response` usage when present; report never prints 0 for unpriced-only lines, separate "calls without usage", catch genai-prices ValueError; drop `--by run` (not in design); real-thread concurrency test + streaming usage test with a fake that has `_astream` (GenericFakeChatModel drops usage when streaming); `app_pool is not None`; named pool-size constant; timedelta units; argparse default as string; sorted upsert params; document at-most-once ledger in the shortcut.
- SUPERSEDED (orchestrator, after the T16 review): item (1) above is replaced by the T16 plan — UsageObserver becomes stateless and the runner owns the per-attempt UsageCallback; usage totals come from RunOutcome.usage_by_key. Run this T32 fix pass AFTER T16 merges and rebase onto it.
- From the final T16 review (orchestrator, add to this fix pass): W1 in runner `_finish`, if `runs.finish` raises, notify observers with status `running` (usage) then re-raise, so the ledger is once per attempt on every path; W2 add `shortcut:` comments on `_settle` (terminal step not bounded by the shutdown budget; T26 bounds settled()); W3 make the lease-lost test tolerant (`1 <= beats <= 2`, assert LOST before lease_s + heartbeat_s); L1 merge `_drain` into `_hand_over`; L2 one `Counts` sum helper reused by `UsageCallback.totals()` and `_notify`; L3 drop `ActiveRuns.__len__` unless T26/T33 needs it as a gauge (T33 will: keep it if T33 uses it, otherwise delete). Also, T16 already created `_internal/usage.py` (Counts, UsageCallback, AGENT_METADATA_KEY, UNKNOWN_MODEL, NO_AGENT): on rebase keep T16's top section and append repository/observer/report; UsageObserver becomes stateless (`callbacks()` -> (), `on_finish` -> `repo.add(ctx, outcome.usage_by_key)`); no ledger observer in memory mode; replace bootstrap's `observers=[...]` line; removing `AGENT_METADATA_KEY` from tracing.py breaks test_runner.py:23 and :656-657 and the `shortcut:` in usage.py:16-17 — update them.
