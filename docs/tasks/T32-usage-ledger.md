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
status: todo
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
