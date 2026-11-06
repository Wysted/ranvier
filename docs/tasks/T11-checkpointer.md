---
id: T11
title: Checkpointer construction (Postgres and in-memory)
part: 4
wave: E
depends_on: [T06]
unblocks: [T15, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T11-checkpointer
status: done
---

## Goal
Build `AsyncPostgresSaver` over the checkpointer pool (or `InMemorySaver`), with an idempotent `setup`.

## Design references
- design.md §10 "Persistence" row "LangGraph checkpoints"
- design.md §8 row "Postgres connections"
- design.md §13 row "Checkpointer pool"
- design.md §14 open check "`AsyncPostgresSaver` serializes writes per instance"

## Scope: files this task may create or edit
- packages/core/src/ranvier/_internal/runtime/checkpointer.py
- packages/core/tests/unit/runtime/test_checkpointer.py
- packages/core/tests/integration/runtime/test_checkpointer.py
Shared files: none.

## Out of scope
- Calling `setup` from the `migrate` CLI (T18 sequences it; `db.migrate` cannot import runtime, contract C1; tasks.md says 'in migrate', see the report)
- Pruning (T25)
- Container wiring (T15)

## Acceptance criteria
- [ ] `create_checkpointer(pool)` returns an `AsyncPostgresSaver` bound to the given pool; `create_memory_checkpointer()` returns `InMemorySaver`
- [ ] `async setup_checkpointer(pool)` is idempotent
- [ ] Integration test: setup twice, then put/get a checkpoint round trip
- [ ] No private pool is created here; lifecycle belongs to the container
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Port from `/home/wysted/code/agintis/src/agintis/platform/checkpointer/postgres.py` and `/home/wysted/code/agintis/src/agintis/bootstrap/checkpointer.py`.
- S4 verifies the per-instance write-lock claim; if it holds, leave a `shortcut:` comment or a note for T15 about checkpointer pool sizing instead of designing around it here.
- langgraph imports are allowed in `runtime` (C5).
- Conventions: follow the project's Python conventions and the decision log; never read `.env`; agintis is read-only reference.
