---
id: T06
title: Postgres pools, migration runner, 0001_init.sql
part: 4
wave: D
depends_on: [T04]
unblocks: [T11, T13, T32, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T06-db-migrations
status: done
---

## Goal
psycopg async pools (app and checkpointer), a numbered-SQL migration runner under an advisory lock, and the first migration creating `runs` and `usage`.

## Design references
- design.md §5 "Runs table (`graph_agent.runs`)" and its invariant
- design.md §9 "Usage ledger" column table
- design.md §10 "Persistence" table and OD-5
- design.md §8 rows "Postgres connections"

## Scope: files this task may create or edit
- packages/core/src/graph_agent/_internal/db/pool.py
- packages/core/src/graph_agent/_internal/db/migrate.py
- packages/core/src/graph_agent/_internal/db/migrations/0001_init.sql
- packages/core/tests/integration/db/test_migrate.py
- packages/core/tests/unit/db/test_migrate_files.py (file discovery/ordering only)
Shared files: none.

## Out of scope
- `AsyncPostgresSaver.setup()` and the Procrastinate schema (T11, T25; the `migrate` CLI sequences all three in T18/T25; db must not import runtime, contract C1)
- Procrastinate pool (T24)
- Repository code over the tables (T13, T32)
- Compose file and integration fixtures (T07)

## Acceptance criteria
- [ ] `pool.py`: `open_app_pool` and `open_checkpointer_pool` (the latter with `autocommit=True`, `prepare_threshold=0`, `row_factory=dict_row`), sizes from `database.*_pool_max`, one function logging the connection budget (sum of the three pools)
- [ ] `migrate.py`: `apply_migrations(pool)` takes `pg_advisory_lock`, creates schema `graph_agent` and `schema_version`, applies `NNNN_*.sql` files in order each in one transaction, is idempotent, and fails loudly if an applied version has no file
- [ ] `0001_init.sql`: `runs` with every column of design §5, status CHECK of the six values, CHECK that exactly one of `input`/`command` is set, partial unique index on `(thread_id) WHERE status = 'running'`, indexes for thread listing and for lease sweeps; `usage` with PK `(run_id, model, agent)` and the columns of design §9 (no foreign key to `runs`, so the ledger outlives thread deletion; say so in an SQL comment)
- [ ] Integration tests: migrations twice = no-op; two concurrent `apply_migrations` do not race; inserting a second `running` row for a thread violates the index
- [ ] SQL files are inside the built wheel (check `uv build` output)
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Reads files with `importlib.resources`, not relative paths.
- Do not read or print connection URLs containing passwords in logs.
- agintis used Alembic + SQLAlchemy (`/home/wysted/code/agintis/src/agintis/platform/db/`); OD-5 explicitly replaces that with ~60 lines of runner.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
