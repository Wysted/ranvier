---
id: T07
title: Dev docker-compose Postgres and integration fixtures
part: 4
wave: B
depends_on: [T01]
unblocks: [T35, S4]
owner: sa-build-py
reviewer: sa-review-py
branch: task/T07-dev-compose
status: done
---

## Goal
A dev `docker-compose.yml` with Postgres and the pytest plumbing that gives integration tests an isolated database.

## Design references
- design.md §2 "Workspace" (`docker-compose.yml`)
- design.md §11 "Platform test layers" row Integration
- design.md §8 "Postgres connections"

## Scope: files this task may create or edit
- docker-compose.yml
- packages/core/tests/integration/conftest.py
Shared files: none.

## Out of scope
- Registering the `integration` marker and default `-m` filter (T02 owns pytest config)
- Migrations and repositories (T06, T13)
- App Dockerfile / starter compose (T34)

## Acceptance criteria
- [ ] `docker compose up -d` starts Postgres with a healthcheck, a named volume and a non-default host port (avoid clashing with a local 5432); dev credentials live only in the compose file
- [ ] `conftest.py`: session fixture `database_url` (env `RANVIER_TEST_DATABASE_URL`, default pointing at the compose service) that creates a throwaway database per session and drops it at the end; a function-scoped fixture that gives a clean schema; every test under `tests/integration/` is auto-marked `integration` through `pytest_collection_modifyitems`
- [ ] If the database is unreachable the fixture fails with a message telling to run `docker compose up -d` (no silent skip)
- [ ] `docker compose config` validates
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Postgres major version: pick the current stable one supported by `langgraph-checkpoint-postgres` and Procrastinate; state it in the compose file comment.
- Reference for fixtures: `agintis/tests/support/postgres.py`.
- Use psycopg (already a dependency), not asyncpg (banned).
- Conventions: follow the project's Python conventions and the decision log; never read `.env`; agintis is read-only reference.
