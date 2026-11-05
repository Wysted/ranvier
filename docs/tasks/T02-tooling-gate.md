---
id: T02
title: Lint, types, import contracts, pytest and check.sh
part: 1
wave: B
depends_on: [T01]
unblocks: [T03, T04, T28, T35]
owner: sa-build-py
reviewer: sa-review-py
branch: task/T02-tooling-gate
status: todo
---

## Goal
Configure ruff (with `banned-api`), mypy strict, import-linter contracts C1-C6, pytest and blockbuster, and a single `scripts/check.sh` gate (ported from agintis).

## Design references
- design.md §2 "Import contracts (import-linter, v0.1)" table C1-C6 and the `banned-api` paragraph
- design.md §8 last row "Blocking-call detection"
- design.md §11 "Platform test layers"
- design.md §13 row "Contracts and gate"

## Scope: files this task may create or edit
- packages/core/pyproject.toml ([tool.ruff], [tool.mypy], [tool.importlinter], [tool.pytest.ini_options] sections only)
- scripts/check.sh
- packages/core/tests/conftest.py (blockbuster fixture)
Shared files: none.

## Out of scope
- Per-member clean-install and git-subdirectory checks (T03; check.sh stays the local gate)
- `integration` tests need Postgres: only register the marker here, the compose file and fixtures are T07
- Adding `examples` to the contracts (T20)

## Acceptance criteria
- [ ] ruff: lint + `format --check`; `banned-api` bans `mcp` and `httpx2` outside `agents/mcp.py` (tests excepted via per-file-ignores), and `langchain.mcp`, `fastmcp`, `langchain_mcp_adapters`, `langgraph.prebuilt.create_react_agent`, `langgraph_supervisor`, `asyncpg`, `fastapi.responses.ORJSONResponse` everywhere
- [ ] mypy `strict = true` on `packages/core/src` and tests; `py.typed` respected
- [ ] import-linter: contracts C1-C6 exactly as in design §2; modules that do not exist yet must not break the run (use optional layers / `allow_indirect`-free config, check the installed import-linter docs); each contract proven to fail by temporarily adding one violating import (do not commit it)
- [ ] pytest: `--import-mode=importlib` (test basenames repeat across dirs), asyncio auto mode, marker `integration` registered, default run is `-m "not integration"`; blockbuster activated for async tests via an autouse fixture
- [ ] `scripts/check.sh` runs, fail-fast and in this order: ruff check, ruff format --check, mypy, lint-imports, pytest; exits non-zero on any failure; it is green on the current empty package
- [ ] `scripts/check.sh` is executable and runs from any cwd (cd to repo root)
- [ ] `scripts/check.sh` green (from T02 on)
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Port from `/home/wysted/code/agintis/scripts/check.sh` and the `[tool.importlinter]` block of `/home/wysted/code/agintis/pyproject.toml` (17 contracts there, 6 here).
- Layer syntax for C1: `cli | api | worker` > `bootstrap` > `runtime` > `agents` > `llm | limits | usage | observability | queue` > `db | config` (siblings independent).
- Tests live in `packages/core/tests/{unit,integration}/` mirroring the `_internal` tree.
- Config only: no source code beyond the conftest fixture.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
