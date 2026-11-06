---
id: T01
title: Workspace skeleton and package metadata
part: 1
wave: A
depends_on: []
unblocks: [T02, T07, T35, S1, S2, S3, S4, S5]
owner: sa-build-py
reviewer: sa-review-py
branch: task/T01-workspace-skeleton
status: done
---

## Goal
Initialise the git repo and the uv workspace, and create the empty `ranvier` package (all subpackages, `py.typed`, a `cli.py` skeleton) with every runtime dependency and extra declared once, so later tasks never fight over `pyproject.toml`.

## Design references
- design.md §2 "Workspace" (tree)
- design.md §2 "Core package" (tree)
- design.md "Decisions at a glance" rows Python, Name, MCP
- design.md §6 "CLI" (console script `ranvier`)
- design.md §14 part 1

## Scope: files this task may create or edit
- .gitignore
- pyproject.toml (uv workspace root: dev dependency group only, members = ["packages/core"])
- uv.lock
- packages/core/pyproject.toml (name, version 0.1.0, requires-python >=3.13, hatchling, deps, extras, script `ranvier = ranvier._internal.cli:main`)
- packages/core/src/ranvier/__init__.py (only `__version__` from importlib.metadata and `__all__ = ["__version__"]`; later tasks append)
- packages/core/src/ranvier/py.typed
- packages/core/src/ranvier/_internal/__init__.py
- packages/core/src/ranvier/_internal/{config,db,observability,agents,runtime,api}/__init__.py (empty)
- packages/core/src/ranvier/_internal/cli.py (argparse skeleton: `main(argv=None) -> int`, a `_build_parser()` with an empty subparsers group; each later task adds one `_add_<name>(sub)` function plus one line in a list)
- packages/core/tests/unit/.gitkeep
- packages/core/tests/integration/.gitkeep
Shared files: none.

## Out of scope
- Ruff/mypy/import-linter/pytest config and `scripts/check.sh` (T02)
- CI (T03)
- `docker-compose.yml` (T07)
- `examples/` workspace member (T20; add it to the root members then)
- `starter/` and `copier.yml` (T34)
- Any module other than the ones listed (no stubs for llm.py, limits.py, etc.)

## Acceptance criteria
- [ ] `git init` done on `main`; initial commit contains the existing `docs/`, `reports/`, `research_notes/` (this task runs in the repo root, no worktree is possible before git exists); work then continues on `task/T01-workspace-skeleton`
- [ ] `.gitignore` covers `.env`, `.venv`, `__pycache__`, `.mypy_cache`, `.ruff_cache`, `dist/`; no `.env` file is created or read
- [ ] Runtime deps: langgraph 1.x, `langchain==1.4.*` (pinned, see `_shared/python.md`), langgraph-checkpoint-postgres, psycopg[binary,pool], pydantic>=2, pydantic-settings, fastapi, uvicorn, procrastinate, `mcp>=2.3,<3`, httpx, genai-prices, PyYAML (skills frontmatter, T28), a cron parser (T05 decides; add `croniter` if procrastinate does not already pull it)
- [ ] Extras `[openai]`, `[anthropic]`, `[ollama]` (the langchain-* provider packages) and `[otel]` (opentelemetry-sdk, OTLP exporter, openinference-instrumentation-langchain); dev group: ruff, mypy, import-linter, pytest, pytest-asyncio, blockbuster
- [ ] `uv sync --all-extras` succeeds; `uv run python -c "import ranvier; print(ranvier.__version__)"` works; `uv run ranvier --help` exits 0
- [ ] `uv build --package ranvier` produces a wheel that contains `py.typed`; `uv.lock` committed
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Downstream tasks assume ALL dependencies already exist. If one is missing, add it (append-only) and mention it in the task report.
- `httpx2` (used by the mcp SDK v2) must resolve alongside `httpx`; check it comes in transitively through `mcp` and do not pin it yourself.
- Look at `agintis/pyproject.toml` and its `uv.lock` for version ranges that already worked together.
- Keep `cli.py` tiny: no subcommand logic, no langchain imports (import-linter C5 forbids them there).
- Conventions: follow the project's Python conventions and the decision log; never read `.env`; agintis is read-only reference.
