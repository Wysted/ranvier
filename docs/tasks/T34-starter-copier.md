---
id: T34
title: Copier starter template and copier update docs
part: 11
wave: J
depends_on: [T20, T23, T24]
unblocks: [T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T34-starter-copier
status: todo
---

## Goal
The Copier template product repos render, containing only what the product owns, with a README covering run/test/deploy/idempotency/`copier update`.

## Design references
- design.md §12 table (all rows) and the closing paragraph
- design.md §2 "Workspace" (`copier.yml` at repo root with `_subdirectory: starter`; one tag versions package and starter)
- design.md §5 "Idempotency (guidance shipped in the starter README)"
- design.md §8 row "Blocking-call detection" (blockbuster in starter CI)

## Scope: files this task may create or edit
- copier.yml
- starter/**
Shared files: none.

## Out of scope
- Changing the package (T01-T33)
- Publishing a tag
- Products' own CI hosting details beyond a generic workflow

## Acceptance criteria
- [ ] `copier.yml` with `_subdirectory: starter`, questions for project name, package name and the graph-agent tag (default `v0.1.0`)
- [ ] Rendered files per design §12: `pyproject.toml` (graph-agent pinned by git tag + extras, ruff/mypy/import-linter forbidding `graph_agent._internal`), `graph-agent.toml`, example flow + fake-model test, `.env.example`, `docker-compose.yml`, `Dockerfile` (CPython 3.14.5+ slim, uv), `scripts/check.sh`, CI workflow, `README.md`
- [ ] README covers run, test, deploy, the idempotency rules (derive keys from durable state, `durability="sync"` for side effects, `interrupt()` nodes separate) and `copier update`
- [ ] Verified by rendering into a temp dir (use the scratchpad, not the repo), overriding the dependency with the local path, running `uv sync` and the rendered `scripts/check.sh` green
- [ ] Rendered project imports only the public facade
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Use `uvx copier` if `copier` is not installed.
- `.env.example` holds names and placeholders only; never create or read a `.env`.
- Reuse `scripts/check.sh` and the CI structure from T02/T03 rather than inventing new ones.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
