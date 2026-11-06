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
status: done
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
- [ ] `copier.yml` with `_subdirectory: starter`, questions for project name, package name and the ranvier tag (default `v0.1.0`)
- [ ] Rendered files per design §12: `pyproject.toml` (ranvier pinned by git tag + extras, ruff/mypy/import-linter forbidding `ranvier._internal`), `ranvier.toml`, example flow + fake-model test, `.env.example`, `docker-compose.yml`, `Dockerfile` (CPython 3.14.5+ slim, uv), `scripts/check.sh`, CI workflow, `README.md`
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
- From the T23 and T31 reviews (orchestrator): deployment docs must state a request body size limit at the gateway (uvicorn does not limit it) and the SSE idle-timeout caveat (no ping yet: proxies with idle timeout shorter than the longest silent node cancel inline runs); an API key without tenant_id is an admin key that sees all tenants. The starter's import-linter contract for user code needs `allow_indirect_imports = true` (the facade imports _internal); tests directories are not covered by import-linter, so add ruff banned-api for `ranvier._internal` in the starter (and examples/) tests. The starter manifest must not add profiles with `api_key_env` unless every flow needs it (bootstrap warms up all profiles).
- PENDING USER ACTION (orchestrator): `starter/.env.example` could not be created by any agent (permission deny rule on `.env*`). Content to add by hand: `RANVIER_DATABASE__URL=postgresql://ranvier:ranvier@127.0.0.1:25432/ranvier`, `API_KEY_ADMIN=change-me`, commented `# OPENAI_API_KEY=` and `# ANTHROPIC_API_KEY=`.
