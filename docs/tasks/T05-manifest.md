---
id: T05
title: Manifest models and lazy target import
part: 2
wave: D
depends_on: [T04]
unblocks: [T08, T12, T14, T27, T29, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T05-manifest
status: todo
---

## Goal
Pydantic models for flows, crons, tools, MCP servers and LLM profiles/providers, validated names/versions/crons, and the lazy `module:object` import helper; attach them to the root `Settings`.

## Design references
- design.md §4 "Manifest" toml example and "What `target` may resolve to"
- design.md §4 "Validation" (names regex, versions, cron expressions)
- design.md §7 toml block `[mcp.servers.github]`
- design.md §9 "Profiles" toml block and "Kept rules"
- design.md §8 table rows LLM provider concurrency / MCP server / Tool

## Scope: files this task may create or edit
- packages/core/src/graph_agent/_internal/config/manifest.py
- packages/core/tests/unit/config/test_manifest.py
- packages/core/src/graph_agent/_internal/config/settings.py (add the `flows`, `tools`, `llm`, `mcp` fields and switch the root model to reject unknown top-level keys)
Shared files: none.

## Out of scope
- Importing and building flows, kind checks, `CompiledStateGraph` rejection (T14)
- The `validate` command (T17)
- Constructing models or HTTP clients from profiles (T08)
- Procrastinate registration of crons (T25)

## Acceptance criteria
- [ ] Models: flow (target, version>=1, description, durability `sync|async|exit` default `sync`, timeout_s 600, max_concurrency, config mapping, crons list), cron (schedule 5-field UTC, input), tool (target, timeout_s, max_concurrency, isolation `async|thread|process`), mcp server (url, headers_env, max_concurrency, timeout_s), llm profile and provider settings (`max_connections` 16, profile timeout_s 60, max_retries 2)
- [ ] Names match `^[a-z][a-z0-9_]{0,62}$` (flows, tools, profiles, mcp servers); `target` matches `module:object`; errors name the manifest key (e.g. `flows.summarize.target`)
- [ ] Profile rules: keyed providers must name `api_key_env`; `base_url` must be https except localhost; headers_env values are env var NAMES
- [ ] Cron expressions validated at load time; the helper that computes the next fire time is exposed for `crons` (T25)
- [ ] `import_target(target) -> object` imports lazily and raises an error naming module and attribute; never imports at model-validation time
- [ ] Unit tests for each rule that rejects input
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Cron lib: check what procrastinate depends on (`croniter`?) and use the same one so schedules mean the same thing in both places; importing `croniter` here does not break contract C4 (it only restricts `procrastinate`).
- Profile shape ported from `/home/wysted/code/agintis/src/agintis/platform/llm/profiles.py` (YAML there, TOML here).
- Same syntax as `langgraph.json` for targets; no `.py` file paths.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
