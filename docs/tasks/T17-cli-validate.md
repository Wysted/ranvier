---
id: T17
title: CLI: validate with diagram and offline mode
part: 2
wave: G
depends_on: [T14]
unblocks: [T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T17-cli-validate
status: done
---

## Goal
`graph-agent validate [--diagram] [--offline]` that checks the whole manifest and names the failing key.

## Design references
- design.md §4 "Validation: `graph-agent validate [--diagram] [--offline]`"
- design.md §6 "CLI" table row `validate`
- design.md §4 names/versions/crons rules

## Scope: files this task may create or edit
- packages/core/src/graph_agent/_internal/runtime/offline.py (new, small: scripted `FakeChatModel` with `calls` and a no-op `bind_tools`; T19 re-exports it as the public one)
- packages/core/tests/unit/runtime/test_offline.py
- packages/core/tests/unit/test_cli_validate.py
Shared files (append-only, keep edits minimal to avoid merge conflicts):
- packages/core/src/graph_agent/_internal/cli.py (append `_add_validate` + its handler, nothing else; T32 appends `usage` in the same wave)
- packages/core/src/graph_agent/_internal/runtime/flows.py (T14's file, earlier wave: append `validate_manifest(settings, *, offline) -> list[Problem]`)

## Out of scope
- `run`, `migrate` (T18)
- A public testing kit (T19)
- Connecting to real MCP servers (never during validate)

## Acceptance criteria
- [ ] Imports every flow and tool target, checks object kinds, builds factories (offline: fake models via `offline.py`, no MCP), compiles against `InMemorySaver`, checks names, versions, cron expressions, referenced profiles and MCP servers
- [ ] Collects ALL problems, prints each with the manifest key (e.g. `flows.summarize.target: ...`), exit code non-zero if any, zero otherwise
- [ ] `--diagram` prints Mermaid per flow (`get_graph().draw_mermaid()`)
- [ ] Tests: a tmp manifest + tmp module on `sys.path` for a good case and several bad ones (missing attr, compiled graph, bad cron, unknown profile)
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Fake model lives in `runtime/offline.py` (not in `testing.py`) because `_internal` must not import `testing` (contract C2) and `cli` must not import langchain directly (C5). This file is a small addition to the design layout; report it.
- Offline `FlowContext`: `model(profile)` validates the profile exists then returns the fake; `tools(*names)` validates names against the manifest; `agent(spec)` validates referenced profile/MCP names and returns a trivial compiled graph.
- tasks.md lists only T14 as a dependency; the tool registry (T27) may be reused if present but is not required.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
