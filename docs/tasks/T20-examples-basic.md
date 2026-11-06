---
id: T20
title: examples/: hello and approval flows with tests
part: 6
wave: I
depends_on: [T19]
unblocks: [T31, T34, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T20-examples-basic
status: done
---

## Goal
The `examples/` workspace member with a plain `hello` flow and an `approval` flow using `interrupt()`, each with a test written against the public testing kit only.

## Design references
- design.md §2 "Workspace" tree (`examples/`)
- design.md §4 "Manifest" and "What `target` may resolve to"
- design.md §5 "Interrupts"
- design.md §11 "Testing a flow" paragraph

## Scope: files this task may create or edit
- examples/pyproject.toml
- examples/ranvier.toml
- examples/src/examples/__init__.py
- examples/src/examples/flows/__init__.py
- examples/src/examples/flows/hello.py
- examples/src/examples/flows/approval.py
- examples/tests/test_hello.py
- examples/tests/test_approval.py
Shared files (append-only, keep edits minimal to avoid merge conflicts):
- pyproject.toml (root: append `examples` to workspace members/sources)
- packages/core/pyproject.toml (append `examples` to import-linter `root_packages` and the C2 forbidden contract)

## Out of scope
- Research/subagent/MCP flow (T31)
- Starter template (T34)
- Using `ranvier._internal` (forbidden by C2)

## Acceptance criteria
- [ ] `examples/pyproject.toml` depends on `ranvier {workspace = true}`; `uv sync` installs it
- [ ] `hello`: plain `StateGraph`, no model; `approval`: a node calls `interrupt()` and a later node uses the answer; no side-effect node shares a node with `interrupt()`
- [ ] `ranvier.toml` registers both with versions; `RANVIER_CONFIG=examples/ranvier.toml ranvier validate --offline` exits 0
- [ ] Tests use `ranvier.testing` only: hello result, approval pause (interrupt visible) and resume by id
- [ ] `ranvier run hello --memory --input ...` works from the examples dir
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- `scripts/check.sh` (T02) must pick the new member up; if it hardcodes paths, fix it minimally and say so.
- Keep flows tiny; they are documentation.
- Conventions: follow the project's Python conventions and the decision log; never read `.env`; agintis is read-only reference.
