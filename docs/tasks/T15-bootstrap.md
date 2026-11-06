---
id: T15
title: Composition root: Container and AsyncExitStack
part: 3
wave: F
depends_on: [T08, T09, T10, T11, T27, T29]
unblocks: [T16, T32, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T15-bootstrap
status: done
---

## Goal
One `open_container(settings, *, memory=False)` async context manager that builds and tears down pools, LLM registry, limits, tools, MCP sources, tracing and checkpointer.

## Design references
- design.md §2 layout row `bootstrap.py` and contract C1
- design.md §8 table (everything is created once in the composition root)
- design.md §13 row "Composition root"
- design.md §7 table rows "Tool registry" and "MCP port"

## Scope: files this task may create or edit
- packages/core/src/ranvier/_internal/bootstrap.py
- packages/core/tests/unit/test_bootstrap.py
- packages/core/tests/integration/test_bootstrap.py
Shared files: none.

## Out of scope
- `FlowRegistry` and runner wiring (T16 appends)
- Queue wiring (T24 appends)
- Usage observer wiring (T32 appends)
- Metrics publishing from the runner (T33)

## Acceptance criteria
- [ ] `Container` dataclass with: settings, app/checkpointer pools (None in memory mode), checkpointer, runs repository (None in memory mode), LLM registry, limits (executor installed, lag sampler started), metrics/tracing handle, tool registry (T27), MCP source (T29), `observers: list[RunObserver]` (empty; T32 appends)
- [ ] Everything is entered into one `AsyncExitStack`; closing order is reverse of opening; exception during open closes what was opened
- [ ] `memory=True` opens no pool and uses `InMemorySaver`
- [ ] Logs the connection budget at startup; foreign-tracing guard runs at boot
- [ ] Unit test with `memory=True`; integration test against compose Postgres checking clean teardown
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Additional dependencies not listed in tasks.md: T27 (tool registry) and T29 (MCP source) are in an earlier wave and must be wired here, because T30 (same wave) must not edit this file. Add them to your reading list.
- Port from `/home/wysted/code/agintis/src/agintis/bootstrap/container.py` (drop RAG, ingest, tenancy wiring).
- Constructors take collaborators explicitly; no module-level globals.
- Metrics gauge sources (loop lag, executor occupancy, pool stats, semaphore waiters) are registered with the metrics handle here as callables; T33 only adds runner-level metrics.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
- From the T10 review (orchestrator): `Observability.shutdown()` blocks (up to ~2x10 s with the collector down): call it via `asyncio.to_thread` and count it inside `limits.shutdown_grace_s`.
- From the T09 review (orchestrator): enter `CpuPool` via its async `start()` (warm-up in a thread) in the AsyncExitStack BEFORE run admission opens; entrypoints using it need the `if __name__ == "__main__"` guard (forkserver re-imports __main__).
- From T08 (orchestrator decision): `LlmRegistry.model()` does sync I/O on first use per provider (httpx2 client: package metadata, SSL context) that blockbuster rejects in a coroutine. The composition root pre-creates every configured profile model in a thread (`await asyncio.to_thread(...)`) at startup, so `model()` is a pure cache read afterwards; register `registry.aclose()` in the AsyncExitStack.
- From T11 (orchestrator): `create_checkpointer` must be called inside the running loop (AsyncPostgresSaver captures get_running_loop in __init__): build it in the container's async start, not in a sync build step.
