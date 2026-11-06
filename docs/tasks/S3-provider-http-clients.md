---
id: S3
title: Provider integrations: shared http_async_client and agent name in callbacks
part: spike
wave: B
depends_on: [T01]
unblocks: [T08]
owner: sa-debug-py
reviewer: sa-review-py
branch: task/S3-provider-http-clients
status: done
---

## Goal
Verify that each provider integration accepts a shared `http_async_client` with limits, and that `create_agent(name=...)` shows up in callback metadata.

## Design references
- design.md §14 "Open checks" rows on `http_async_client` and `create_agent(name=...)`
- design.md §8 row "LLM provider concurrency"
- design.md §9 "Usage ledger" (`agent` key)

## Scope: files this task may create or edit
- docs/spikes/S3.md (findings)
- bench/spikes/S3/ (throwaway scripts only)
Shared files: none.

## Out of scope
- Editing `docs/design.md` (write proposed edits in the findings file; the orchestrator applies them)
- Editing anything under `packages/`, `pyproject.toml` or `uv.lock` (use `uv run --with <pkg>` or a script-local `uv run --script` header)
- Implementing the dependent tasks: T08, T30, T32
- Reading `.env`, calling paid APIs, or touching `~/code/agintis*` beyond reading

## Acceptance criteria
- [ ] For langchain-openai, langchain-anthropic, langchain-ollama and an OpenAI-compatible `base_url`: via `init_chat_model` kwargs, does the model use the SAME `httpx.AsyncClient` instance, and do its `Limits` apply (prove with a local stub HTTP server counting concurrent connections; no real keys, dummy key strings only)
- [ ] If an integration does not accept it: the smallest working alternative (delegating wrapper, or documented per-provider kwarg)
- [ ] `create_agent(name=...)`: which callback (`on_chat_model_start`/`on_llm_end`) and which metadata key carries the agent name, including a subagent invoked through a tool and a node-subgraph
- [ ] Whether `stream_usage` is needed for streamed token counts per provider
- [ ] `docs/spikes/S3.md` has: Question, Method, Result per check (PASS/FAIL/PARTIAL with exact package versions), Impact on design (list of concrete `design.md` edits as before/after text), Impact on dependent tasks (T08, T30, T32)
- [ ] Every claim is backed by a script output or a source file path; no claim from memory
- [ ] Scripts live only under `bench/spikes/S3/` and run with one documented command

## Notes
- Provider packages are extras; install them for the spike with `uv run --with`.
- Never use real API keys.
- Conventions: follow the project's Python conventions and the decision log; never read `.env`; agintis is read-only reference.
