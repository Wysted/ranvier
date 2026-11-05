---
id: T08
title: LLM profiles to chat models with shared HTTP clients
part: 3
wave: E
depends_on: [T05, S3]
unblocks: [T15, T30, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T08-llm-profiles
status: done
---

## Goal
Build chat models from profiles with `init_chat_model`, sharing one `httpx.AsyncClient` per provider with connection limits.

## Design references
- design.md §9 "Profiles" block and "Kept rules"
- design.md §8 rows "LLM provider concurrency" and "LLM call timeout and retries"
- design.md §3 `FlowContext.model(profile)`
- design.md §14 open check "Each provider integration accepts a shared `http_async_client`"

## Scope: files this task may create or edit
- packages/core/src/graph_agent/_internal/llm.py
- packages/core/tests/unit/test_llm.py
Shared files: none.

## Out of scope
- `FlowContext` implementation (T12)
- Container wiring and closing clients (T15)
- Usage accounting (T32)

## Acceptance criteria
- [ ] `LlmRegistry` (or equivalent) built from the profiles/providers settings with `model(profile) -> BaseChatModel` and `aclose()`; unknown profile raises an error naming the key
- [ ] One shared `httpx.AsyncClient` per provider with `Limits(max_connections=provider.max_connections)`, passed per the S3 findings (if an integration rejects it, implement what S3 recommends and note it)
- [ ] Profile `timeout_s` / `max_retries` override SDK defaults; `api_key_env` is resolved at build time, a missing variable fails loudly naming the variable (never the value)
- [ ] `stream_usage=True` for OpenAI-compatible providers; provider package missing -> error telling which extra to install
- [ ] Unit tests use fake env vars and no network: client sharing, limits, error cases
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Read `docs/spikes/S3.md` first; it decides how the shared client reaches each integration.
- Port ideas from `/home/wysted/code/agintis/src/agintis/platform/llm/profiles.py`, `models.py`, `errors.py`, `/home/wysted/code/agintis/src/agintis/bootstrap/llm.py`.
- Provider imports must be lazy (extras are optional). langchain is allowed here (contract C5).
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
- From S3 (orchestrator): declare `httpx2>=2.16,<3` as a direct dependency in packages/core/pyproject.toml (llm.py imports it) and cap `langchain-anthropic` to its current minor; follow docs/spikes/S3.md "Impact on dependent tasks" for the per-provider client wiring, the anthropic `shortcut:` and its identity test.
