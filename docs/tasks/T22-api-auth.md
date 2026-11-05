---
id: T22
title: API authentication: none, API keys, module hook
part: 7
wave: D
depends_on: [T04]
unblocks: [T23, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T22-api-auth
status: done
---

## Goal
`Principal` and the three authenticators (OD-2) exposed as a FastAPI dependency.

## Design references
- design.md OD-2 row in "Resolved decisions"
- design.md §2 layout row `api/auth.py`
- design.md §12 last paragraph (`api.auth = "module:callable"`)
- design.md §6 "HTTP API" intro (error body)

## Scope: files this task may create or edit
- packages/core/src/graph_agent/_internal/api/auth.py
- packages/core/tests/unit/api/test_auth.py
Shared files: none.

## Out of scope
- Mapping auth errors to the JSON error body (T23 registers the handler)
- JWT/JWKS verification (product side or a later `[jwt]` extra)
- Routes (T23)

## Acceptance criteria
- [ ] `Principal(subject, tenant_id)` frozen dataclass; `build_authenticator(settings.api)` returns an object with an async `authenticate(request) -> Principal`
- [ ] `none`: allowed only when the API binds to a loopback host, otherwise building it raises with a clear message
- [ ] `api_keys`: bearer token compared with `hmac.compare_digest` against keys read from the env var NAMES in settings; unknown/missing key -> `AuthError(code="unauthorized")`
- [ ] `module:callable` hook (sync or async) returning a `Principal`; import failure or wrong return type fails at build time
- [ ] `require_principal` FastAPI dependency; tests cover each mode and the error cases
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Hook import: use `importlib` directly (a few lines); if T05's `import_target` is already merged, reuse it.
- Reference only: `/home/wysted/code/agintis/src/agintis/platform/auth/verifier.py`, `/home/wysted/code/agintis/src/agintis/platform/http/dependencies.py`.
- Never log keys; never put key values in settings reprs.
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
