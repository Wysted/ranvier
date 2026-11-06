---
id: T04
title: Root Settings (env, .env, ranvier.toml)
part: 2
wave: C
depends_on: [T02]
unblocks: [T05, T06, T09, T10, T22, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T04-settings
status: done
---

## Goal
One root pydantic `Settings` with nested groups and the precedence init > env > `.env` > `ranvier.toml` > defaults.

## Design references
- design.md §10 "Configuration and persistence" table (all rows)
- design.md §8 table (config key defaults)
- design.md §5 "Runs table" and "Serialization, recovery and housekeeping" (`runs.heartbeat_s`, `threads.ttl_days`)
- design.md §6 "HTTP API" admission paragraph and OD-2 row (api auth modes)

## Scope: files this task may create or edit
- packages/core/src/ranvier/_internal/config/settings.py
- packages/core/tests/unit/config/test_settings.py
Shared files: none.

## Out of scope
- `flows`, `tools`, `llm`, `mcp` models: T05 appends those fields to this class (leave one obvious place and keep the root model tolerant of unknown top-level toml keys until then)
- Reading real secrets or a real `.env`: tests pass `_env_file=None`
- Any consumer of these settings

## Acceptance criteria
- [ ] Groups with typed fields and the design defaults: `database` (url, app/checkpointer/procrastinate pool max = 5/10/5), `api` (host, port, `auth` = `none` | `api_keys` | `module:callable`, keys table by subject giving an env var NAME and optional tenant_id), `limits` (max_active_runs 32, admission_wait_s 5, sync_threads 16, cpu_workers 2, tool_timeout_s 30, shutdown_grace_s 60, loop_lag_warn_ms 100), `runs` (heartbeat_s, lease_s, max_attempts), `threads` (ttl_days, delete_after_days), `observability` (otel enabled, endpoint)
- [ ] Values the design leaves open (`runs.*`, `threads.*`, readiness lag threshold, api port) get named constants with a one-line justification; list them in the task report so design.md can be updated
- [ ] Env prefix `RANVIER_`, nested delimiter `__`; `.env` path from `RANVIER_ENV_FILE`; toml path from `RANVIER_CONFIG` (default `./ranvier.toml`; an explicitly set but missing path is an error, a missing default is not)
- [ ] Precedence proven by one test per layer pair; no global cache, no `isolated` hack; nested groups reject unknown keys with a message naming the key
- [ ] Secrets never appear in `repr(settings)` (use `SecretStr` or env-name indirection)
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Use pydantic-settings `settings_customise_sources` with `TomlConfigSettingsSource`; stdlib `tomllib`, no PyYAML (OD-6).
- Contrast: agintis has 13 `BaseSettings` classes each loading `.env` (`/home/wysted/code/agintis/src/agintis/bootstrap/settings.py`); do not port that shape, only the field ideas.
- Tests use `tmp_path` toml files and `monkeypatch.setenv`; never touch the repo `.env`.
- Conventions: follow the project's Python conventions and the decision log; never read `.env`; agintis is read-only reference.
