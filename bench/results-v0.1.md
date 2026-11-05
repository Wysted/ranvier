# v0.1 bench and exit check

Run on 2026-11-05 at commit `fffab21` (main plus T35 bench files).

Exit criterion used: tasks.md leaves "agintis flows run unchanged on graph-agent" undecided, so
this check runs the `examples/` flows (`hello`, `approval`, `research`). The agintis flows were
not run.

## Machine

| | |
|---|---|
| OS | Linux 6.18.54.1-microsoft-standard-WSL2 x86_64 (WSL2) |
| CPU | AMD Ryzen 9 5950X, 24 logical CPUs visible to WSL |
| RAM | 24 GB visible to WSL (the target is 8 GB; the bench stays far below it) |
| Python | 3.14.8 |
| Libraries | langgraph 1.2.19, langchain 1.4.9, psycopg 3.3.8, procrastinate 3.10.0, mcp 2.3.0 |
| Postgres | `pgvector/pgvector:pg18` from compose, 127.0.0.1:25432 |
| Settings | defaults: app pool 5, checkpointer pool 10, durability `sync`, one process, no HTTP layer |

## Bench (`bench/bench_runs.py`)

Real container, `execute_run` against Postgres, fake LLM that only sleeps. One run = `steps`
supersteps, each one fake model call plus one synchronous checkpoint. Single process, one event
loop. All runs ended `success`. Run times are row creation to terminal event.

| N | steps | step latency | ideal run | p50 | p95 | max loop lag (10 ms sampler) | container lag (1 s sampler) | RSS before / after | peak RSS |
|---|-------|--------------|-----------|-----|-----|------------------------------|-----------------------------|--------------------|----------|
| 10 | 5 | 0.5 s | 2.5 s | 2.611 s | 2.622 s | 1.4 ms | 1.0 ms | 136.8 / 138.2 MiB | 138.2 MiB |
| 50 | 5 | 0.5 s | 2.5 s | 2.773 s | 2.832 s | 3.0 ms | 2.4 ms | 137.0 / 141.3 MiB | 141.2 MiB |
| 100 | 5 | 0.5 s | 2.5 s | 2.935 s | 3.111 s | 4.3 ms | 0.8 ms | 136.9 / 144.9 MiB | 144.8 MiB |
| 200 | 5 | 0.5 s | 2.5 s | 3.319 s | 3.549 s | 5.5 ms | 1.0 ms | 136.9 / 151.9 MiB | 151.7 MiB |
| 200 | 20 | 0 s (overhead only) | 0 s | 8.151 s | 8.473 s | 3.8 ms | 4.2 ms | 136.8 / 149.1 MiB | 149.1 MiB |

Reading:
- Platform overhead per run at N=200 with a 2.5 s ideal: +0.8 s at p50 (about 0.16 s per step), and
  the loop never lagged more than 6 ms (warn threshold 100 ms, readiness 1000 ms).
- The last row removes the model: 4000 synchronous checkpointed supersteps in 8.5 s, about 470
  steps/s through one process and the 10-connection checkpointer pool. That is the platform
  ceiling for flows with no model latency; real flows are bounded by the LLM.
- Memory grows roughly 0.07 MiB per concurrent run over the 137 MiB baseline.
- Heartbeats were never exercised: the default `runs.heartbeat_s` is 15 s and runs lasted 3-8 s.
  Run admission (`limits.admit()`) is not called by the bench either.
- Not measured: CPU-pool (process isolation) flows, MCP, real providers, several processes.
- The pools were left at their defaults, so N=200 shares 5 + 10 connections. Larger N was not
  tried to keep the machine usable.

## Exit check

| Item | Result | Evidence |
|------|--------|----------|
| `scripts/check.sh` | PASS | ruff, ruff format, mypy strict, import-linter, 387 unit tests passed (36 s) |
| import-linter contracts | PASS | part of `check.sh` |
| `pytest -m integration` on compose Postgres | PASS | `cd packages/core && uv run pytest -m integration`: 103 passed (31 s) |
| `validate` on `examples/` | PASS | `ok: 3 flow(s), 0 tool(s)`, also with `--offline` and `--diagram` |
| `run --memory hello` | PASS | `end` status `success`, greeting in the `updates` event |
| `run --memory approval` | PASS | `interrupt` event then `end` status `interrupted` |
| `run --memory research` | NOT RUNNABLE | needs Ollama (`qwen3:4b`, not running here) and an MCP server (the manifest URL is a placeholder: `flow_build_failed`, `McpSourceError`). The flow is covered by `examples/tests/test_research.py` with fakes (inside `check.sh`) |
| `run` on Postgres (not asked, extra) | PASS | `migrate`, `run hello`, `run approval` (interrupt) and `run approval --resume true` ended `success` on a throwaway database |
| Clean wheel install | PASS | `uv build --package graph-agent --wheel`, installed in a fresh venv outside the repo: `graph-agent --help`, `import graph_agent, graph_agent.testing`, migrations SQL packaged, `migrate` applied `[1, 2]`; `langchain_postgres` and `deepagents` absent (extras do not leak). `scripts/ci-install-check.sh` also passes (members alone, git subdirectory install) |
| Starter renders | PASS | `uvx copier copy --vcs-ref HEAD` with `graph_agent_repo` pointing at a local clone with a throwaway tag `v0.1.0-exitcheck` (the real repo was not tagged) |
| Rendered starter `uv lock && uv sync` + `scripts/check.sh` | PASS | ruff, ruff format, mypy strict, import-linter (1 contract kept), 1 test passed |
| Full compose stack (db, migrate, api, worker) | PASS with caveats | built the starter image, `migrate` exited 0, api `/healthz` and `/readyz` 200, `/flows` 200 with the API key and 401 without, `POST /runs` was executed by the worker (ended `error` / `ConnectError` because Ollama is not running, as expected), `stop api worker` took 1 s and both exited 0 |

How the compose stack was brought up (so the result can be repeated):
- A dummy variables file outside the repo (not named `.env*`) replaced the starter's `env_file: .env`
  through a compose override with `!override`; it held `OLLAMA_HOST` and `API_KEY_ADMIN=<dummy>`.
- The override also removed the db host port (25432 belongs to the platform's compose), moved the
  api to 127.0.0.1:18000 and the project name was `t35exit`. Everything was removed afterwards
  (`down -v --rmi local`); the platform's compose was not touched.
- The container cannot reach a host path, and `uv` accepts only `https`, `ssh` and `file` git URLs.
  So the throwaway clone was copied into the build context, the pin in `pyproject.toml` and
  `uv.lock` was rewritten to `file:///gitsrv/graph-agent.git`, and one line `COPY gitsrv /gitsrv`
  was added to the rendered Dockerfile. Nothing else differs from the template output.

Exact override (`<dummy.vars>` is a file outside the repo, not named `.env*`; the project dir is the
rendered starter):

```yaml
# override.yml
x-dummy: &dummy
  env_file: !override
    - <dummy.vars>
services:
  db:
    ports: !override []
  migrate: *dummy
  api:
    <<: *dummy
    ports: !override
      - "127.0.0.1:18000:8000"
  worker: *dummy
```

```bash
docker compose -p t35exit -f docker-compose.yml -f override.yml up -d --build
docker compose -p t35exit -f docker-compose.yml -f override.yml down -v --rmi local
```

Exact pin rewrite (run in the rendered starter after `uv lock`; `$CLONE` is the throwaway clone
tagged `v0.1.0-exitcheck`, rendered with `graph_agent_repo=file://$CLONE`):

```bash
mkdir gitsrv && cp -a "$CLONE" gitsrv/graph-agent.git
sd -F "$CLONE" "/gitsrv/graph-agent.git" pyproject.toml uv.lock
sd '^COPY pyproject.toml uv.lock \./' 'COPY gitsrv /gitsrv
COPY pyproject.toml uv.lock ./' Dockerfile
```

## Failing items

None failed. Open points (2 is a pre-condition for the tag):

1. `research` was not exercised end to end (no Ollama, no real MCP server). Owner of the flow: T31.
2. PRE-CONDITION FOR THE v0.1 TAG (user action): `starter/.env.example` does not exist in the
   template (a permission rule blocks `.env*` files for agents; the user adds it). Without it `serve` in the compose stack stops at
   `api.keys.admin: env var 'API_KEY_ADMIN' is unset or empty` (correct, fail-loud behavior, but the
   README points to a missing file). Task T34.
3. The rendered starter cannot be built or locked without a remote git URL for graph-agent
   (https/ssh/file only). The git host is undecided, so a platform CI job that renders the starter
   needs a local-clone workaround like the one above. Follow-up for T34/T03 (CI), as noted in the
   T35 brief.
4. `pytest` run from `examples/` fails with "async def functions are not natively supported": the
   asyncio config lives in `packages/core/pyproject.toml`. `scripts/check.sh` passes `-c`, so the gate
   is green; the README of `examples/` (if any) should not suggest bare `pytest`. Task T31 (minor).
