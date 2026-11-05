# graph-agent design (v0.1)

graph-agent is an installable Python package that runs plain LangGraph 1.x `StateGraph` flows in production: it registers them from a manifest, executes them durably on Postgres, exposes them over CLI, HTTP/SSE and a queue worker, and ships a small kit to build subagents. Product repos install it by git tag and own only their flows, tools and config.

Source of truth for the "why": `.claude/agents/_shared/decisions.md` (decision log), `reports/Plataforma de flujos sobre LangGraph.md`, `reports/Rendimiento de Python en la plataforma.md`. Items marked **(OD-n)** follow the recommendation of an open decision at the end of this document.

## Decisions at a glance

| Area | Decision | Justification |
|------|----------|---------------|
| Flow authoring | Plain `StateGraph`; no DSL, no wrapper base class | Decision log: users write plain LangGraph |
| Discovery | Explicit manifest `graph-agent.toml`, `name -> "module:object"` | Decision log; same syntax as `langgraph.json` |
| Execution | One runner function; Postgres checkpointer; runs table with leases | Checkpoint is not durable execution (platform report) |
| Queue and cron | Procrastinate on Postgres, `lock=thread_id`, periodic stalled-job requeue | Decision log (verified) |
| HTTP | Own FastAPI API borrowing `threads`/`runs`/`thread_id`/`run_id`, no compatibility claim | Decision log (option A) |
| Agents | Factory over `create_agent` from an `AgentSpec` | Platform report: middleware exists, per-subagent registry does not |
| MCP | Own adapter over the official `mcp` SDK v2 (`mcp>=2.3,<3`, `mcp.Client`) behind the `McpToolSource` port; Streamable HTTP only | Decision log (supersedes `langchain.mcp.MCPAdapter` beta and the archived `langchain-mcp-adapters`) |
| Concurrency | Global limits owned by the platform (LangGraph has none) | Performance report, "features the package must provide" |
| Python | `requires-python >=3.13`; images on CPython 3.14.5+ **(OD-7)** | Performance report: 3.14.0-3.14.4 GC regression |
| Name | Distribution `graph-agent`, import `graph_agent` (working name, renameable: only the facade and the manifest default path carry it) | Brief |

## 1. Purpose and non-goals

**Purpose:** run general AI workflows (agents, subagents, pipelines, approvals, scheduled jobs) on LangGraph with durable state, resumable interrupts, governed concurrency, cost accounting and traces, on a self-hosted box (~8 GB RAM, no GPU, Postgres).

**Non-goals (all versions unless a concrete case appears):**

- Not a chatbot product and not a UI.
- Not a LangGraph wrapper: no node base classes, no graph builder DSL.
- Not compatible with Agent Server / Agent Protocol / `langgraph-sdk` (Elastic-2.0 and Enterprise key; parity fails on interrupts and crons).
- No Redis, no external orchestrator (Temporal, DBOS, Hatchet): the "execute a run" function is the narrow seam if one is ever needed.
- No multi-provider abstraction of its own: `init_chat_model` already is one.

## 2. Repository and package layout

### Workspace

```text
graph-agent/
  pyproject.toml          # uv workspace root: dev tools only, never published
  uv.lock
  copier.yml              # _subdirectory: starter (Copier needs it at repo root)
  packages/core/          # distribution "graph-agent"; installed by products
    pyproject.toml        # deps, extras, ruff/mypy/import-linter config
    src/graph_agent/
    tests/{unit,integration}/
  examples/               # workspace member; depends on graph-agent {workspace = true}
    graph-agent.toml
    src/examples/flows/   # hello (plain), approval (interrupt), research (subagent + MCP)
    tests/
  starter/                # Copier template rendered into product repos
  bench/                  # N concurrent runs with a fake LLM, loop-lag report
  scripts/check.sh        # ruff, format, mypy, lint-imports, pytest (ported)
  docker-compose.yml      # Postgres for dev and integration tests
  docs/design.md
```

One git tag (`v0.1.0`) versions both the package and the starter, so `copier update` and the dependency pin move together. Products depend on `graph-agent @ git+https://<host>/graph-agent@v0.1.0#subdirectory=packages/core` (to verify, see open checks).

### Core package

Size target: under ~5k loc for v0.1 (agintis core is ~32k). Single-file concerns are modules, not packages.

```text
graph_agent/
  __init__.py             # public facade: re-exports only (section 3)
  testing.py              # public test kit (section 11)
  py.typed
  _internal/
    config/
      settings.py         # one root Settings: env + .env + graph-agent.toml
      manifest.py         # flows/tools/crons/mcp models, lazy "module:object" import
    db/
      pool.py             # psycopg pools (app, checkpointer) and budget logging
      migrate.py          # applies migrations/*.sql under an advisory lock (OD-5)
      migrations/         # 0001_init.sql: runs, usage
    llm.py                # profiles -> init_chat_model, one httpx client per provider
    limits.py             # semaphores, admission, bounded thread/process executors, loop lag
    usage.py              # ledger callback, repository, genai-prices at read time
    observability/
      tracing.py          # port, foreign-tracing guard, span attribute names
      otlp.py             # lazy OTLP + OpenInference adapter ([otel] extra)
    queue.py              # Procrastinate app factory, defer by task name (no runtime import)
    agents/
      spec.py             # AgentSpec
      factory.py          # build_agent, as_tool
      tools.py            # tool registry, limit wrappers, sync/process isolation
      skills.py           # SKILL.md loader + load_skill middleware
      mcp.py              # McpToolSource port + own adapter over the mcp SDK v2
    runtime/
      context.py          # RunContext, FlowContext implementation
      flows.py            # flow resolution, lazy compile cache, version checks
      runner.py           # execute_run: claim, astream v2 -> events, finish
      runs.py             # runs repository: state machine, claims, leases, cancel flag
      events.py           # transport-agnostic run events
      checkpointer.py     # AsyncPostgresSaver / InMemorySaver construction
    bootstrap.py          # composition root: Container + AsyncExitStack
    worker.py             # Procrastinate tasks: execute, sweep, prune, crons
    api/
      app.py              # FastAPI factory + lifespan
      routes.py           # endpoints (section 6)
      dto.py              # wire DTOs and SSE mapping
      auth.py             # Principal + authenticators (OD-2)
    cli.py                # argparse subcommands
```

### Import contracts (import-linter, v0.1)

| # | Contract | Rule |
|---|----------|------|
| C1 | Layers | `cli \| api \| worker` > `bootstrap` > `runtime` > `agents` > `llm \| limits \| usage \| observability \| queue` > `db \| config` (siblings independent) |
| C2 | Facade direction | `graph_agent._internal` never imports `graph_agent.testing`; examples and starter forbid `graph_agent._internal` |
| C3 | Web stays at the edge | `fastapi`, `starlette` only in `api`; `uvicorn` only in `api` and `cli` |
| C4 | Queue stays at the edge | `procrastinate` only in `queue` and `worker` |
| C5 | LangGraph surface | `langgraph` only in `runtime`, `agents`, facade, `testing`; `langchain`/`langchain_core` additionally in `llm`, `usage` |
| C6 | Tracing behind its port | `opentelemetry`, `openinference` only in `observability.otlp`; only `observability.tracing` imports `otlp` (lazily) |

Ruff `banned-api` (import-linter cannot target external subpackages): `mcp` and `httpx2` outside `agents/mcp.py` (tests excepted); `langchain.mcp`, `fastmcp`, `langchain_mcp_adapters` (archived), `langgraph.prebuilt.create_react_agent`, `langgraph_supervisor`, `asyncpg`, `fastapi.responses.ORJSONResponse` (deprecated) everywhere.

## 3. Public API

Everything users import lives in `graph_agent` and `graph_agent.testing`. Anything under `_internal` may change in any release.

```python
# graph_agent/__init__.py
__all__ = ["AgentSpec", "FlowContext", "RunContext", "as_tool", "__version__"]

@dataclass(frozen=True, slots=True)
class RunContext:                  # LangGraph Runtime.context of every run; data only, not persisted
    run_id: str
    thread_id: str
    flow: str
    flow_version: int
    attempt: int                   # 1 + number of orphan recoveries of this run
    tenant_id: str | None

class FlowContext(Protocol):       # handed to flow factories at build time
    flow: str
    config: Mapping[str, Any]      # [flows.<name>.config] from the manifest
    def model(self, profile: str) -> BaseChatModel: ...
    def tools(self, *names: str) -> list[BaseTool]: ...
    async def agent(self, spec: AgentSpec) -> CompiledStateGraph: ...

@dataclass(frozen=True, slots=True)
class AgentSpec:
    name: str                      # also the usage-ledger attribution key
    profile: str                   # LLM profile name
    system_prompt: str | None = None
    tools: Sequence[str | BaseTool] = ()      # registry names or tool objects
    mcp_servers: Sequence[str] = ()           # names from [mcp.servers]
    skills: Sequence[Path] = ()               # directories holding SKILL.md folders
    middleware: Sequence[AgentMiddleware] = ()
    response_format: type | None = None

def as_tool(agent: CompiledStateGraph, *, name: str, description: str) -> BaseTool: ...
```

Event emission, interrupts, `Command`, `Send` and subgraphs are plain LangGraph (`get_stream_writer()`, `interrupt()`); the platform adds no helper for them.

## 4. Flow contract and manifest

### Manifest (`graph-agent.toml`, product repo root; path override `GRAPH_AGENT_CONFIG`)

```toml
[flows.summarize]
target = "my_product.flows.summarize:build"   # builder or factory
version = 2
description = "Summarize a document set"
durability = "sync"        # sync | async | exit (OD-4)
timeout_s = 900            # whole run
max_concurrency = 8        # LangGraph per-run max_concurrency
config = { language = "es" }

[[flows.summarize.crons]]
schedule = "0 3 * * *"     # UTC only; each firing starts a new thread
input = { source = "daily" }

[tools.web_search]
target = "my_product.tools:web_search"
timeout_s = 30
max_concurrency = 4
isolation = "async"        # async | thread | process
```

Profiles, MCP servers and limits live in the same file (sections 7-10).

### What `target` may resolve to

| Object | Accepted | Notes |
|--------|----------|-------|
| `StateGraph` builder (uncompiled) | Yes | Platform compiles it with its checkpointer |
| `Callable[[FlowContext], StateGraph \| Awaitable[StateGraph]]` | Yes | For flows needing models, tools or subagents |
| `CompiledStateGraph` | No, loud error | The platform owns the checkpointer and compile options |

Flows are compiled lazily on first use per process and cached; a build failure fails that run with `flow_build_failed` (retryable) instead of blocking the process. Flows declare `context_schema=RunContext` to get a typed `runtime.context`.

### Validation: `graph-agent validate [--diagram] [--offline]`

Imports every flow and tool target, checks the object kind, builds factories (with fake models and no MCP when `--offline`), compiles against `InMemorySaver`, checks names (`^[a-z][a-z0-9_]{0,62}$`), versions (positive int), cron expressions, referenced profiles and MCP servers. `--diagram` prints Mermaid per flow. Exit code non-zero on any error; errors name the manifest key.

### Persisted contracts

Flow names, versions, node names and state schemas are data stored in runs and checkpoints.

| Change | Rule |
|--------|------|
| Rename a flow | New flow; old threads cannot continue |
| Incompatible state schema or node rename | Bump `version`; runs on a thread recorded with another version are rejected (`409 flow_version_mismatch`) |
| Compatible addition (new optional key, new node) | No bump |
| Migrating old threads | Product responsibility in v0.1 (finish or abandon them before deploy) |

## 5. Runtime

### Single runner

`execute_run(run_id)` is the only way a flow runs (CLI, HTTP inline, worker, cron, recovery):

1. **Claim**: conditional `UPDATE runs SET status='running', lease_owner, lease_expires_at, attempt=attempt+1` where status is `pending`, or `running` with an expired lease. No row updated: another executor owns it, return.
2. **Resolve** the flow (lazy compile cache) and check its version against the thread.
3. **Invoke** `graph.astream(input, config, context=RunContext(...), stream_mode=[...], subgraphs=True, version="v2", durability=flow.durability)` with `configurable.thread_id`, `metadata.run_id`, usage and tracing callbacks, inside `asyncio.timeout(flow.timeout_s)`.
4. **Heartbeat** every `runs.heartbeat_s`: extends the lease and reads `cancel_requested`.
5. **Finish**: write the terminal status, error and usage rows.

Input on step 3: original input or `Command(resume=...)` on the first attempt; on a recovery attempt `None` if the latest checkpoint already carries this `run_id`, otherwise the original input again (open check: checkpoint metadata propagation).

### Runs table (`graph_agent.runs`)

| Column | Notes |
|--------|-------|
| `run_id` uuid PK, `thread_id` uuid | Thread identity = checkpointer `thread_id` (OD-3: no threads table) |
| `flow`, `flow_version`, `tenant_id` | Pinned by the first run of a thread |
| `status` | `pending \| running \| success \| error \| interrupted \| cancelled` |
| `input` jsonb, `command` jsonb | Exactly one set |
| `error` jsonb | `{code, message, retryable}` |
| `attempt`, `lease_owner`, `lease_expires_at`, `cancel_requested` | Recovery and cancellation |
| `created_at`, `started_at`, `finished_at`, `metadata` jsonb | |

Invariant: partial unique index on `(thread_id) WHERE status = 'running'`: at most one running run per thread, whatever the entry path.

### Transitions

| From | To | Trigger |
|------|----|---------|
| (new) | `pending` | API, CLI or cron creates the run |
| `pending` | `running` | Claim succeeds |
| `pending` | `cancelled` | Cancel before claim (the queued job no-ops) |
| `running` | `success` / `error` | Graph finishes / raises or times out (`run_timeout`) |
| `running` | `interrupted` | Stream reports interrupts; resuming is a new run on the thread |
| `running` | `cancelled` | Heartbeat sees `cancel_requested`, or client disconnects from an inline run |
| `running` | `pending` | Lease expired (orphan sweep) or graceful shutdown; `attempt > runs.max_attempts` goes to `error` (`orphan_retries_exhausted`) |

### Serialization, recovery and housekeeping

| Mechanism | How |
|-----------|-----|
| Per-thread order (queued runs) | Procrastinate job `execute_run` deferred with `lock=thread_id` |
| Per-thread exclusion (all paths) | Partial unique index; inline run on a busy thread gets `409 thread_busy`; a queued job hitting it retries with backoff |
| Stalled jobs | Periodic task (every minute): `job_manager.get_stalled_jobs()` then `retry_job()` (worker heartbeats) |
| Orphan runs | Same periodic task: `running` runs with expired lease go back to `pending` and are deferred (covers API crashes of inline runs) |
| Checkpoint growth | Periodic prune: `keep_latest` for threads idle more than `threads.ttl_days`, `delete_thread` past a second TTL |

Claims make both recovery paths idempotent: the job that loses the claim exits.

### Interrupts, cancellation, idempotency

- **Interrupts**: the run ends `interrupted`; the checkpoint is the source of truth (`GET /threads/{id}/state`). Resume sends `command.resume` as a map `{interrupt_id: value}` (a bare value is accepted only with exactly one pending interrupt). New `input` on a thread with pending interrupts is rejected (`409 thread_interrupted`), because LangGraph would silently discard them.
- **Cancellation**: pending runs are cancelled in the row; running runs are cancelled at the next heartbeat (latency at most `heartbeat_s`). Cancellation propagates through task cancellation to tools, subagents and MCP calls; thread-bound sync tools cannot be stopped, which is why they have their own bounded pool.
- **Idempotency** (guidance shipped in the starter README): side-effect nodes run at least once more after a crash or a resume. Derive idempotency keys from durable state written by an earlier node (not from `run_id`: a resume is a new run), use `durability="sync"` for flows with side effects, and keep `interrupt()` nodes separate from side-effect nodes.

## 6. Entry points

### CLI (`graph-agent`, argparse)

| Command | Does |
|---------|------|
| `validate [--diagram] [--offline]` | Section 4 |
| `run <flow> --input JSON [--thread ID] [--resume JSON] [--memory]` | Inline run printing events; `--memory` uses `InMemorySaver` and no database |
| `serve` | uvicorn with the API |
| `worker` | Procrastinate worker: execute, sweep, prune, crons |
| `migrate` | Own SQL migrations, `checkpointer.setup()`, Procrastinate schema |
| `crons` | Lists declared crons with next fire time (UTC) |
| `usage [--since] [--by flow\|model\|agent]` | Tokens and indicative cost from the ledger |

### HTTP API

Inline streamed runs execute in the API process; background runs and crons go through the queue (OD-1, resolved). All errors use one body `{code, message, retryable}`; status codes from `http.HTTPStatus`.

| Method and path | Purpose | Success |
|-----------------|---------|---------|
| `GET /healthz`, `GET /readyz` | Liveness; readiness (pools, loop lag under threshold) | 200 |
| `GET /flows` | Name, version, description | 200 |
| `POST /runs` | Background run on a new thread | 202 `{run_id, thread_id}` |
| `POST /runs/stream` | Inline run on a new thread, SSE | 200 |
| `POST /threads/{thread_id}/runs` | Background run (input or resume) | 202 |
| `POST /threads/{thread_id}/runs/stream` | Inline run (input or resume), SSE | 200 |
| `GET /threads/{thread_id}/runs` | Runs of a thread, newest first | 200 |
| `GET /threads/{thread_id}/runs/{run_id}` | One run | 200 |
| `POST /threads/{thread_id}/runs/{run_id}/cancel` | Request cancellation | 202 |
| `GET /threads/{thread_id}/state` | `values`, `next`, `interrupts`, `checkpoint_id` | 200 |
| `DELETE /threads/{thread_id}` | Delete checkpoints and runs (refused while running) | 204 |

Run request body: `{flow, input | command: {resume}, stream_mode?: ["updates","values","messages","custom"], metadata?}`. Default `stream_mode` is `["updates", "custom"]`; token streaming is opt-in by asking for `messages` **(OD-8)**. Admission: when `limits.max_active_runs` is full, inline runs wait up to `limits.admission_wait_s` and then get `429 too_many_runs`.

### SSE events

Transport: FastAPI `EventSourceResponse` (as in agintis). Event id `{run_id}:{seq}`, with `seq` monotonic per run from the first release, so a later `run_events` table (OD-1 option c) can add `Last-Event-ID` replay without changing the wire format; replay is not supported in v0.1.

| `event` | `data` DTO | When |
|---------|------------|------|
| `metadata` | `{run_id, thread_id, flow, flow_version, attempt}` | First event |
| `updates` | `{ns, node, update}` | `updates` mode |
| `values` | `{ns, values}` | `values` mode |
| `messages` | `{ns, node, id, type, content, tool_call_chunks}` | `messages` mode, LLM chunks only |
| `custom` | `{ns, node, data}` | Payloads from `get_stream_writer()` |
| `interrupt` | `{interrupts: [{id, value}]}` | Run paused |
| `end` | `{status, usage: {input_tokens, output_tokens, cached_input_tokens}}` | Terminal success, interrupt or cancel |
| `error` | `{code, message, retryable}` | Terminal error |

State is JSON-serializable by convention; a value the encoder cannot serialize produces an `error` event, never a silent drop.

## 7. Agents kit

`await ctx.agent(spec)` builds `create_agent(model=ctx.model(spec.profile), tools=registry(spec.tools) + mcp(spec.mcp_servers), system_prompt=..., middleware=[SkillsMiddleware(spec.skills), *spec.middleware], response_format=..., name=spec.name)`. No checkpointer is passed: as a node it uses the parent's, as a tool it is stateless per call.

| Piece | v0.1 behavior |
|-------|---------------|
| Tool registry | `[tools.*]` entries imported lazily, wrapped once with timeout, semaphore and isolation; tool objects passed directly get the default limits |
| MCP port | `McpToolSource.tools(server) -> Sequence[BaseTool]`. Own adapter: one `mcp.Client` per server with its own `httpx2.AsyncClient` (headers, timeouts), entered in the composition root; lists tools once (paginated); each tool becomes `StructuredTool(name, description, args_schema=inputSchema, coroutine=..., response_format="content_and_artifact")`; wraps each call with the server semaphore and timeout; reconnects on session loss |
| MCP results | `CallToolResult` text blocks become content; image and resource blocks become content blocks plus artifact; `structuredContent` goes to the artifact; `isError` becomes an error `ToolMessage`; `InputRequiredResult` raises a clear error in v0.1 (future: map to `interrupt()`) |
| MCP transport | Streamable HTTP only; headers from env var names (`headers_env`), never inline secrets |
| Skills | Reads `SKILL.md` per agentskills.io (required `name`, `description`), injects name and description into the system prompt, exposes `load_skill(name)` returning the body; no script execution |
| Middleware | Any LangChain `AgentMiddleware`; official ones (retry, fallback, limits, HITL, summarization) are used as-is |

```toml
[mcp.servers.github]
url = "https://mcp.internal/github"
headers_env = { Authorization = "GITHUB_MCP_TOKEN" }
max_concurrency = 4
timeout_s = 30
```

### Using a subagent in a flow

| Pattern | How | Trade-off |
|---------|-----|-----------|
| As a node | `builder.add_node("research", agent)` when the parent state has `messages`; otherwise a wrapper node mapping state in and out | Visible to `get_state(subgraphs=True)`, interrupts work |
| As a tool | `as_tool(agent, name=..., description=...)` handed to another agent | Isolated context; not inspectable per subagent |

### Extras as extension points (not implemented in v0.1)

| Extra | Reserved surface | Trigger to build |
|-------|------------------|------------------|
| `[rag]` | `graph_agent.rag.make_retriever_tool(store, *, name, description, k)` over `PGVectorStore` | First product flow needing retrieval |
| `[deepagents]` | `AgentSpec`-to-`create_deep_agent` builder | A case needing file-working agents; verify it works as a node first |

## 8. Concurrency and resilience

LangGraph's `max_concurrency` is per invocation; R runs times N tool calls is unbounded. The platform owns every global limit, all created once in the composition root.

| Concern | Mechanism | Config key (default) |
|---------|-----------|----------------------|
| Run admission | Process semaphore shared by inline runs; Procrastinate worker `concurrency` set to the same value | `limits.max_active_runs` (32), `limits.admission_wait_s` (5) |
| LLM provider concurrency | One shared `httpx.AsyncClient` per provider with connection limits passed to the integration (open check per integration) | `llm.providers.<p>.max_connections` (16) |
| LLM call timeout and retries | Profile `timeout_s`, `max_retries` override SDK defaults (600 s, 2) | per profile (60, 2) |
| MCP server | Semaphore + `asyncio.timeout` per server in the MCP port | `mcp.servers.<s>.max_concurrency`, `timeout_s` |
| Tool | Semaphore + `asyncio.timeout` per tool in the registry wrapper | `tools.<t>.*`, `limits.tool_timeout_s` (30) |
| Sync tools and sync nodes | Bounded `ThreadPoolExecutor` installed with `loop.set_default_executor` | `limits.sync_threads` (16) |
| CPU-heavy tools | `isolation = "process"`: `ProcessPoolExecutor` (forkserver), timeout recycles the pool (`kill_workers`, 3.14) | `limits.cpu_workers` (2) |
| Run timeout | `asyncio.timeout` around the stream; error `run_timeout` | `flows.<f>.timeout_s` (600) |
| Node timeout | Native LangGraph 1.2 `add_node(..., timeout=TimeoutPolicy(...))` (async nodes) and `error_handler=` for compensation; the platform does not wrap nodes, the starter documents the pattern | set in the flow |
| Graceful shutdown | On SIGTERM, stop admission, call `RunControl.request_drain()` on running runs (checkpoint at the next superstep, `GraphDrained`), put them back to `pending` and defer them; uvicorn `--timeout-graceful-shutdown` and Procrastinate `shutdown_graceful_timeout` must exceed the longest node timeout | `limits.shutdown_grace_s` (60) |
| Postgres connections | Separate pools: app, checkpointer, Procrastinate; budget logged at startup | `database.*_pool_max` (5, 10, 5) |
| Loop health | Lag sampler (sleep drift every 1 s): gauge, warning over threshold, `/readyz` | `limits.loop_lag_warn_ms` (100) |
| Error isolation | Tool and MCP errors become error `ToolMessage`s; a failing run never touches another; supersteps are transactional | none |
| Blocking-call detection | `blockbuster` in the platform CI and in the starter CI | none |

Tools are async by contract; sync tools must be declared `isolation = "thread"` (validated). Horizontal scale is more processes, watching the sum of pools against Postgres `max_connections`.

## 9. LLM profiles, observability and usage

**Profiles** (ported from agintis, TOML instead of YAML **(OD-6)**):

```toml
[llm.profiles.fast]
provider = "openai"
model = "gpt-5.4-mini"
api_key_env = "OPENAI_API_KEY"   # the variable name, never the key
temperature = 0
timeout_s = 60
```

Kept rules: keyed providers must name `api_key_env`; `base_url` must be https except localhost; `stream_usage` on for OpenAI-compatible providers so streamed runs report tokens. Built with `init_chat_model`; provider packages are extras (`[openai]`, `[anthropic]`, `[ollama]`).

**Observability** (`[otel]` extra): OpenInference LangChain instrumentation exported via OTLP; backend chosen by config (Phoenix today). Span attributes `graph_agent.flow`, `graph_agent.run_id`, `graph_agent.thread_id` defined in one module. Boot fails if LangSmith tracing env vars are on (ported guard). Metrics (loop lag, executor occupancy, pool stats, semaphore waiters, active runs) go through OTel when enabled, else to logs.

**Usage ledger**: a per-run callback aggregates `usage_metadata` per `(model, agent)`; rows are upserted at run end, including failed and recovered runs (attempts accumulate).

| `graph_agent.usage` column | Notes |
|----------------------------|-------|
| `run_id`, `model`, `agent` (PK) | `agent` = `AgentSpec.name`, `""` for direct flow calls |
| `flow`, `thread_id`, `tenant_id`, `created_at` | Reporting dimensions |
| `input_tokens`, `output_tokens`, `cached_input_tokens`, `reasoning_tokens`, `calls` | Raw counts only |

Money is never stored: `graph-agent usage` prices rows at read time with `genai-prices`; unknown models are reported as unpriced, never as $0.

## 10. Configuration and persistence

**One settings story** (fixes agintis' 13 `BaseSettings` classes each loading `.env`, and the `Settings.isolated` test hack):

| Rule | Value |
|------|-------|
| One root `Settings` | Nested pydantic models per group (`database`, `api`, `limits`, `llm`, `mcp`, `observability`, `runs`, `threads`), plus the manifest (`flows`, `tools`) |
| Precedence | init args > environment > `.env` > `graph-agent.toml` > defaults |
| Environment | Prefix `GRAPH_AGENT_`, nested delimiter `__` (`GRAPH_AGENT_DATABASE__URL`) |
| Files | One `.env` (path `GRAPH_AGENT_ENV_FILE`), one `graph-agent.toml` (path `GRAPH_AGENT_CONFIG`) |
| Secrets | Env only; the TOML holds env var names (`api_key_env`, `headers_env`) |
| Tests | `Settings(_env_file=None, _toml_file=...)` with explicit values; no global cache |

**Persistence:**

| Schema | Owner | Setup |
|--------|-------|-------|
| `graph_agent` (`runs`, `usage`, `schema_version`) | graph-agent | `migrate`: numbered SQL files, one transaction, advisory lock **(OD-5)** |
| LangGraph checkpoints | `langgraph-checkpoint-postgres` | `AsyncPostgresSaver.setup()`; pool with `autocommit=True`, `prepare_threshold=0`, `dict_row` |
| Procrastinate | Procrastinate | Its schema manager (upgrade path: open check) |

## 11. Testing

**Kit shipped to users (`graph_agent.testing`):**

```python
class FakeChatModel(BaseChatModel):          # scripted replies, tool calls allowed; bind_tools is a no-op
    def __init__(self, responses: Sequence[AIMessage | str]) -> None: ...
    calls: list[list[BaseMessage]]           # what the model received, for assertions

def flow_context(*, models: Mapping[str, BaseChatModel] = ..., tools: Mapping[str, BaseTool] = ...,
                 config: Mapping[str, Any] = ...) -> FlowContext: ...

@dataclass(frozen=True)
class FlowResult:
    values: dict[str, Any]
    interrupts: tuple[Interrupt, ...]
    events: tuple[RunEvent, ...]

async def run_flow(target: StateGraph | Callable[[FlowContext], Any], input: Any = None, *,
                   ctx: FlowContext | None = None, thread_id: str | None = None,
                   resume: Any = None) -> FlowResult: ...   # InMemorySaver, same runner mapping
```

Testing a flow: build `flow_context(models={"fast": FakeChatModel([...])})`, call `run_flow`, assert on `values`; call again with the same `thread_id` and `resume=` to test interrupts.

**Platform test layers:**

| Layer | Scope | Runs in |
|-------|-------|---------|
| Unit | Manifest and settings parsing, transitions, DTO mapping, limit wrappers | `pytest` default |
| Contract | Runner with `FakeChatModel` + `InMemorySaver`, SSE sequence, interrupts | `pytest` default |
| Integration | Postgres: migrations, claims, unique index, recovery, Procrastinate lock, prune | `pytest -m integration` + docker compose |
| Packaging | Fresh venv per workspace member; git-subdirectory install from a tag | CI |
| Bench | `bench/`: N concurrent runs, fake LLM, loop lag, RSS | Manual, before each release |

## 12. Starter and extension story

Copier template contents (only what the product owns, so `copier update` merges cleanly):

| File | Purpose |
|------|---------|
| `pyproject.toml` | Pins `graph-agent` by tag and extras; ruff/mypy/import-linter (forbids `graph_agent._internal`) |
| `graph-agent.toml` | One example flow, one profile, limits |
| `src/<pkg>/flows/example.py`, `tests/test_example.py` | Plain `StateGraph` flow and its fake-model test |
| `.env.example`, `docker-compose.yml`, `Dockerfile` (CPython 3.14.5+ slim, uv) | Runtime |
| `scripts/check.sh`, CI workflow | Same gate as the platform |
| `README.md` | Run, test, deploy, idempotency rules, `copier update` |

Product extension points: flows, tools and MCP servers via the manifest; agent behavior via `AgentSpec.middleware`; authentication via `api.auth = "module:callable"` **(OD-2)**. Anything else (extra HTTP routes, UI, ingestion) stays in the product.

## 13. Ported from agintis

| Piece | agintis path | What changes |
|-------|--------------|--------------|
| Composition root | `bootstrap/container.py` | Same `Container` + `AsyncExitStack`; drops RAG, ingest, tenancy wiring |
| Settings | `bootstrap/settings.py` | One root class, one prefix, nested delimiter, TOML source |
| Runner | `agent_kit/runtime/langgraph_runner.py` | Keeps astream v2 mapping and interrupt-by-id resume; drops chat events and `STREAM_TOKENS_TAG`; adds `stream_mode` passthrough, durability, timeout, leases |
| Run context | `agent_kit/runtime/context.py` | `RunContext` with ids only; `QueryVectors` dropped |
| Event DTOs and SSE | `modules/agents_catalog/domain/events.py`, `http.py` | Generic events (section 6); `EventSourceResponse` and id format kept |
| Run leases | `modules/conversations/application/run_leases.py`, `domain/thread.py` | Lease moves from a threads row to the runs row plus a partial unique index |
| Checkpointer pool | `platform/checkpointer/postgres.py` | Kept |
| LLM profiles | `platform/llm/profiles.py`, `models.py`, `bootstrap/llm.py` | YAML to TOML; adds shared httpx clients per provider |
| Usage ledger | `platform/usage/*` | SQLAlchemy/Alembic to SQL; `Purpose` replaced by `flow`; adds `agent`; own `prices.yaml` replaced by `genai-prices` |
| Tracing | `platform/observability/*`, `bootstrap/observability.py` | Port, lazy OTLP and foreign-tracing guard kept; attributes renamed |
| Contracts and gate | `pyproject.toml` (17 contracts), `scripts/check.sh` | 6 contracts plus ruff bans; gate adds per-member clean install |
| Not ported | `agent_kit/judge`, `agent_kit/nodes`, `modules/knowledge`, `platform/auth`, eval CLIs | Product code or out of scope |

## 14. v0.1 plan

### Implementation parts (each one reviewable unit, in order)

- [ ] 1. Workspace skeleton: root and core `pyproject.toml`, extras, ruff/mypy, import contracts, `check.sh`, CI with clean installs and the git-subdirectory install check.
- [ ] 2. Config: `Settings`, manifest models, lazy target import, `validate`.
- [ ] 3. Composition root: pools, LLM profiles and httpx clients, executors, loop-lag sampler, tracing port.
- [ ] 4. Persistence: SQL migrations (`runs`, `usage`), checkpointer setup, `migrate`.
- [ ] 5. Runtime: flow cache, `execute_run`, runs state machine, claims, leases, timeout, cancellation; `run` CLI (with `--memory`).
- [ ] 6. Testing kit and `examples/` hello and approval flows with tests.
- [ ] 7. HTTP API: endpoints, DTOs, SSE, admission, auth (OD-2), `serve`.
- [ ] 8. Worker: Procrastinate app, execute job with `lock=thread_id`, sweep (stalled jobs and orphan runs), prune, crons, `worker` and `crons`.
- [ ] 9. Agents kit: `AgentSpec`, factory, tool registry and isolation, `as_tool`, skills middleware, MCP port with the own adapter over the `mcp` SDK v2 (tested against an in-process FastMCP server from the same SDK); `examples/` research flow.
- [ ] 10. Usage ledger and `usage` report; OTel attributes and metrics.
- [ ] 11. Starter (Copier) and `copier update` documentation.
- [ ] 12. Bench run and exit check: agintis' flows run unchanged on graph-agent (the migration itself happens in the agintis repo).

### Out of scope for v0.1

Studio and `langgraph-sdk` compatibility, Agent Protocol, A2A; Temporal/DBOS; `[rag]` and `[deepagents]` implementations, RAG ingestion; skill script execution; stdio MCP, MCP resources and prompts, MCP input requests (`InputRequiredResult`); tenancy beyond `tenant_id` and the auth hook; UI; webhooks; `Last-Event-ID` replay; joining the stream of a background run from another process; `Idempotency-Key` on run creation; double-texting strategies other than reject; extra HTTP routers; price overrides; tool selection by embeddings; free-threaded Python and Python 3.15.

### Open checks to verify in code

| Check | Why it matters |
|-------|----------------|
| Own MCP adapter: one `mcp.Client` kept open for the process reuses its session; reconnect after a server restart; a failure on one request does not kill concurrent ones (python-sdk #2604, fixed only in v2: confirm in the pinned 2.x); image and resource blocks reach the chosen chat models | MCP port design |
| Checkpoint metadata carries `metadata.run_id` from the config | Recovery input choice (section 5) |
| Procrastinate periodic tasks can be registered from config (one per cron, with kwargs) and its schema upgrades have a scripted path | Crons, `migrate` |
| Each provider integration accepts a shared `http_async_client` with limits (else a delegating model wrapper is needed) | Provider concurrency |
| `create_agent(name=...)` exposes the agent name in callback metadata | Usage per subagent |
| `uv add "git+...@tag#subdirectory=packages/core"` works | Distribution |
| `AsyncPostgresSaver` serializes writes per instance (forum claim) | Checkpointer pool sizing |
| Runtime context reaches nodes when a graph declares no `context_schema` | Flow contract |
| `ProcessPoolExecutor.kill_workers` behavior on 3.14; fallback on 3.13 | CPU tool timeout |
| Studio via `langgraph dev` can load a shim that builds factories with a dev `FlowContext` | Dev experience |
| langgraph on 3.14 (classifiers stop at 3.13; CI tests 3.14) | Runtime image |
| `RunControl` import path and how it is passed to `astream` in the installed langgraph 1.2.x (sources disagree; no official page found) | Graceful shutdown |
| Lease expiry while the owner is alive (event-loop stall) cannot double-run: the sweep only requeues if the conditional claim wins and the partial unique index holds | Never run a thread twice |

## Resolved decisions (2026-11-04)

All eight were resolved by adopting the resolution below. OD-1 was confirmed by two independent researchers; OD-4 and OD-7 by measurements and package metadata. To change one, record it first in `.claude/agents/_shared/decisions.md`.

| # | Decision | Options | Resolution |
|---|----------|---------|----------------|
| OD-1 | Where inline streamed runs execute | (a) In the API process, same `execute_run`, queue only for background and cron; (b) everything via the queue, events relayed with `LISTEN/NOTIFY`; (c) via the queue with a `run_events` table for replay | (a), confirmed by both researchers: ports agintis' working SSE path, no cross-process bus; the partial unique index, leases and the orphan sweep cover API crashes. (b) rejected: 8000-byte NOTIFY payload, no replay, global commit lock. Move to (c) (table as log, NOTIFY only as a payload-less wake-up) when clients need reconnect or replay, stream join across clients, more than 2 replicas, or deploys keep cutting minutes-long interactive runs. Long runs should use background mode plus polling |
| OD-2 | Authentication in v0.1 | (a) `none` (localhost only) + static API keys + `"module:callable"` hook returning `Principal(subject, tenant_id)`; (b) port agintis' JWT/JWKS verifier now; (c) no auth | (a): covers self-hosted use; agintis' JWT verifier plugs in through the hook from the product side, or ships as a `[jwt]` extra later |
| OD-3 | Threads table | (a) None: thread flow, version and tenant pinned by its first run, listing by `runs`; (b) small `threads` table (owner, flow, metadata, TTL) | (a): honors "own tables: runs, usage"; add (b) when thread metadata or listing per tenant gets slow |
| OD-4 | Default durability | (a) `sync` default, `async`/`exit` per flow; (b) `async` default (performance report) | (a): the platform report ties recovery of side-effect flows to `sync`; flows without effects opt into `async` |
| OD-5 | Migrations for own tables | (a) Numbered SQL files + ~60-line runner; (b) Alembic + SQLAlchemy Core (agintis) | (a): two tables do not justify SQLAlchemy and Alembic as runtime dependencies of every product |
| OD-6 | Config file format | (a) One `graph-agent.toml` (manifest, profiles, MCP, limits); (b) TOML manifest + YAML profiles (brief, agintis); (c) manifest in `pyproject.toml` `[tool.graph-agent]` (platform report) | (a): one file, stdlib `tomllib`, no PyYAML; runtime config does not belong in packaging metadata |
| OD-7 | Python floor | (a) `>=3.13`, images on 3.14.5+; (b) `>=3.14` | (a) until langgraph classifies 3.14; then (b), which also guarantees `kill_workers` |
| OD-8 | Token streaming | (a) Opt-in per request via `stream_mode` `messages` (client filters by `node`); (b) opt-in per model via a tag (agintis `streamed()`) | (a): no platform helper, LangGraph vocabulary; add a tag filter only if bandwidth becomes a problem |

## Refinements from the task breakdown (2026-11-04)

Found while writing `docs/tasks/`; each brief already follows them.

| # | Refinement | Why |
|---|------------|-----|
| R1 | `runtime/checkpointer.py` exposes `setup_checkpointer`; the `migrate` CLI chains own SQL migrations, checkpointer setup and (from T25) the Procrastinate schema | `db` cannot import `runtime` (C1); `queue` lands after `migrate` |
| R2 | `runtime/runner.py` splits `stream_graph` (no database) from `execute_run` (claim, leases, finish) | `run --memory` and tests must run a flow without Postgres |
| R3 | New module `runtime/offline.py`: fake chat model used by `validate --offline`; `graph_agent.testing` re-exports it | `_internal` cannot import `testing` (C2) and `validate` lands before the testing kit |
| R4 | `RunQueue` Protocol defined in the runtime (T16), implemented by `queue.py` (T24) | background endpoints (T23) must not depend on the worker task |
| R5 | `Settings` grows in two steps: scalar groups first (T04), then `flows`, `tools`, `llm`, `mcp` (T05) | manifest models are owned by T05 |
| R6 | `graph_agent.testing.flow_context(..., mcp=None)` accepts a fake MCP tool source | the research example (T31) is tested without network |
| R7 | Defaults not fixed above (`runs.heartbeat_s`, `runs.lease_s`, `runs.max_attempts`, `threads.*`, API port) are named constants chosen in T04 and reported back | keep magic numbers out of the code |
