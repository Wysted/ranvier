"""Usage ledger (design section 9): per-attempt token aggregation, repository and cost report.

The runner owns one `UsageCallback` per attempt; `UsageObserver.on_finish` upserts its sums into
`graph_agent.usage`. Money is never stored: `build_report` prices the token counts with
`genai-prices` at read time, with the prices current at that moment.

This layer sits below `runtime`, so `UsageObserver` matches the `RunObserver` protocol
structurally (a small local protocol for the run context) instead of importing it.
"""

import threading
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal
from typing import Any, Final, LiteralString, Protocol
from uuid import UUID

from langchain_core.callbacks import AsyncCallbackHandler, BaseCallbackHandler
from langchain_core.messages import AIMessage
from langchain_core.outputs import LLMResult
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

# Callback metadata key carrying AgentSpec.name (S3); `agents/factory.py` imports it from here.
AGENT_METADATA_KEY: Final = "graph_agent_agent"
UNKNOWN_MODEL: Final = "unknown"  # a client that reports no model name
NO_AGENT: Final = ""  # direct flow calls


@dataclass(slots=True)
class Counts:
    """Raw token counts; the first three names match `runtime.events.UsageTotals` on purpose."""

    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0
    reasoning_tokens: int = 0
    calls: int = 0

    def add(self, other: "Counts") -> None:
        self.input_tokens += other.input_tokens
        self.output_tokens += other.output_tokens
        self.cached_input_tokens += other.cached_input_tokens
        self.reasoning_tokens += other.reasoning_tokens
        self.calls += other.calls


def sum_counts(items: Iterable[Counts]) -> Counts:
    total = Counts()
    for counts in items:
        total.add(counts)
    return total


def _call_counts(response: LLMResult) -> Counts:
    """What one finished call reported; `calls` is 1 even when no usage came back."""
    counts = Counts(calls=1)
    for generations in response.generations:
        for generation in generations:
            message = getattr(generation, "message", None)
            usage = message.usage_metadata if isinstance(message, AIMessage) else None
            if usage:
                counts.input_tokens += usage.get("input_tokens", 0)
                counts.output_tokens += usage.get("output_tokens", 0)
                input_details = usage.get("input_token_details") or {}
                counts.cached_input_tokens += input_details.get("cache_read", 0)
                output_details = usage.get("output_token_details") or {}
                counts.reasoning_tokens += output_details.get("reasoning", 0)
    return counts


class UsageCallback(AsyncCallbackHandler):
    """Sums `usage_metadata` per `(model, agent)` over every LLM call it sees.

    A failed call counts as a call with no tokens (it may have been billed).
    shortcut: SDK-level HTTP retries are invisible here and a stream that dies midway reports
    no usage, so totals are a lower bound; raise it if retries become a real cost source.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()  # sync tools in threads fire callbacks off the loop
        self._inflight: dict[UUID, tuple[str, str]] = {}
        self._by_key: dict[tuple[str, str], Counts] = defaultdict(Counts)

    async def on_chat_model_start(
        self,
        serialized: dict[str, Any],
        messages: Any,
        *,
        run_id: UUID,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        meta = metadata or {}
        key = (
            str(meta.get("ls_model_name") or UNKNOWN_MODEL),
            str(meta.get(AGENT_METADATA_KEY) or NO_AGENT),
        )
        with self._lock:
            self._inflight[run_id] = key

    async def on_llm_end(self, response: LLMResult, *, run_id: UUID, **kwargs: Any) -> None:
        self._record(run_id, _call_counts(response))

    async def on_llm_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        # A stream that failed after its last chunk can still carry the partial result.
        response = kwargs.get("response")
        self._record(
            run_id, _call_counts(response) if isinstance(response, LLMResult) else Counts(calls=1)
        )

    def _record(self, run_id: UUID, counts: Counts) -> None:
        with self._lock:
            key = self._inflight.pop(run_id, (UNKNOWN_MODEL, NO_AGENT))
            self._by_key[key].add(counts)

    def snapshot(self) -> dict[tuple[str, str], Counts]:
        with self._lock:
            return {key: replace(counts) for key, counts in self._by_key.items()}

    def totals(self) -> Counts:
        return sum_counts(self.snapshot().values())


class _RunInfo(Protocol):
    """The slice of `RunContext` the ledger reads."""

    @property
    def run_id(self) -> str: ...
    @property
    def thread_id(self) -> str: ...
    @property
    def flow(self) -> str: ...
    @property
    def tenant_id(self) -> str | None: ...


class _Outcome(Protocol):
    """The slice of `RunOutcome` the ledger reads."""

    @property
    def usage_by_key(self) -> Mapping[tuple[str, str], Counts]: ...


_UPSERT: Final[LiteralString] = """
INSERT INTO graph_agent.usage
    (run_id, model, agent, flow, thread_id, tenant_id,
     input_tokens, output_tokens, cached_input_tokens, reasoning_tokens, calls)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (run_id, model, agent) DO UPDATE SET
    input_tokens = usage.input_tokens + EXCLUDED.input_tokens,
    output_tokens = usage.output_tokens + EXCLUDED.output_tokens,
    cached_input_tokens = usage.cached_input_tokens + EXCLUDED.cached_input_tokens,
    reasoning_tokens = usage.reasoning_tokens + EXCLUDED.reasoning_tokens,
    calls = usage.calls + EXCLUDED.calls
"""

# One full query per `--by` (fixed SQL, never built from user input).
_REPORT_SQL: Final[dict[str, LiteralString]] = {
    by: f"""
        SELECT {by} AS grp, model,
               sum(input_tokens)::bigint AS i, sum(output_tokens)::bigint AS o,
               sum(cached_input_tokens)::bigint AS c, sum(calls)::bigint AS n
        FROM graph_agent.usage WHERE created_at >= %s
        GROUP BY 1, 2 ORDER BY 1, 2
    """
    for by in ("flow", "model", "agent")
}
GROUP_BYS: Final = tuple(_REPORT_SQL)


@dataclass(frozen=True, slots=True)
class ReportRow:
    group: str
    model: str
    input_tokens: int
    output_tokens: int
    cached_input_tokens: int
    calls: int


class UsageRepository:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    async def add(self, ctx: _RunInfo, usage: Mapping[tuple[str, str], Counts]) -> None:
        """Add this attempt's counts to the run's rows (one transaction, additive on conflict).

        shortcut: at-most-once. The runner calls this once per attempt and never retries, so a
        failed write (DB down) loses that attempt's counts, and a crash between the terminal
        write and this call loses them too; move to an outbox row written in the terminal
        transaction if the ledger must be exact.
        """
        if not usage:
            return
        params = [
            (
                ctx.run_id,
                model,
                agent,
                ctx.flow,
                ctx.thread_id,
                ctx.tenant_id,
                c.input_tokens,
                c.output_tokens,
                c.cached_input_tokens,
                c.reasoning_tokens,
                c.calls,
            )
            for (model, agent), c in sorted(usage.items())  # stable lock order
        ]
        async with self._pool.connection() as conn, conn.cursor() as cur:
            await cur.executemany(_UPSERT, params)

    async def report_rows(self, since: datetime, by: str) -> list[ReportRow]:
        """Rows since `since` (by `created_at`, the first attempt's date: tokens a run recovered
        on a later day adds land on its old row)."""
        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(_REPORT_SQL[by], (since,))
            return [
                ReportRow(r["grp"], r["model"], r["i"], r["o"], r["c"], r["n"])
                for r in await cur.fetchall()
            ]


class UsageObserver:
    """`RunObserver`: upserts the attempt's counts (the runner's `usage_by_key`) when it ends.

    Stateless: the runner owns the per-attempt `UsageCallback`, so it adds no callbacks.
    """

    def __init__(self, repo: UsageRepository) -> None:
        self._repo = repo

    def callbacks(self, ctx: _RunInfo) -> Sequence[BaseCallbackHandler]:
        return ()

    async def on_finish(self, ctx: _RunInfo, outcome: _Outcome) -> None:
        await self._repo.add(ctx, outcome.usage_by_key)


@dataclass(slots=True)
class ReportLine:
    group: str
    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0
    calls: int = 0  # calls that reported tokens
    calls_without_usage: int = 0
    cost_usd: Decimal = Decimal(0)  # priced rows only; meaningless while `priced` is False
    priced: bool = False
    unpriced_models: set[str] = field(default_factory=set)


LOCAL_PROVIDERS: Final = frozenset({"ollama"})  # no per-token price


def _price(row: ReportRow, provider: str | None) -> Decimal | None:
    """Estimated USD at today's prices; None for local models or ones genai-prices can't price.

    shortcut: the ledger stores no provider, so it comes from today's LLM profiles (model ->
    provider); an unmapped model is priced by name across providers. Add a provider column
    if profiles change or the same model name is served by several providers.
    """
    if provider in LOCAL_PROVIDERS:
        return None
    from genai_prices import Usage, calc_price  # lazy: ~100 ms, only the CLI report needs it

    usage = Usage(
        input_tokens=row.input_tokens,
        cache_read_tokens=row.cached_input_tokens,
        output_tokens=row.output_tokens,
    )
    try:
        return calc_price(usage, row.model, provider_id=provider).total_price
    except (LookupError, ValueError):  # unknown model, or a usage the model's price cannot take
        return None


def build_report(
    rows: Iterable[ReportRow], providers: Mapping[str, str] | None = None
) -> list[ReportLine]:
    """Sum rows per group; unpriceable models are listed and never counted as $0.

    `providers` maps model name to provider id (see `_price`).

    shortcut: a row that mixes calls with and without usage counts all its calls as with usage
    (the ledger sums per `(run, model, agent)`); split the column if that matters.
    """
    lines: dict[str, ReportLine] = {}
    for row in rows:
        line = lines.setdefault(row.group, ReportLine(row.group))
        if row.input_tokens == 0 and row.output_tokens == 0:
            line.calls_without_usage += row.calls  # tokens unknown: nothing to price
            continue
        line.input_tokens += row.input_tokens
        line.output_tokens += row.output_tokens
        line.cached_input_tokens += row.cached_input_tokens
        line.calls += row.calls
        cost = _price(row, (providers or {}).get(row.model))
        if cost is None:
            line.unpriced_models.add(row.model)
        else:
            line.cost_usd += cost
            line.priced = True
    return list(lines.values())


def format_report(lines: Sequence[ReportLine], by: str) -> str:
    header = (by, "calls", "no usage", "input", "cached", "output", "est. cost (USD)")
    body = []
    for ln in lines:
        cost = f"{ln.cost_usd:.4f}" if ln.priced else "-"
        if ln.unpriced_models:
            cost += " + unpriced: " + ", ".join(sorted(ln.unpriced_models))
        body.append(
            (
                ln.group or "-",
                str(ln.calls),
                str(ln.calls_without_usage),
                str(ln.input_tokens),
                str(ln.cached_input_tokens),
                str(ln.output_tokens),
                cost,
            )
        )
    widths = [max(len(r[i]) for r in [header, *body]) for i in range(len(header))]
    out = ["  ".join(c.ljust(w) for c, w in zip(r, widths, strict=True)) for r in [header, *body]]
    out.append("Cost is an ESTIMATE at current genai-prices prices, not a bill; nothing is stored.")
    return "\n".join(out)
