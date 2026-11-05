"""Usage ledger (design section 9): per-attempt token aggregation by `(model, agent)`.

T16 owns only the callback; the repository, observer and cost report land with T32.
"""

import threading
from collections import defaultdict
from dataclasses import dataclass, replace
from typing import Any, Final
from uuid import UUID

from langchain_core.callbacks import AsyncCallbackHandler
from langchain_core.messages import AIMessage
from langchain_core.outputs import LLMResult

# shortcut: literal duplicated from `observability.tracing` (usage and observability are sibling
# layers and cannot import each other); a unit test pins both to the same value.
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
        self._record(run_id, Counts(calls=1))

    def _record(self, run_id: UUID, counts: Counts) -> None:
        with self._lock:
            key = self._inflight.pop(run_id, (UNKNOWN_MODEL, NO_AGENT))
            self._by_key[key].add(counts)

    def snapshot(self) -> dict[tuple[str, str], Counts]:
        with self._lock:
            return {key: replace(counts) for key, counts in self._by_key.items()}

    def totals(self) -> Counts:
        total = Counts()
        for counts in self.snapshot().values():
            total.add(counts)
        return total
