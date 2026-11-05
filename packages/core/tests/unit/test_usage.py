import asyncio
import uuid
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any, cast

import pytest
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage
from langchain_core.messages.ai import UsageMetadata
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, LLMResult

from graph_agent._internal import usage as usage_module
from graph_agent._internal.cli import USAGE_GROUP_BYS
from graph_agent._internal.runtime.context import RunContext
from graph_agent._internal.runtime.events import RunObserver, RunOutcome, UsageTotals
from graph_agent._internal.usage import (
    AGENT_METADATA_KEY,
    GROUP_BYS,
    Counts,
    ReportRow,
    UsageCallback,
    UsageObserver,
    UsageRepository,
    build_report,
    format_report,
)


def _result(**usage: object) -> LLMResult:
    msg = AIMessage(content="x", usage_metadata=usage or None)
    return LLMResult(generations=[[ChatGeneration(message=msg)]])


class _StreamingModel(GenericFakeChatModel):
    """A fake with `_astream` that reports usage on its last chunk (the generic one drops it)."""

    async def _astream(
        self, messages: list[BaseMessage], *args: Any, **kwargs: Any
    ) -> AsyncIterator[ChatGenerationChunk]:
        yield ChatGenerationChunk(message=AIMessageChunk(content="a"))
        usage = UsageMetadata(input_tokens=5, output_tokens=3, total_tokens=8)
        yield ChatGenerationChunk(message=AIMessageChunk(content="b", usage_metadata=usage))


async def _call(cb: UsageCallback, model: str, agent: str | None, result: LLMResult) -> None:
    run_id = uuid.uuid4()
    meta = {"ls_model_name": model} | ({AGENT_METADATA_KEY: agent} if agent else {})
    await cb.on_chat_model_start({}, [], run_id=run_id, metadata=meta)
    await cb.on_llm_end(result, run_id=run_id)


async def test_aggregates_per_model_and_agent() -> None:
    cb = UsageCallback()
    usage = {
        "input_tokens": 10,
        "output_tokens": 5,
        "total_tokens": 15,
        "input_token_details": {"cache_read": 4},
        "output_token_details": {"reasoning": 2},
    }
    await _call(cb, "m1", "writer", _result(**usage))
    await _call(cb, "m1", "writer", _result(**usage))
    await _call(cb, "m1", None, _result(input_tokens=1, output_tokens=1, total_tokens=2))
    await _call(cb, "m2", "writer", _result())  # provider reported no usage
    snap = cb.snapshot()
    assert snap[("m1", "writer")] == Counts(20, 10, 8, 4, 2)
    assert snap[("m1", "")] == Counts(1, 1, 0, 0, 1)
    assert snap[("m2", "writer")] == Counts(calls=1)
    assert cb.totals() == Counts(21, 11, 8, 4, 4)


async def test_failed_call_counts_as_call_without_tokens() -> None:
    cb = UsageCallback()
    run_id = uuid.uuid4()
    await cb.on_chat_model_start({}, [], run_id=run_id, metadata={"ls_model_name": "m"})
    await cb.on_llm_error(RuntimeError("boom"), run_id=run_id)
    assert cb.snapshot() == {("m", ""): Counts(calls=1)}


async def test_concurrent_calls_do_not_lose_counts() -> None:
    cb = UsageCallback()
    result = _result(input_tokens=1, output_tokens=1, total_tokens=2)
    await asyncio.gather(*(_call(cb, "m", "a", result) for _ in range(200)))
    await asyncio.to_thread(lambda: asyncio.run(_call(cb, "m", "a", result)))
    assert cb.snapshot()[("m", "a")].calls == 201


async def test_real_chat_model_reports_to_callback() -> None:
    cb = UsageCallback()
    model = GenericFakeChatModel(messages=iter([AIMessage(content="hi")]))
    await model.ainvoke("q", config={"callbacks": [cb], "metadata": {AGENT_METADATA_KEY: "a"}})
    ((model_name, agent),) = cb.snapshot()
    assert agent == "a" and model_name  # fake reports no usage: counted as a call
    assert cb.totals().calls == 1


async def test_streamed_call_reports_usage() -> None:
    cb = UsageCallback()
    model = _StreamingModel(messages=iter([]))
    async for _ in model.astream("q", config={"callbacks": [cb]}):
        pass
    assert cb.totals() == Counts(5, 3, 0, 0, 1)


async def test_failed_call_with_a_partial_response_keeps_its_tokens() -> None:
    cb = UsageCallback()
    run_id = uuid.uuid4()
    await cb.on_chat_model_start({}, [], run_id=run_id, metadata={"ls_model_name": "m"})
    partial = _result(input_tokens=4, output_tokens=1, total_tokens=5)
    await cb.on_llm_error(RuntimeError("cut"), run_id=run_id, response=partial)
    assert cb.snapshot() == {("m", ""): Counts(4, 1, 0, 0, 1)}


async def test_observer_is_stateless_and_writes_the_outcome() -> None:
    class Repo:
        def __init__(self) -> None:
            self.added: list[tuple[str, dict[tuple[str, str], Counts]]] = []

        async def add(self, ctx: RunContext, usage: dict[tuple[str, str], Counts]) -> None:
            self.added.append((ctx.run_id, usage))

    repo = Repo()
    observer: RunObserver = UsageObserver(cast(UsageRepository, repo))
    ctx = RunContext(str(uuid.uuid4()), str(uuid.uuid4()), "f", 1, 1, None)
    assert not observer.callbacks(ctx)
    by_key = {("m", ""): Counts(3, 2, 0, 0, 1)}
    await observer.on_finish(ctx, RunOutcome("success", UsageTotals(3, 2, 0), usage_by_key=by_key))
    assert repo.added == [(ctx.run_id, by_key)]


def test_cli_group_choices_match_the_ledger() -> None:
    assert USAGE_GROUP_BYS == GROUP_BYS


def test_report_prices_known_models_and_never_zeroes_unknown() -> None:
    rows = [
        ReportRow("flow", "gpt-5.4-mini", 1000, 100, 200, 3),
        ReportRow("flow", "my-local-llama", 500, 50, 0, 1),
        ReportRow("flow", "gpt-5.4-mini", 0, 0, 0, 2),  # calls without tokens
        ReportRow("other", "my-local-llama", 7, 7, 0, 1),
    ]
    flow, other = build_report(rows)
    assert flow.cost_usd == Decimal("0.001065")
    assert flow.unpriced_models == {"my-local-llama"}
    assert (flow.calls, flow.calls_without_usage, flow.input_tokens) == (4, 2, 1500)
    assert not other.priced and other.unpriced_models == {"my-local-llama"}
    out = format_report([flow, other], "flow")
    assert "0.0011 + unpriced: my-local-llama" in out
    assert "-  + unpriced" not in out and "  - + unpriced: my-local-llama" in out
    assert "ESTIMATE" in out


def test_report_survives_a_price_lookup_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*args: object) -> object:
        raise ValueError("bad usage")

    monkeypatch.setattr(usage_module, "calc_price", boom)
    (line,) = build_report([ReportRow("f", "gpt-5.4-mini", 1, 1, 0, 1)])
    assert not line.priced and line.unpriced_models == {"gpt-5.4-mini"}
