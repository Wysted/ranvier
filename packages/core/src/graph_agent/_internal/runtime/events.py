"""Transport-agnostic run events and the mapping from LangGraph v2 stream parts."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import AIMessageChunk

from graph_agent._internal.runtime.context import RunContext

INTERRUPT_KEY = "__interrupt__"


@dataclass(frozen=True, slots=True)
class UsageTotals:
    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0


@dataclass(frozen=True, slots=True)
class MetadataEvent:
    run_id: str
    thread_id: str
    flow: str
    flow_version: int
    attempt: int


@dataclass(frozen=True, slots=True)
class UpdatesEvent:
    ns: tuple[str, ...]
    node: str
    update: Any


@dataclass(frozen=True, slots=True)
class ValuesEvent:
    ns: tuple[str, ...]
    values: Any


@dataclass(frozen=True, slots=True)
class MessagesEvent:
    ns: tuple[str, ...]
    node: str
    id: str | None
    type: str
    content: Any
    tool_call_chunks: Sequence[Mapping[str, Any]]


@dataclass(frozen=True, slots=True)
class CustomEvent:
    ns: tuple[str, ...]
    node: str | None  # custom parts carry no node; kept for the DTO shape
    data: Any


@dataclass(frozen=True, slots=True)
class InterruptItem:
    id: str
    value: Any


@dataclass(frozen=True, slots=True)
class InterruptEvent:
    interrupts: tuple[InterruptItem, ...]


@dataclass(frozen=True, slots=True)
class EndEvent:
    status: str
    usage: UsageTotals


@dataclass(frozen=True, slots=True)
class ErrorEvent:
    code: str
    message: str
    retryable: bool


RunEvent = (
    MetadataEvent
    | UpdatesEvent
    | ValuesEvent
    | MessagesEvent
    | CustomEvent
    | InterruptEvent
    | EndEvent
    | ErrorEvent
)


@dataclass(frozen=True, slots=True)
class RunOutcome:
    status: str
    usage: UsageTotals
    error: ErrorEvent | None = None


class RunObserver(Protocol):
    """Hook around a run: extra LangChain callbacks in, final outcome out."""

    def callbacks(self, ctx: RunContext) -> Sequence[BaseCallbackHandler]: ...

    async def on_finish(self, ctx: RunContext, outcome: RunOutcome) -> None: ...


def map_stream_part(part: Mapping[str, Any]) -> RunEvent | None:
    """Map one `version="v2"` stream part.

    Returns None only for non-LLM `messages` parts (design: LLM chunks only). A `values`
    part's `interrupts` duplicate `updates.__interrupt__` and are not mapped again.
    Unknown part types raise.
    """
    kind = part["type"]
    ns = tuple(part["ns"])
    data = part["data"]
    if kind == "updates":
        if INTERRUPT_KEY in data:
            return InterruptEvent(tuple(InterruptItem(i.id, i.value) for i in data[INTERRUPT_KEY]))
        if len(data) != 1:
            raise ValueError(f"updates part must carry exactly one node, got {sorted(data)}")
        ((node, update),) = data.items()
        return UpdatesEvent(ns, node, update)
    if kind == "values":
        return ValuesEvent(ns, data)
    if kind == "messages":
        chunk, metadata = data
        if not isinstance(chunk, AIMessageChunk):
            return None
        return MessagesEvent(
            ns,
            metadata["langgraph_node"],
            chunk.id,
            chunk.type,
            chunk.content,
            chunk.tool_call_chunks,
        )
    if kind == "custom":
        return CustomEvent(ns, None, data)
    raise ValueError(f"unknown stream part type {kind!r}")
