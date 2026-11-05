"""Transport-agnostic run events and the mapping from LangGraph v2 stream parts."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import AIMessage, AIMessageChunk

from graph_agent._internal.runtime.context import RunContext
from graph_agent._internal.usage import Counts

INTERRUPT_KEY = "__interrupt__"
# Reserved `updates` key: LangGraph re-emits cached writes as `{"__metadata__": {"cached": True}}`.
UPDATES_METADATA_KEY = "__metadata__"
# Set by chat models on every call, streamed or not; node-returned messages never carry it.
LLM_METADATA_KEY = "ls_provider"


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
    node: str | None  # last segment of ns (node name); None at the root
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
    """What `RunObserver.on_finish` gets; `status` is `running` when the attempt ended without a
    terminal write (drain, lost lease): the ledger still records the tokens it spent."""

    status: str
    usage: UsageTotals
    error: ErrorEvent | None = None
    usage_by_key: dict[tuple[str, str], Counts] = field(default_factory=dict)  # (model, agent)


class RunObserver(Protocol):
    """Hook around a run: extra LangChain callbacks in, final outcome out."""

    def callbacks(self, ctx: RunContext) -> Sequence[BaseCallbackHandler]: ...

    async def on_finish(self, ctx: RunContext, outcome: RunOutcome) -> None: ...


def map_stream_part(part: Mapping[str, Any]) -> RunEvent | None:
    """Map one `version="v2"` stream part.

    Returns None for non-LLM `messages` parts (design: LLM output only, streamed chunks or
    whole messages from non-streaming models) and for `__interrupt__` updates from a
    subgraph (the root re-emits them with the same id). A `values` part's `interrupts`
    duplicate `updates.__interrupt__` and are not mapped again. Unknown part types raise.
    """
    kind = part["type"]
    ns = tuple(part["ns"])
    data = part["data"]
    if kind == "updates":
        if INTERRUPT_KEY in data:
            if ns:
                return None
            return InterruptEvent(tuple(InterruptItem(i.id, i.value) for i in data[INTERRUPT_KEY]))
        data = {k: v for k, v in data.items() if k != UPDATES_METADATA_KEY}
        if len(data) != 1:
            raise ValueError(f"updates part must carry exactly one node, got {sorted(data)}")
        ((node, update),) = data.items()
        return UpdatesEvent(ns, node, update)
    if kind == "values":
        return ValuesEvent(ns, data)
    if kind == "messages":
        chunk, metadata = data
        if not isinstance(chunk, AIMessageChunk) and not (
            isinstance(chunk, AIMessage) and LLM_METADATA_KEY in metadata
        ):
            return None
        return MessagesEvent(
            ns,
            metadata["langgraph_node"],
            chunk.id,
            chunk.type,
            chunk.content,
            chunk.tool_call_chunks if isinstance(chunk, AIMessageChunk) else (),
        )
    if kind == "custom":
        return CustomEvent(ns, ns[-1].split(":")[0] if ns else None, data)
    raise ValueError(f"unknown stream part type {kind!r}")
