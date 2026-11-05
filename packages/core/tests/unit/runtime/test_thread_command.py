import uuid
from typing import TypedDict

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from graph_agent._internal.runtime.runner import check_thread_command, pending_interrupts
from graph_agent._internal.runtime.runs import ErrorCode, RunError

I1, I2, FOREIGN = "a" * 32, "b" * 32, "c" * 32  # LangGraph ids are 32 hex chars


class State(TypedDict, total=False):
    a: str
    b: str


def _build(saver: InMemorySaver):  # type: ignore[no-untyped-def]
    g = StateGraph(State)
    g.add_node("a", lambda s: {"a": interrupt("a?")})
    g.add_node("b", lambda s: {"b": interrupt("b?")})
    g.add_edge(START, "a")
    g.add_edge(START, "b")  # parallel: two interrupts in one superstep
    g.add_edge("a", END)
    g.add_edge("b", END)
    return g.compile(checkpointer=saver)


async def test_pending_interrupts_follow_the_checkpoint() -> None:
    saver = InMemorySaver()
    graph = _build(saver)
    thread = uuid.uuid4()
    config = {"configurable": {"thread_id": str(thread)}}
    assert await pending_interrupts(saver, thread) == ()
    await graph.ainvoke({}, config)
    ids = await pending_interrupts(saver, thread)
    assert len(ids) == 2 and len(set(ids)) == 2
    await graph.ainvoke(Command(resume={i: "x" for i in ids}), config)
    assert await pending_interrupts(saver, thread) == ()


def test_input_with_pending_interrupts_is_rejected() -> None:
    with pytest.raises(RunError) as exc:
        check_thread_command((I1,), input={"x": 1}, resume=None)
    assert exc.value.code is ErrorCode.THREAD_INTERRUPTED


@pytest.mark.parametrize(
    ("pending", "input", "resume"),
    [
        ((), {"x": 1}, None),  # fresh input
        ((I1,), None, "bare"),  # bare value, exactly one pending
        ((I1,), None, {"free": "form"}),  # a dict that is not an id map is a bare value
        ((I1, I2), None, {I1: 1, I2: 2}),  # map
        ((I1, I2), None, {I2: 2}),  # partial map
    ],
)
def test_valid_commands_pass(pending: tuple[str, ...], input: object, resume: object) -> None:
    check_thread_command(pending, input=input, resume=resume)


@pytest.mark.parametrize(
    "pending", [(), (I1, I2)], ids=["nothing-to-resume", "ambiguous-bare-value"]
)
def test_bare_resume_needs_exactly_one_pending(pending: tuple[str, ...]) -> None:
    with pytest.raises(RunError) as exc:
        check_thread_command(pending, input=None, resume="x")
    assert exc.value.code is ErrorCode.INVALID_REQUEST


@pytest.mark.parametrize(
    ("pending", "resume"),
    [
        ((I1,), {}),  # LangGraph reads `{}` as an (empty) id map
        ((I1,), {FOREIGN: "x"}),
        ((I1, I2), {I1: 1, FOREIGN: 2}),
        ((), {I1: 1}),
    ],
    ids=["empty-map", "foreign-id", "partly-foreign", "nothing-pending"],
)
def test_bad_id_maps_are_rejected(pending: tuple[str, ...], resume: object) -> None:
    with pytest.raises(RunError) as exc:
        check_thread_command(pending, input=None, resume=resume)
    assert exc.value.code is ErrorCode.INVALID_REQUEST
