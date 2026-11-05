"""Shared by the checkpointer unit and integration tests: a graph that drains after node one."""

from typing import Any, TypedDict

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.runtime import RunControl


class State(TypedDict):
    trace: list[str]


def build_drain_graph(
    saver: BaseCheckpointSaver[Any], control: RunControl, calls: list[str]
) -> CompiledStateGraph[State, Any, State, State]:
    def first(state: State) -> State:
        calls.append("first")
        control.request_drain()
        return {"trace": ["first"]}

    def second(state: State) -> State:
        calls.append("second")
        return {"trace": [*state["trace"], "second"]}

    builder = StateGraph(State)
    builder.add_node("first", first)
    builder.add_node("second", second)
    builder.add_edge(START, "first")
    builder.add_edge("first", "second")
    builder.add_edge("second", END)
    return builder.compile(checkpointer=saver)
