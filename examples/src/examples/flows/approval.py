from typing import TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt


class State(TypedDict):
    request: str
    approved: bool
    result: str


def ask(state: State) -> dict[str, bool]:
    # Only the interrupt lives here: this node re-runs from the top on resume.
    # `is True`: a "no" or {"approved": false} answer must reject, not approve.
    return {"approved": interrupt(f"Approve: {state['request']}?") is True}


def finish(state: State) -> dict[str, str]:
    # Side effects belong in their own node, after the answer is in the state.
    return {"result": "done" if state["approved"] else "rejected"}


# The manifest target is this uncompiled builder; the platform compiles it.
graph = StateGraph(State)
graph.add_node("ask", ask)
graph.add_node("finish", finish)
graph.add_edge(START, "ask")
graph.add_edge("ask", "finish")
graph.add_edge("finish", END)
