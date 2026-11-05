from typing import TypedDict

from langgraph.graph import END, START, StateGraph


class State(TypedDict):
    name: str
    greeting: str


def greet(state: State) -> dict[str, str]:
    return {"greeting": f"Hello, {state['name']}!"}


# The manifest target is this uncompiled builder; the platform compiles it.
graph = StateGraph(State)
graph.add_node("greet", greet)
graph.add_edge(START, "greet")
graph.add_edge("greet", END)
