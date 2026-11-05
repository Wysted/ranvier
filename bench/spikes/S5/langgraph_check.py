"""S5 spike: langgraph + langgraph-checkpoint-postgres import and run a tiny graph on 3.14.

Run with the project venv (locked versions): `uv run python bench/spikes/S5/langgraph_check.py`.
No database: the Postgres saver is only imported. Throwaway.
"""

import asyncio
import sys
import warnings
from importlib.metadata import version
from typing import TypedDict

PACKAGES = (
    "langgraph",
    "langgraph-checkpoint",
    "langgraph-checkpoint-postgres",
    "langchain-core",
    "langsmith",
    "psycopg",
    "psycopg-binary",
    "psycopg-pool",
    "ormsgpack",
    "pydantic",
    "pydantic-core",
    "xxhash",
)


class State(TypedDict):
    n: int
    answer: str


def inc(state: State) -> dict[str, int]:
    return {"n": state["n"] + 1}


def review(state: State) -> dict[str, str]:
    from langgraph.types import interrupt

    return {"answer": interrupt({"n": state["n"]})}


async def main() -> None:
    with warnings.catch_warnings(record=True) as caught:
        # Surface DeprecationWarning and friends raised at import and run time.
        warnings.simplefilter("default")
        import psycopg
        from langgraph.checkpoint.memory import InMemorySaver
        from langgraph.checkpoint.postgres import PostgresSaver
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
        from langgraph.graph import END, START, StateGraph
        from langgraph.types import Command

        print(f"python={sys.version.split()[0]} gil_enabled={sys._is_gil_enabled()}")
        for pkg in PACKAGES:
            print(f"  {pkg}=={version(pkg)}")
        print(f"  psycopg.pq.__impl__={psycopg.pq.__impl__}")
        print(f"  imported {PostgresSaver.__name__}, {AsyncPostgresSaver.__name__}")

        graph = (
            StateGraph(State)
            .add_node("inc", inc)
            .add_node("review", review)
            .add_edge(START, "inc")
            .add_edge("inc", "review")
            .add_edge("review", END)
            .compile(checkpointer=InMemorySaver())
        )
        cfg = {"configurable": {"thread_id": "s5"}}
        first = graph.invoke({"n": 1, "answer": ""}, cfg)
        print(f"  invoke -> {first}")
        resumed = graph.invoke(Command(resume="ok"), cfg)
        print(f"  resume -> {resumed}")
        assert resumed == {"n": 2, "answer": "ok"}, resumed

        cfg2 = {"configurable": {"thread_id": "s5-async"}}
        updates = [
            u
            async for u in graph.astream(
                {"n": 10, "answer": ""}, cfg2, stream_mode="updates"
            )
        ]
        print(f"  astream(updates) -> {updates}")
        final = await graph.ainvoke(Command(resume="async-ok"), cfg2)
        assert final == {"n": 11, "answer": "async-ok"}, final
        snap = await graph.aget_state(cfg2)
        print(f"  aget_state.values={snap.values} next={snap.next}")

    print(f"warnings captured: {len(caught)}")
    for w in caught:
        print(f"  {w.category.__name__}: {w.message} ({w.filename}:{w.lineno})")
    print("PASS")


if __name__ == "__main__":
    asyncio.run(main())
