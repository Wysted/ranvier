"""S1 spike: LangGraph runtime checks (RunControl, context, checkpoint metadata, v2 parts).

Run: uv run python bench/spikes/S1/checks.py
Throwaway script; every check prints its evidence and asserts the result.
"""

import asyncio
import importlib.metadata as md
import operator
import sys
from dataclasses import dataclass
from typing import Annotated, Any, TypedDict

from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.config import get_stream_writer
from langgraph.errors import GraphBubbleUp, GraphDrained
from langgraph.graph import END, START, StateGraph
from langgraph.runtime import RunControl, Runtime
from langgraph.types import Command, interrupt


@dataclass(frozen=True, slots=True)
class RunContext:
    run_id: str
    thread_id: str
    flow: str
    flow_version: int
    attempt: int
    tenant_id: str | None


class S(TypedDict):
    log: Annotated[list[str], operator.add]


def ctx(run_id: str, thread_id: str, attempt: int = 1) -> RunContext:
    return RunContext(run_id, thread_id, "demo", 1, attempt, None)


def cfg(thread_id: str, run_id: str) -> dict[str, Any]:
    return {"configurable": {"thread_id": thread_id}, "metadata": {"run_id": run_id}}


def header(title: str) -> None:
    print(f"\n=== {title} ===")


# ---------------------------------------------------------------- RunControl


def drain_graph(
    started: asyncio.Event, release: asyncio.Event, calls: list[str]
) -> StateGraph:
    async def a(state: S) -> S:
        calls.append("a")
        return {"log": ["a"]}

    async def b(state: S) -> S:
        calls.append("b")
        started.set()
        await release.wait()
        return {"log": ["b"]}

    async def c(state: S) -> S:
        calls.append("c")
        return {"log": ["c"]}

    g = StateGraph(S)
    g.add_node("a", a)
    g.add_node("b", b)
    g.add_node("c", c)
    g.add_edge(START, "a")
    g.add_edge("a", "b")
    g.add_edge("b", "c")
    g.add_edge("c", END)
    return g


async def check_drain(durability: str) -> None:
    header(f"RunControl.request_drain() durability={durability}")
    started, release, calls = asyncio.Event(), asyncio.Event(), []
    graph = drain_graph(started, release, calls).compile(checkpointer=InMemorySaver())
    control = RunControl()
    config = cfg("t-drain", "run-1")

    async def drainer() -> None:
        await started.wait()  # node b is running
        control.request_drain("sigterm")
        release.set()

    parts: list[Any] = []
    task = asyncio.create_task(drainer())
    try:
        async for p in graph.astream(
            {"log": []},
            config,
            control=control,
            stream_mode="updates",
            version="v2",
            durability=durability,
        ):
            parts.append(p["data"])
        raise AssertionError("expected GraphDrained")
    except GraphDrained as e:
        print(
            f"raised {type(e).__module__}.{type(e).__name__}: reason={e.reason!r} msg={e!s}"
            f" GraphBubbleUp={isinstance(e, GraphBubbleUp)}"
        )
    await task
    print("updates streamed before drain:", parts)
    snap = await graph.aget_state(config)
    print(
        "state after drain:",
        snap.values,
        "next:",
        snap.next,
        "metadata:",
        {k: snap.metadata.get(k) for k in ("source", "step", "run_id")},
    )
    print("node calls:", calls)
    assert snap.values == {"log": ["a", "b"]} and snap.next == ("c",)
    assert control.drain_requested and control.drain_reason == "sigterm"

    # reuse of a drained control drains again before any node runs
    try:
        async for _ in graph.astream(None, config, control=control, version="v2"):
            pass
        raise AssertionError("expected GraphDrained on reused control")
    except GraphDrained:
        print("reused drained RunControl -> GraphDrained again; calls:", calls)
    assert calls == ["a", "b"]

    # resume with a fresh control and None input
    out = await graph.ainvoke(
        None, cfg("t-drain", "run-2"), control=RunControl(), durability=durability
    )
    print("resume(None) output:", out, "calls:", calls)
    assert out == {"log": ["a", "b", "c"]} and calls == ["a", "b", "c"]


async def check_drain_subgraph() -> None:
    header("RunControl drain inside a subgraph (subgraphs=True)")
    started, release, calls = asyncio.Event(), asyncio.Event(), []
    inner = drain_graph(
        started, release, calls
    ).compile()  # inherits parent checkpointer

    async def pre(state: S) -> S:
        calls.append("pre")
        return {"log": ["pre"]}

    async def post(state: S) -> S:
        calls.append("post")
        return {"log": ["post"]}

    g = StateGraph(S)
    g.add_node("pre", pre)
    g.add_node("inner", inner)
    g.add_node("post", post)
    g.add_edge(START, "pre")
    g.add_edge("pre", "inner")
    g.add_edge("inner", "post")
    g.add_edge("post", END)
    graph = g.compile(checkpointer=InMemorySaver())
    control = RunControl()
    config = cfg("t-sub", "run-1")

    async def drainer() -> None:
        await started.wait()
        control.request_drain()
        release.set()

    task = asyncio.create_task(drainer())
    try:
        async for p in graph.astream(
            {"log": []},
            config,
            control=control,
            stream_mode="updates",
            subgraphs=True,
            version="v2",
        ):
            print("  part:", p["type"], p["ns"], p["data"])
        raise AssertionError("expected GraphDrained")
    except GraphDrained as e:
        print("raised GraphDrained reason:", e.reason)
    await task
    snap = await graph.aget_state(config, subgraphs=True)
    sub = snap.tasks[0].state if snap.tasks else None
    print("parent values:", snap.values, "next:", snap.next)
    print(
        "subgraph values:",
        getattr(sub, "values", None),
        "next:",
        getattr(sub, "next", None),
    )
    print("calls:", calls)
    out = await graph.ainvoke(None, cfg("t-sub", "run-2"), control=RunControl())
    print("resume(None) output:", out, "calls:", calls)
    assert calls == ["pre", "a", "b", "c", "post"], calls

    # baseline without drain: same output, so the duplicated "pre" is the reducer
    # (subgraph returns its full shared-key state), not a drain artifact
    started.clear()
    release.set()
    base = await graph.ainvoke({"log": []}, cfg("t-sub-base", "run-1"))
    print("baseline (no drain) output:  ", base)
    assert base == out


async def check_drain_last_node() -> None:
    header("drain requested while the LAST node runs")
    started, release, calls = asyncio.Event(), asyncio.Event(), []

    async def only(state: S) -> S:
        calls.append("only")
        started.set()
        await release.wait()
        return {"log": ["only"]}

    g = StateGraph(S)
    g.add_node("only", only)
    g.add_edge(START, "only")
    graph = g.compile(checkpointer=InMemorySaver())
    control = RunControl()

    async def drainer() -> None:
        await started.wait()
        control.request_drain()
        release.set()

    task = asyncio.create_task(drainer())
    out = await graph.ainvoke({"log": []}, cfg("t-last", "run-1"), control=control)
    await task
    snap = await graph.aget_state({"configurable": {"thread_id": "t-last"}})
    print(
        "no GraphDrained; output:",
        out,
        "next:",
        snap.next,
        "drain_requested:",
        control.drain_requested,
    )
    assert out == {"log": ["only"]} and snap.next == ()


# ---------------------------------------------------------------- context


async def check_context() -> None:
    header("runtime.context with and without context_schema")
    seen: dict[str, Any] = {}

    def build(schema: type | None, key: str) -> Any:
        async def node(state: S, runtime: Runtime[RunContext]) -> S:
            seen[key] = runtime.context
            return {"log": [key]}

        g = StateGraph(S, context_schema=schema) if schema else StateGraph(S)
        g.add_node("n", node)
        g.add_edge(START, "n")
        return g.compile(checkpointer=InMemorySaver())

    rc = ctx("r1", "t1")
    await build(RunContext, "with").ainvoke({"log": []}, cfg("t1", "r1"), context=rc)
    await build(None, "without").ainvoke({"log": []}, cfg("t2", "r1"), context=rc)
    await build(None, "without_dict").ainvoke(
        {"log": []}, cfg("t3", "r1"), context={"run_id": "r1"}
    )
    await build(RunContext, "with_dict").ainvoke(
        {"log": []},
        cfg("t4", "r1"),
        context={
            "run_id": "r1",
            "thread_id": "t4",
            "flow": "demo",
            "flow_version": 1,
            "attempt": 1,
            "tenant_id": None,
        },
    )
    await build(None, "none_passed").ainvoke({"log": []}, cfg("t5", "r1"))
    for k, v in seen.items():
        print(f"  {k}: {type(v).__name__} {v!r}")
    assert seen["with"] is rc and seen["without"] is rc
    assert seen["without_dict"] == {"run_id": "r1"}  # no coercion without schema
    assert isinstance(seen["with_dict"], RunContext)  # coerced with dataclass schema
    assert seen["none_passed"] is None

    # incomplete dict against a dataclass schema: exact failure mode
    try:
        await build(RunContext, "bad").ainvoke(
            {"log": []}, cfg("t6", "r1"), context={"run_id": "r1"}
        )
        raise AssertionError("expected TypeError")
    except TypeError as e:
        print("  with schema + incomplete dict ->", type(e).__name__, e)

    # subgraph without schema receives the parent's context
    inner = build(None, "subgraph")
    g = StateGraph(S)
    g.add_node("inner", inner)
    g.add_edge(START, "inner")
    await g.compile(checkpointer=InMemorySaver()).ainvoke(
        {"log": []}, cfg("t7", "r1"), context=rc
    )
    print("  subgraph (no schema anywhere):", seen["subgraph"])
    assert seen["subgraph"] is rc


# ---------------------------------------------------------------- metadata


def interrupt_graph() -> Any:
    async def ask(state: S) -> S:
        return {"log": [f"ask:{interrupt('approve?')}"]}

    async def done(state: S) -> S:
        return {"log": ["done"]}

    g = StateGraph(S)
    g.add_node("ask", ask)
    g.add_node("done", done)
    g.add_edge(START, "ask")
    g.add_edge("ask", "done")
    return g


async def check_metadata() -> None:
    header("config['metadata']['run_id'] -> checkpoint.metadata (InMemorySaver)")
    saver = InMemorySaver()
    graph = interrupt_graph().compile(checkpointer=saver)
    tcfg = {"configurable": {"thread_id": "t-meta"}}

    await graph.ainvoke({"log": []}, cfg("t-meta", "run-A"))
    latest = await saver.aget_tuple(tcfg)
    print("after run-A (interrupted): latest metadata =", latest.metadata)
    assert latest.metadata["run_id"] == "run-A"
    print(
        "run_id per checkpoint (newest first):",
        [
            (c.metadata["source"], c.metadata.get("run_id"))
            async for c in saver.alist(tcfg)
        ],
    )

    await graph.ainvoke(Command(resume="yes"), cfg("t-meta", "run-B"))
    latest = await saver.aget_tuple(tcfg)
    snap = await graph.aget_state(tcfg)
    print(
        "after run-B (resume): aget_tuple().metadata['run_id'] =",
        latest.metadata["run_id"],
        "| aget_state().metadata['run_id'] =",
        snap.metadata["run_id"],
    )
    assert latest.metadata["run_id"] == "run-B" == snap.metadata["run_id"]
    print(
        "run_id per checkpoint (newest first):",
        [
            (c.metadata["source"], c.metadata.get("run_id"))
            async for c in saver.alist(tcfg)
        ],
    )

    # only str/int/bool/float metadata values are copied (get_checkpoint_metadata)
    t2 = {"configurable": {"thread_id": "t-meta2"}}
    await graph.ainvoke(
        {"log": []}, {**t2, "metadata": {"run_id": "r", "obj": {"x": 1}, "n": 3}}
    )
    m = (await saver.aget_tuple(t2)).metadata
    print(
        "dict value dropped, int kept:", {k: m.get(k) for k in ("run_id", "obj", "n")}
    )
    assert "obj" not in m and m["n"] == 3

    # without metadata.run_id: langchain's top-level config["run_id"] does NOT land in metadata
    t3 = {"configurable": {"thread_id": "t-meta3"}}
    await graph.ainvoke(
        {"log": []}, {**t3, "run_id": "00000000-0000-0000-0000-000000000001"}
    )
    m = (await saver.aget_tuple(t3)).metadata
    print("top-level config['run_id'] only -> metadata:", m)


async def check_recovery_input() -> None:
    header(
        "recovery: same metadata.run_id + ORIGINAL input on a thread that carries it"
    )
    calls: list[str] = []
    gate = asyncio.Event()

    async def a(state: S) -> S:
        calls.append("a")
        return {"log": ["a"]}

    async def b(state: S) -> S:
        calls.append("b")
        if not gate.is_set():
            await asyncio.Event().wait()  # hang: simulated crash point
        return {"log": ["b"]}

    g = StateGraph(S)
    g.add_node("a", a)
    g.add_node("b", b)
    g.add_edge(START, "a")
    g.add_edge("a", "b")
    graph = g.compile(checkpointer=InMemorySaver())
    original = {"log": ["input"]}

    t = asyncio.create_task(
        graph.ainvoke(original, cfg("t-rec", "run-1"), durability="sync")
    )
    while calls != ["a", "b"]:
        await asyncio.sleep(0.01)
    t.cancel()  # orphaned run: the worker died while b was running
    try:
        await t
    except asyncio.CancelledError:
        pass
    snap = await graph.aget_state({"configurable": {"thread_id": "t-rec"}})
    print(
        "after cancel: values",
        snap.values,
        "next",
        snap.next,
        "run_id",
        snap.metadata["run_id"],
    )
    gate.set()

    out = await graph.ainvoke(original, cfg("t-rec", "run-1"))
    print("same run_id + original input ->", out, "calls:", calls)
    assert out == {"log": ["input", "a", "b"]}, "input must NOT be re-applied"

    out2 = await graph.ainvoke(original, cfg("t-rec", "run-2"))
    print("new run_id + original input  ->", out2)
    assert out2["log"] == ["input", "a", "b", "input", "a", "b"]

    # same run_id + original input on an INTERRUPTED thread: is it a resume?
    graph2 = interrupt_graph().compile(checkpointer=InMemorySaver())
    await graph2.ainvoke({"log": []}, cfg("t-rec-int", "run-1"))
    out3 = await graph2.ainvoke({"log": ["input"]}, cfg("t-rec-int", "run-1"))
    snap3 = await graph2.aget_state({"configurable": {"thread_id": "t-rec-int"}})
    print(
        "interrupted + same run_id + original input ->",
        out3,
        "next",
        snap3.next,
        "interrupts",
        [i.value for i in snap3.interrupts],
    )
    assert snap3.values == {"log": []} and snap3.next == ("ask",)


async def check_recovery_of_resume_run() -> None:
    header("recovery of a run whose input was Command(resume=...)")
    calls: list[str] = []
    gate = asyncio.Event()

    async def ask(state: S) -> S:
        calls.append("ask")
        return {"log": [f"ask:{interrupt('approve?')}"]}

    async def slow(state: S) -> S:
        calls.append("slow")
        if not gate.is_set():
            await asyncio.Event().wait()
        return {"log": ["slow"]}

    async def ask2(state: S) -> S:
        calls.append("ask2")
        return {"log": [f"ask2:{interrupt('second?')}"]}

    g = StateGraph(S)
    g.add_node("ask", ask)
    g.add_node("slow", slow)
    g.add_node("ask2", ask2)
    g.add_edge(START, "ask")
    g.add_edge("ask", "slow")
    g.add_edge("slow", "ask2")
    tcfg = {"configurable": {"thread_id": "t-rr"}}

    async def crash_resume() -> Any:
        calls.clear()
        gate.clear()
        graph = g.compile(checkpointer=InMemorySaver())
        await graph.ainvoke({"log": []}, cfg("t-rr", "run-A"))  # interrupts at ask
        t = asyncio.create_task(
            graph.ainvoke(Command(resume="yes"), cfg("t-rr", "run-B"))
        )
        while "slow" not in calls:
            await asyncio.sleep(0.01)
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass
        snap = await graph.aget_state(tcfg)
        print(
            "  after crash: values",
            snap.values,
            "next",
            snap.next,
            "latest run_id",
            snap.metadata["run_id"],
        )
        gate.set()
        return graph

    graph = await crash_resume()
    out = await graph.ainvoke(None, cfg("t-rr", "run-B"))
    snap = await graph.aget_state(tcfg)
    print(
        "  recovery with None:                ",
        out["log"],
        "next",
        snap.next,
        "calls:",
        calls,
    )
    assert out["log"] == ["ask:yes", "slow"] and snap.next == ("ask2",)

    graph = await crash_resume()
    out = await graph.ainvoke(Command(resume="yes"), cfg("t-rr", "run-B"))
    snap = await graph.aget_state(tcfg)
    print(
        "  recovery with Command(resume) again:",
        out["log"],
        "next",
        snap.next,
        "calls:",
        calls,
    )
    # the replayed resume value must not answer the later interrupt('second?')
    assert out["log"] == ["ask:yes", "slow"] and snap.next == ("ask2",)


# ---------------------------------------------------------------- v2 parts


async def check_v2_parts() -> None:
    header("version='v2' stream part shapes (subgraphs=True)")

    class MS(TypedDict):
        log: Annotated[list[str], operator.add]
        messages: Annotated[list[Any], operator.add]

    model = GenericFakeChatModel(messages=iter([AIMessage(content="hello world")]))

    async def llm(state: MS) -> dict[str, Any]:
        get_stream_writer()({"progress": 0.5})
        msg = await model.ainvoke("hi")
        return {"messages": [msg], "log": ["llm"]}

    async def ask(state: MS) -> dict[str, Any]:
        return {"log": [f"ask:{interrupt({'q': 'approve?'})}"]}

    inner = StateGraph(MS)
    inner.add_node("llm", llm)
    inner.add_edge(START, "llm")

    g = StateGraph(MS)
    g.add_node("agent", inner.compile())
    g.add_node("ask", ask)
    g.add_edge(START, "agent")
    g.add_edge("agent", "ask")
    graph = g.compile(checkpointer=InMemorySaver())
    modes = ["updates", "values", "messages", "custom"]
    seen: set[tuple[str, tuple[str, ...], bool]] = set()

    def show(p: dict[str, Any]) -> None:
        is_int = bool(p.get("interrupts")) or (
            p["type"] == "updates" and "__interrupt__" in p["data"]
        )
        key = (p["type"], p["ns"], is_int)
        if key in seen:
            return
        seen.add(key)
        data = p["data"]
        if p["type"] == "messages":
            msg, meta = data
            data = (
                f"{type(msg).__name__}(content={msg.content!r}, id={msg.id!r})",
                {k: meta[k] for k in sorted(meta)},
            )
        extra = f" interrupts={p['interrupts']!r}" if "interrupts" in p else ""
        print(
            f"  {type(p).__name__} keys={sorted(p)} type={p['type']!r} ns={p['ns']!r}{extra}"
            f"\n    data={data!r}"
        )

    async for p in graph.astream(
        {"log": [], "messages": []},
        cfg("t-v2", "run-1"),
        stream_mode=modes,
        subgraphs=True,
        version="v2",
    ):
        show(p)
    print("  -- resume --")
    seen.clear()
    async for p in graph.astream(
        Command(resume="ok"),
        cfg("t-v2", "run-2"),
        stream_mode=modes,
        subgraphs=True,
        version="v2",
    ):
        show(p)


async def main() -> None:
    print(
        "python",
        sys.version.split()[0],
        {
            p: md.version(p)
            for p in (
                "langgraph",
                "langgraph-checkpoint",
                "langchain-core",
                "langchain",
            )
        },
    )
    for durability in ("sync", "async", "exit"):
        await check_drain(durability)
    await check_drain_subgraph()
    await check_drain_last_node()
    await check_context()
    await check_metadata()
    await check_recovery_input()
    await check_recovery_of_resume_run()
    await check_v2_parts()
    print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
