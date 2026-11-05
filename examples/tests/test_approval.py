import pytest

from examples.flows.approval import graph
from graph_agent.testing import flow_context, run_flow


@pytest.mark.parametrize(
    ("answer", "expected"),
    [(True, "done"), (False, "rejected"), ("no", "rejected"), ({"approved": False}, "rejected")],
)
async def test_pause_then_resume(answer: object, expected: str) -> None:
    ctx = flow_context()  # the same ctx (and its saver) must be reused to resume
    paused = await run_flow(graph, {"request": "deploy"}, ctx=ctx, thread_id="t")
    assert len(paused.interrupts) == 1
    assert paused.interrupts[0].value == "Approve: deploy?"
    assert "result" not in paused.values

    # A bare value resumes the single pending interrupt.
    resumed = await run_flow(graph, ctx=ctx, thread_id="t", resume=answer)
    assert resumed.values["result"] == expected
    assert not resumed.interrupts


async def test_resume_by_id() -> None:
    ctx = flow_context()
    paused = await run_flow(graph, {"request": "deploy"}, ctx=ctx, thread_id="t")
    resumed = await run_flow(graph, ctx=ctx, thread_id="t", resume={paused.interrupts[0].id: True})
    assert resumed.values["result"] == "done"
