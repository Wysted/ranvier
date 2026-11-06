from examples.flows.hello import graph
from ranvier.testing import run_flow


async def test_hello() -> None:
    result = await run_flow(graph, {"name": "Ada"})
    assert result.values["greeting"] == "Hello, Ada!"
    assert not result.interrupts
