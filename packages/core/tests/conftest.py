from collections.abc import Iterator

import langgraph.graph  # noqa: F401  # first import does blocking I/O; keep it out of blockbuster
import pytest
from blockbuster import blockbuster_ctx


@pytest.fixture(autouse=True)
def _blockbuster(request: pytest.FixtureRequest) -> Iterator[None]:
    """Fail async tests that make blocking calls inside the event loop."""
    # asyncio auto mode marks every coroutine test with `asyncio` at collection time.
    if request.node.get_closest_marker("asyncio") is None:
        yield
        return
    with blockbuster_ctx():
        yield
