import sys
import types
from collections.abc import Iterator

import pytest

from unit.runtime._runner_doubles import (
    CHAIN,
    MODULE,
    _after,
    _ask,
    _boom,
    _builder,
    _drain_first,
    _fanout,
    _slow,
    _step,
    _work,
    cancel_seen,
    drain_targets,
    reached,
)


@pytest.fixture
def user_module() -> Iterator[types.ModuleType]:
    mod = types.ModuleType(MODULE)
    mod.work = _builder(_work)  # type: ignore[attr-defined]
    mod.ask = _builder(_ask)  # type: ignore[attr-defined]
    mod.slow = _builder(_slow)  # type: ignore[attr-defined]
    mod.boom = _builder(_boom)  # type: ignore[attr-defined]
    mod.drain = _builder(_drain_first, _after)  # type: ignore[attr-defined]
    mod.chain = _builder(*(_step(i) for i in range(CHAIN)))  # type: ignore[attr-defined]
    mod.fanout = _fanout()  # type: ignore[attr-defined]
    sys.modules[MODULE] = mod
    yield mod
    del sys.modules[MODULE]
    drain_targets.clear()
    reached.clear()
    cancel_seen.clear()
