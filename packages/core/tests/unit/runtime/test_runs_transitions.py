import itertools

import pytest

from graph_agent._internal.runtime.runs import (
    ErrorCode,
    RunError,
    RunStatus,
    can_transition,
)

S = RunStatus
ALLOWED = {
    (S.PENDING, S.RUNNING),
    (S.PENDING, S.CANCELLED),
    (S.RUNNING, S.SUCCESS),
    (S.RUNNING, S.ERROR),
    (S.RUNNING, S.INTERRUPTED),
    (S.RUNNING, S.CANCELLED),
}


@pytest.mark.parametrize(("source", "target"), list(itertools.product(RunStatus, RunStatus)))
def test_transition_table(source: RunStatus, target: RunStatus) -> None:
    assert can_transition(source, target) is ((source, target) in ALLOWED)


def test_run_error_roundtrip() -> None:
    error = RunError(ErrorCode.THREAD_BUSY, "busy", retryable=True)
    again = RunError.from_dict(error.as_dict())
    assert (again.code, again.message, again.retryable) == (ErrorCode.THREAD_BUSY, "busy", True)
