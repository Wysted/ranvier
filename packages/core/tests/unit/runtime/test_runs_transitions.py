from ranvier._internal.runtime.runs import ErrorCode, RunError


def test_run_error_roundtrip() -> None:
    error = RunError(ErrorCode.THREAD_BUSY, "busy", retryable=True)
    again = RunError.from_dict(error.as_dict())
    assert (again.code, again.message, again.retryable) == (ErrorCode.THREAD_BUSY, "busy", True)
