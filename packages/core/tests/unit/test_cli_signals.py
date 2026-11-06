import asyncio
import os
import signal

import pytest

from ranvier._internal.cli import _until_signalled

TERM = signal.SIGTERM


async def test_first_signal_cancels_and_the_block_returns_normally(
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls: list[str] = []
    task: asyncio.Task[None] | None = None

    def cancel() -> None:
        calls.append("cancel")
        assert task is not None
        task.cancel()

    async def body() -> None:
        async with _until_signalled(cancel):
            os.kill(os.getpid(), TERM)
            os.kill(os.getpid(), TERM)  # second one: ignored
            await asyncio.sleep(10)

    task = asyncio.create_task(body())
    await asyncio.wait_for(task, 5)  # returns normally: the cancellation was ours
    assert calls == ["cancel"]
    assert "SIGTERM ignored" in caplog.text
    assert signal.getsignal(TERM) == signal.SIG_DFL  # handlers removed on exit


async def test_signals_during_the_cleanup_are_still_handled() -> None:
    async def body() -> None:
        async with _until_signalled(lambda: None):
            os.kill(os.getpid(), TERM)
            await asyncio.sleep(0.01)
            os.kill(os.getpid(), TERM)  # no KeyboardInterrupt / SIG_DFL kill
            await asyncio.sleep(0.01)

    await asyncio.wait_for(body(), 5)


async def test_external_cancellation_propagates() -> None:
    async def body() -> None:
        async with _until_signalled(lambda: None):
            await asyncio.sleep(10)

    task = asyncio.create_task(body())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
