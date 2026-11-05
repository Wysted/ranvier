"""Procrastinate worker side: the `execute_run` task (design section 5).

Worker processes build their own container; nothing is shared with the API process. T25 adds the
sweep, prune and cron tasks on the same app.
"""

import logging
import uuid
from contextlib import aclosing
from typing import Final

from procrastinate import App, RetryStrategy

from graph_agent._internal.queue import EXECUTE_RUN, ProcrastinateRunQueue
from graph_agent._internal.runtime.runner import RunnerResources, execute_run
from graph_agent._internal.runtime.runs import ErrorCode, RunError

logger = logging.getLogger(__name__)

# `max_attempts=N` means N+1 executions and stalled requeues spend the same counter (S4). A job
# waiting for its retry blocks later jobs of the same thread, so the wait stays short.
THREAD_BUSY_WAIT_S: Final = 2
THREAD_BUSY_MAX_ATTEMPTS: Final = 30
# Finishing a job sends no notification, so the next job of the same lock is found by polling.
FETCH_POLL_INTERVAL_S: Final = 1.0


class ThreadBusy(Exception):
    """Another run of the thread is running: Procrastinate retries the job with backoff."""


def register_tasks(container: RunnerResources) -> App:
    """Register the worker tasks on the container's queue app and return it."""
    queue = container.queue
    if not isinstance(queue, ProcrastinateRunQueue):
        raise RuntimeError("the worker needs the Procrastinate queue in the container")

    @queue.app.task(
        name=EXECUTE_RUN,
        retry=RetryStrategy(
            max_attempts=THREAD_BUSY_MAX_ATTEMPTS,
            wait=THREAD_BUSY_WAIT_S,
            retry_exceptions={ThreadBusy},
        ),
    )
    async def execute_run_job(run_id: str) -> None:
        # The run row is the run status, never the job status: a sweep can close a job this one
        # still runs, so Procrastinate may log "not in doing or todo status" on finish. Expected.
        try:
            async with aclosing(execute_run(container, uuid.UUID(run_id))) as events:
                async for _ in events:  # no client: just drain
                    pass
        except RunError as exc:
            if exc.code is ErrorCode.THREAD_BUSY:
                raise ThreadBusy(str(exc)) from exc
            raise

    return queue.app


async def run_worker(container: RunnerResources) -> None:
    """Serve jobs until cancelled, `limits.max_active_runs` at a time (heartbeat defaults)."""
    app = register_tasks(container)
    await app.run_worker_async(
        concurrency=container.settings.limits.max_active_runs,
        wait=True,
        fetch_job_polling_interval=FETCH_POLL_INTERVAL_S,
    )
