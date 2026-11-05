"""Procrastinate app factory and the `RunQueue` implementation (design section 5, contract C4).

`queue` sits below `runtime`: it defers the worker task BY NAME and never imports `worker` or the
runtime. Task names live here so both sides agree on them.
"""

import logging
import uuid
from typing import Final

from procrastinate import App, PsycopgConnector
from procrastinate.exceptions import AlreadyEnqueued

from graph_agent._internal.config.settings import Settings
from graph_agent._internal.db.pool import MIN_POOL_SIZE

logger = logging.getLogger(__name__)

EXECUTE_RUN: Final = "graph_agent.execute_run"
# Recovery defers (a `running` run handed over or re-deferred) outrank normal ones: fetch skips a
# job while an earlier todo job of its lock has the same priority, even one waiting for a retry
# (S4), so a recovery job would sit behind every queued run of its thread.
RECOVERY_PRIORITY: Final = 1


def make_app(settings: Settings) -> App:
    """A Procrastinate app with its own pool (`database.procrastinate_pool_max`).

    The caller opens it (`async with app.open_async()`). Workers register their tasks on the
    same app (`worker.register_tasks`); the API only defers by name.
    """
    db = settings.database
    connector = PsycopgConnector(
        conninfo=db.url.get_secret_value(),
        min_size=MIN_POOL_SIZE,
        max_size=db.procrastinate_pool_max,
    )
    return App(connector=connector)


class ProcrastinateRunQueue:
    """`runtime.runner.RunQueue` over Procrastinate."""

    def __init__(self, app: App) -> None:
        self.app = app

    async def defer(
        self, run_id: uuid.UUID, thread_id: uuid.UUID, *, recovery: bool = False
    ) -> None:
        # lock: one job of a thread at a time, FIFO within one priority. queueing_lock: at
        # most one `todo` job per run; a job deferred while the run's job is `doing` is accepted
        # and exits at the claim.
        job = self.app.configure_task(
            name=EXECUTE_RUN,
            lock=str(thread_id),
            queueing_lock=str(run_id),
            priority=RECOVERY_PRIORITY if recovery else 0,
        )
        try:
            await job.defer_async(run_id=str(run_id))
        except AlreadyEnqueued:
            logger.debug("run %s already has a queued job", run_id)
