"""`RunnerResources` for integration tests that run the runner/worker without a container."""

from dataclasses import dataclass, field

from ranvier._internal.bootstrap import ShutdownBudget
from ranvier._internal.config.settings import Settings
from ranvier._internal.observability.tracing import LoggingMetrics, Observability
from ranvier._internal.runtime.checkpointer import Checkpointer
from ranvier._internal.runtime.events import RunObserver
from ranvier._internal.runtime.flows import FlowRegistry
from ranvier._internal.runtime.runner import ActiveRuns, RunQueue
from ranvier._internal.runtime.runs import RunsRepository

GRACE_S = 10.0


@dataclass
class Resources:
    settings: Settings
    runs: RunsRepository | None
    flows: FlowRegistry
    checkpointer: Checkpointer
    queue: RunQueue | None = None
    observers: list[RunObserver] = field(default_factory=list)
    active_runs: ActiveRuns = field(default_factory=ActiveRuns)
    shutdown_budget: ShutdownBudget = field(default_factory=lambda: ShutdownBudget(GRACE_S))
    observability: Observability = field(
        default_factory=lambda: Observability(metrics=LoggingMetrics())
    )
