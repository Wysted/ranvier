"""`RunnerResources` for integration tests that run the runner/worker without a container."""

from dataclasses import dataclass, field

from graph_agent._internal.bootstrap import ShutdownBudget
from graph_agent._internal.config.settings import Settings
from graph_agent._internal.observability.tracing import LoggingMetrics, Observability
from graph_agent._internal.runtime.checkpointer import Checkpointer
from graph_agent._internal.runtime.events import RunObserver
from graph_agent._internal.runtime.flows import FlowRegistry
from graph_agent._internal.runtime.runner import ActiveRuns, RunQueue
from graph_agent._internal.runtime.runs import RunsRepository

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
