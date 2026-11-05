from importlib.metadata import version

from graph_agent._internal.runtime.context import FlowContext, RunContext

__version__ = version("graph-agent")

__all__ = ["FlowContext", "RunContext", "__version__"]
