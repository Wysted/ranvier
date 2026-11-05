from importlib.metadata import version
from typing import TYPE_CHECKING, Any

from graph_agent._internal.agents.factory import as_tool
from graph_agent._internal.agents.spec import AgentSpec
from graph_agent._internal.runtime.context import FlowContext, RunContext

if TYPE_CHECKING:
    from graph_agent._internal.api.auth import AuthError, Principal

__version__ = version("graph-agent")

__all__ = [
    "AgentSpec",
    "AuthError",
    "FlowContext",
    "Principal",
    "RunContext",
    "__version__",
    "as_tool",
]


def __getattr__(name: str) -> Any:
    # Lazy: the auth module pulls fastapi and the settings stack, which most imports never need.
    if name in ("AuthError", "Principal"):
        from graph_agent._internal.api import auth

        return getattr(auth, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
