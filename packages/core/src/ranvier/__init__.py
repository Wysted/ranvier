from importlib.metadata import version
from typing import TYPE_CHECKING, Any

from ranvier._internal.agents.factory import as_tool
from ranvier._internal.agents.spec import AgentSpec
from ranvier._internal.runtime.context import FlowContext, RunContext

if TYPE_CHECKING:
    from ranvier._internal.api.auth import AuthError, Principal

__version__ = version("ranvier")

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
        from ranvier._internal.api import auth

        return getattr(auth, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
