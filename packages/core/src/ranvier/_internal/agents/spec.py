"""`AgentSpec`: declarative agent definition, re-exported by the facade.

langchain types stay under TYPE_CHECKING so importing the facade does not load langchain.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from langchain.agents.middleware import AgentMiddleware
    from langchain_core.tools import BaseTool


@dataclass(frozen=True, slots=True)
class AgentSpec:
    name: str  # also the usage-ledger attribution key
    profile: str  # LLM profile name
    system_prompt: str | None = None
    tools: Sequence[str | BaseTool] = ()  # registry names or tool objects
    mcp_servers: Sequence[str] = ()  # names from [mcp.servers]
    skills: Sequence[Path] = ()  # directories holding SKILL.md folders
    middleware: Sequence[AgentMiddleware] = ()
    response_format: type | None = None
