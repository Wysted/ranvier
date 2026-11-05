"""Manifest sections of `graph-agent.toml`: flows, crons, tools, MCP servers and LLM profiles."""

import importlib
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, Self
from urllib.parse import urlsplit

from croniter import CroniterBadDateError, croniter
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)
from pydantic_core import PydanticCustomError

NAME_PATTERN = r"^[a-z][a-z0-9_]{0,62}$"
TARGET_PATTERN = r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*)*:[A-Za-z_]\w*$"
ENV_NAME_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*$"
OWN_ENV_PREFIX = "GRAPH_AGENT_"
CRON_FIELDS = 5
LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

DEFAULT_FLOW_TIMEOUT_S = 600  # design §7 example
DEFAULT_TOOL_MAX_CONCURRENCY = 4  # the per-tool semaphore needs a bound
DEFAULT_MCP_MAX_CONCURRENCY = 4  # design §7 example
DEFAULT_MCP_TIMEOUT_S = 30  # design §7 example
DEFAULT_PROVIDER_MAX_CONNECTIONS = 16  # design §7 example
DEFAULT_PROFILE_TIMEOUT_S = 60  # design §7 example
DEFAULT_PROFILE_MAX_RETRIES = 2  # design §7 example

Name = Annotated[str, StringConstraints(pattern=NAME_PATTERN)]
Target = Annotated[str, StringConstraints(pattern=TARGET_PATTERN)]


def _not_own_secret(name: str) -> str:
    # Never hand the app's own secrets to a provider or MCP server.
    if name.startswith(OWN_ENV_PREFIX):
        raise ValueError(f"env var names starting with {OWN_ENV_PREFIX} are reserved")
    return name


EnvName = Annotated[
    str, StringConstraints(pattern=ENV_NAME_PATTERN), AfterValidator(_not_own_secret)
]
Provider = Literal["openai", "anthropic", "ollama"]
KEYED_PROVIDERS: frozenset[str] = frozenset({"openai", "anthropic"})

# Unknown keys fail naming the key; input is hidden so URL credentials never reach error text.
_STRICT = ConfigDict(extra="forbid", hide_input_in_errors=True)


class TargetImportError(Exception):
    """A `module:object` target could not be imported."""


def import_target(target: str) -> object:
    """Import `module:object` lazily; never called while validating models."""
    module_name, sep, attr = target.partition(":")
    if not (module_name and sep and attr):
        raise TargetImportError(f"target {target!r} must look like 'module:object'")
    try:
        module = importlib.import_module(module_name)
        return getattr(module, attr)
    except AttributeError as exc:
        raise TargetImportError(f"module {module_name!r} has no attribute {attr!r}") from exc
    except Exception as exc:  # user code runs at import: any failure is a target error
        raise TargetImportError(f"cannot import {module_name!r}:{attr!r}: {exc!r}") from exc


def _check_cron(schedule: str) -> str:
    fields = schedule.split()
    if len(fields) != CRON_FIELDS or not croniter.is_valid(schedule):
        raise ValueError(f"not a valid 5-field cron expression: {schedule!r}")
    # Random/hash items are resolved per instance, so the schedule would not be deterministic.
    if any(item[:1] in ("R", "H") for f in fields for item in f.split(",")):
        raise ValueError(f"random/hash cron syntax is not supported: {schedule!r}")
    try:
        croniter(schedule).get_next(float)  # e.g. "0 0 30 2 *" parses but never fires
    except CroniterBadDateError as exc:
        raise ValueError(f"cron expression never fires: {schedule!r}") from exc
    return schedule


def next_fire(schedule: str, after: datetime) -> datetime:
    """Next UTC firing of `schedule` strictly after `after` (same lib as Procrastinate)."""
    if after.tzinfo is None:
        raise ValueError("`after` must be timezone-aware")
    fire: datetime = croniter(_check_cron(schedule), after.astimezone(UTC)).get_next(datetime)
    return fire


def _check_url(value: str) -> str:
    parts = urlsplit(value)
    if parts.username is not None or parts.password is not None:
        raise PydanticCustomError("url_userinfo", "URL must not embed credentials; use headers_env")
    if parts.scheme == "https" and parts.hostname:
        return value
    if parts.scheme == "http" and parts.hostname in LOCAL_HOSTS:
        return value
    raise ValueError("must be an https URL (http only for localhost)")


class CronSpec(BaseModel):
    model_config = _STRICT

    schedule: str  # UTC only; each firing starts a new thread
    input: dict[str, Any] = {}

    @field_validator("schedule")
    @classmethod
    def _valid_schedule(cls, value: str) -> str:
        return _check_cron(value)


class FlowSpec(BaseModel):
    model_config = _STRICT

    target: Target
    version: int = Field(default=1, ge=1)
    description: str = ""
    durability: Literal["sync", "async", "exit"] = "sync"
    timeout_s: float = Field(default=DEFAULT_FLOW_TIMEOUT_S, gt=0)
    max_concurrency: int | None = Field(default=None, gt=0)  # None: LangGraph default
    config: dict[str, Any] = {}
    crons: list[CronSpec] = []


class ToolSpec(BaseModel):
    model_config = _STRICT

    target: Target
    timeout_s: float | None = Field(default=None, gt=0)  # None: limits.tool_timeout_s
    max_concurrency: int = Field(default=DEFAULT_TOOL_MAX_CONCURRENCY, gt=0)
    isolation: Literal["async", "thread", "process"] = "async"


class McpServerSpec(BaseModel):
    model_config = _STRICT

    url: str
    headers_env: dict[str, EnvName] = {}  # header name -> env var NAME holding the value
    max_concurrency: int = Field(default=DEFAULT_MCP_MAX_CONCURRENCY, gt=0)
    timeout_s: float = Field(default=DEFAULT_MCP_TIMEOUT_S, gt=0)

    @field_validator("url")
    @classmethod
    def _https_url(cls, value: str) -> str:
        return _check_url(value)


class ProviderSettings(BaseModel):
    model_config = _STRICT

    max_connections: int = Field(default=DEFAULT_PROVIDER_MAX_CONNECTIONS, gt=0)


class LlmProfile(BaseModel):
    model_config = _STRICT

    provider: Provider
    model: str = Field(min_length=1)
    api_key_env: EnvName | None = None  # the variable NAME, never the key
    base_url: str | None = None
    temperature: float | None = Field(default=None, ge=0, le=2)
    timeout_s: float = Field(default=DEFAULT_PROFILE_TIMEOUT_S, gt=0)
    max_retries: int = Field(default=DEFAULT_PROFILE_MAX_RETRIES, ge=0)

    @field_validator("base_url")
    @classmethod
    def _https_base_url(cls, value: str | None) -> str | None:
        return None if value is None else _check_url(value)

    @model_validator(mode="after")
    def _provider_rules(self) -> Self:
        if self.provider in KEYED_PROVIDERS and self.api_key_env is None:
            raise ValueError(f"api_key_env is required for provider {self.provider!r}")
        # Ollama has no retries (S3); an explicit value must not be silently dropped.
        if self.provider == "ollama" and "max_retries" in self.model_fields_set:
            raise ValueError("max_retries is not supported by provider 'ollama'")
        if self.provider == "ollama" and self.api_key_env is not None:
            raise ValueError("api_key_env is not supported by provider 'ollama'")
        return self


class LlmSettings(BaseModel):
    model_config = _STRICT

    providers: dict[Provider, ProviderSettings] = {}
    profiles: dict[Name, LlmProfile] = {}


class McpSettings(BaseModel):
    model_config = _STRICT

    servers: dict[Name, McpServerSpec] = {}
