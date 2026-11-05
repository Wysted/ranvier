"""Root `Settings`: one pydantic model, precedence init > env > .env > toml > defaults."""

import os
from pathlib import Path
from typing import Annotated, Any, Self

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import (
    BaseSettings,
    DotEnvSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

from graph_agent._internal.config.manifest import FlowSpec, LlmSettings, McpSettings, Name, ToolSpec

ENV_PREFIX = "GRAPH_AGENT_"
ENV_NESTED_DELIMITER = "__"
ENV_FILE_VAR = "GRAPH_AGENT_ENV_FILE"
CONFIG_FILE_VAR = "GRAPH_AGENT_CONFIG"
DEFAULT_ENV_FILE = ".env"
DEFAULT_CONFIG_FILE = "graph-agent.toml"

AUTH_NONE = "none"
AUTH_API_KEYS = "api_keys"
# `none`, `api_keys` or a `module:callable` hook returning a Principal (OD-2).
AUTH_PATTERN = rf"^({AUTH_NONE}|{AUTH_API_KEYS}|[A-Za-z_][\w.]*:[A-Za-z_]\w*)$"
MAX_PORT = 65535

# Defaults from design sections 5 and 8.
DEFAULT_DATABASE_URL = "postgresql://graph_agent:graph_agent@127.0.0.1:54329/graph_agent"  # compose
DEFAULT_APP_POOL_MAX = 5
DEFAULT_CHECKPOINTER_POOL_MAX = 10
DEFAULT_PROCRASTINATE_POOL_MAX = 5
DEFAULT_MAX_ACTIVE_RUNS = 32
DEFAULT_ADMISSION_WAIT_S = 5
DEFAULT_SYNC_THREADS = 16
DEFAULT_CPU_WORKERS = 2
DEFAULT_TOOL_TIMEOUT_S = 30
DEFAULT_SHUTDOWN_GRACE_S = 60
DEFAULT_LOOP_LAG_WARN_MS = 100

# Values the design left open (R7); each justified in one line.
DEFAULT_API_HOST = "127.0.0.1"  # auth defaults to none, so bind to localhost only
DEFAULT_API_PORT = 8000  # uvicorn/FastAPI convention
DEFAULT_LOOP_LAG_READY_MS = 1000  # 10x the warn threshold: a loop this late is not serving traffic
DEFAULT_HEARTBEAT_S = 15  # cancel_requested is noticed within this delay; cheap one-row update
LEASE_MIN_HEARTBEATS = 2  # lease must span at least this many heartbeats
DEFAULT_LEASE_S = 60  # 4 heartbeats: tolerates missed beats before the orphan sweep reclaims
DEFAULT_MAX_ATTEMPTS = 3  # first try plus two recoveries before orphan_retries_exhausted
DEFAULT_THREAD_TTL_DAYS = 30  # idle threads get keep_latest pruning after a month
DEFAULT_THREAD_DELETE_AFTER_DAYS = 180  # second TTL: whole thread deleted after six months idle

_STRICT = ConfigDict(extra="forbid")  # unknown keys fail naming the key


class DatabaseSettings(BaseModel):
    model_config = _STRICT

    url: SecretStr = SecretStr(DEFAULT_DATABASE_URL)
    app_pool_max: int = Field(default=DEFAULT_APP_POOL_MAX, gt=0)
    checkpointer_pool_max: int = Field(default=DEFAULT_CHECKPOINTER_POOL_MAX, gt=0)
    procrastinate_pool_max: int = Field(default=DEFAULT_PROCRASTINATE_POOL_MAX, gt=0)


class ApiKeyRef(BaseModel):
    """A static API key, by the NAME of the env var that holds it (secrets are env only)."""

    model_config = _STRICT

    key_env: str
    tenant_id: str | None = None


class ApiSettings(BaseModel):
    model_config = _STRICT

    host: str = DEFAULT_API_HOST
    port: int = Field(default=DEFAULT_API_PORT, gt=0, le=MAX_PORT)
    auth: Annotated[str, Field(pattern=AUTH_PATTERN)] = AUTH_NONE
    keys: dict[str, ApiKeyRef] = {}  # by subject

    @model_validator(mode="after")
    def _keys_match_auth(self) -> Self:
        if self.auth == AUTH_API_KEYS and not self.keys:
            raise ValueError("api.auth = 'api_keys' needs at least one entry in api.keys")
        return self


class LimitsSettings(BaseModel):
    model_config = _STRICT

    max_active_runs: int = Field(default=DEFAULT_MAX_ACTIVE_RUNS, gt=0)
    admission_wait_s: float = Field(default=DEFAULT_ADMISSION_WAIT_S, ge=0)
    sync_threads: int = Field(default=DEFAULT_SYNC_THREADS, gt=0)
    cpu_workers: int = Field(default=DEFAULT_CPU_WORKERS, gt=0)
    tool_timeout_s: float = Field(default=DEFAULT_TOOL_TIMEOUT_S, gt=0)
    shutdown_grace_s: float = Field(default=DEFAULT_SHUTDOWN_GRACE_S, ge=0)
    loop_lag_warn_ms: float = Field(default=DEFAULT_LOOP_LAG_WARN_MS, gt=0)
    loop_lag_ready_ms: float = Field(default=DEFAULT_LOOP_LAG_READY_MS, gt=0)


class RunsSettings(BaseModel):
    model_config = _STRICT

    heartbeat_s: float = Field(default=DEFAULT_HEARTBEAT_S, gt=0)
    lease_s: float = Field(default=DEFAULT_LEASE_S, gt=0)
    max_attempts: int = Field(default=DEFAULT_MAX_ATTEMPTS, ge=1)

    @model_validator(mode="after")
    def _lease_outlives_heartbeat(self) -> Self:
        if self.lease_s < LEASE_MIN_HEARTBEATS * self.heartbeat_s:
            raise ValueError(
                f"runs.lease_s must be at least {LEASE_MIN_HEARTBEATS} x runs.heartbeat_s"
            )
        return self


class ThreadsSettings(BaseModel):
    model_config = _STRICT

    ttl_days: int = Field(default=DEFAULT_THREAD_TTL_DAYS, gt=0)
    delete_after_days: int = Field(default=DEFAULT_THREAD_DELETE_AFTER_DAYS, gt=0)

    @model_validator(mode="after")
    def _delete_after_prune(self) -> Self:
        if self.delete_after_days <= self.ttl_days:
            raise ValueError("threads.delete_after_days must be greater than threads.ttl_days")
        return self


class ObservabilitySettings(BaseModel):
    model_config = _STRICT

    otel_enabled: bool = False
    otel_endpoint: str | None = None  # None: SDK default / OTEL_* env vars


class Settings(BaseSettings):
    # Unknown top-level keys fail naming the key (typo'd section in the toml).
    model_config = SettingsConfigDict(
        env_prefix=ENV_PREFIX,
        env_nested_delimiter=ENV_NESTED_DELIMITER,
        env_file=DEFAULT_ENV_FILE,
        extra="forbid",
    )

    database: DatabaseSettings = DatabaseSettings()
    api: ApiSettings = ApiSettings()
    limits: LimitsSettings = LimitsSettings()
    runs: RunsSettings = RunsSettings()
    threads: ThreadsSettings = ThreadsSettings()
    observability: ObservabilitySettings = ObservabilitySettings()
    flows: dict[Name, FlowSpec] = {}
    tools: dict[Name, ToolSpec] = {}
    llm: LlmSettings = LlmSettings()
    mcp: McpSettings = McpSettings()

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Bootstrap vars (GRAPH_AGENT_CONFIG, GRAPH_AGENT_ENV_FILE) are read from the process
        # environment only, never from the .env file (they locate it).
        env_file = os.environ.get(ENV_FILE_VAR)
        # Swap only while the source still carries the class default: `_env_file=None` wins.
        if isinstance(dotenv_settings, DotEnvSettingsSource):
            if env_file and dotenv_settings.env_file == DEFAULT_ENV_FILE:
                dotenv_settings = DotEnvSettingsSource(settings_cls, env_file=env_file)
            dotenv_settings = _FieldsOnlyDotEnv(settings_cls, env_file=dotenv_settings.env_file)
        return (init_settings, env_settings, dotenv_settings, _toml_source(settings_cls))


class _FieldsOnlyDotEnv(DotEnvSettingsSource):
    """With `extra="forbid"` pydantic-settings turns every unrelated .env line (OPENAI_API_KEY,
    GRAPH_AGENT_CONFIG, ...) into an extra key and rejects it; keep only real fields."""

    def __call__(self) -> dict[str, Any]:
        return {k: v for k, v in super().__call__().items() if k in self.settings_cls.model_fields}


def _toml_source(settings_cls: type[BaseSettings]) -> TomlConfigSettingsSource:
    explicit = os.environ.get(CONFIG_FILE_VAR)
    path = Path(explicit or DEFAULT_CONFIG_FILE)
    if explicit and not path.is_file():
        raise FileNotFoundError(f"{CONFIG_FILE_VAR} points to a missing file: {path}")
    return TomlConfigSettingsSource(settings_cls, toml_file=path)
