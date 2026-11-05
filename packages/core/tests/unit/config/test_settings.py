from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from graph_agent._internal.config.settings import (
    DEFAULT_API_PORT,
    DEFAULT_CHECKPOINTER_POOL_MAX,
    Settings,
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for var in ("GRAPH_AGENT_ENV_FILE", "GRAPH_AGENT_CONFIG", "GRAPH_AGENT_API__PORT"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.chdir(tmp_path)  # no stray graph-agent.toml / .env from the cwd


def make(**init: Any) -> Settings:
    """Settings isolated from the repo .env; `init` goes through the init-args layer."""
    return Settings(_env_file=None, **init)  # type: ignore[call-arg]  # pydantic-settings arg


def _toml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, body: str) -> None:
    path = tmp_path / "cfg.toml"
    path.write_text(body)
    monkeypatch.setenv("GRAPH_AGENT_CONFIG", str(path))


def _dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, body: str) -> None:
    path = tmp_path / "custom.env"
    path.write_text(body)
    monkeypatch.setenv("GRAPH_AGENT_ENV_FILE", str(path))


def test_defaults() -> None:
    s = make()
    assert s.api.port == DEFAULT_API_PORT
    assert s.database.checkpointer_pool_max == DEFAULT_CHECKPOINTER_POOL_MAX
    assert s.limits.max_active_runs == 32


def test_toml_beats_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _toml(tmp_path, monkeypatch, "[api]\nport = 1111\n")
    assert make().api.port == 1111


def test_dotenv_beats_toml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _toml(tmp_path, monkeypatch, "[api]\nport = 1111\n")
    _dotenv(tmp_path, monkeypatch, "GRAPH_AGENT_API__PORT=2222\n")
    assert Settings().api.port == 2222


def test_env_beats_dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _dotenv(tmp_path, monkeypatch, "GRAPH_AGENT_API__PORT=2222\n")
    monkeypatch.setenv("GRAPH_AGENT_API__PORT", "3333")
    assert Settings().api.port == 3333


def test_init_beats_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAPH_AGENT_API__PORT", "3333")
    assert make(api={"port": 4444}).api.port == 4444


def test_explicit_missing_config_is_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAPH_AGENT_CONFIG", str(tmp_path / "nope.toml"))
    with pytest.raises(FileNotFoundError):
        make()


def test_missing_default_config_is_fine() -> None:
    assert make().api.port == DEFAULT_API_PORT


def test_unknown_top_level_toml_key_tolerated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _toml(tmp_path, monkeypatch, "[flows.x]\ntarget = 'a:b'\n")
    make()


def test_unknown_group_key_names_the_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _toml(tmp_path, monkeypatch, "[limits]\nmax_active_runz = 1\n")
    with pytest.raises(ValidationError, match="max_active_runz"):
        make()


def test_secrets_not_in_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAPH_AGENT_DATABASE__URL", "postgresql://u:hunter2@h/db")
    s = make()
    assert "hunter2" not in repr(s)
    assert s.database.url.get_secret_value().endswith("@h/db")


def test_api_auth_modes() -> None:
    with pytest.raises(ValidationError, match=r"api\.keys"):
        make(api={"auth": "api_keys"})
    with pytest.raises(ValidationError):
        make(api={"auth": "not valid"})
    assert make(api={"auth": "my.mod:check"}).api.auth == "my.mod:check"
    ok = make(api={"auth": "api_keys", "keys": {"bot": {"env": "BOT_KEY", "tenant_id": "t1"}}})
    assert ok.api.keys["bot"].env == "BOT_KEY"


def test_lease_must_outlive_heartbeat() -> None:
    with pytest.raises(ValidationError, match="lease_s"):
        make(runs={"heartbeat_s": 10, "lease_s": 5})
