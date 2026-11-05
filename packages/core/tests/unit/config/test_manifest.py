import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from graph_agent._internal.config.manifest import (
    TargetImportError,
    import_target,
    next_fire,
)
from graph_agent._internal.config.settings import Settings


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for var in [v for v in os.environ if v.startswith("GRAPH_AGENT_")]:
        monkeypatch.delenv(var)
    monkeypatch.chdir(tmp_path)


def make(**init: Any) -> Settings:
    return Settings(_env_file=None, **init)


FLOW = {"target": "pkg.mod:build"}


def test_full_manifest_loads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "cfg.toml"
    path.write_text(
        """
[flows.summarize]
target = "pkg.mod:build"
version = 2
durability = "async"
config = { language = "es" }
[[flows.summarize.crons]]
schedule = "0 3 * * *"
input = { source = "daily" }

[tools.web_search]
target = "pkg.tools:web_search"
isolation = "process"

[mcp.servers.github]
url = "https://mcp.internal/github"
headers_env = { Authorization = "GITHUB_MCP_TOKEN" }

[llm.providers.openai]
max_connections = 8
[llm.profiles.fast]
provider = "openai"
model = "gpt-5.4-mini"
api_key_env = "OPENAI_API_KEY"
"""
    )
    monkeypatch.setenv("GRAPH_AGENT_CONFIG", str(path))
    s = make()
    assert s.flows["summarize"].crons[0].input == {"source": "daily"}
    assert s.flows["summarize"].timeout_s == 600
    assert s.tools["web_search"].isolation == "process"
    assert s.llm.providers["openai"].max_connections == 8
    assert s.llm.profiles["fast"].max_retries == 2
    assert s.mcp.servers["github"].timeout_s == 30


@pytest.mark.parametrize("name", ["Bad", "1x", "has-dash", "", "a" * 64])
def test_bad_names_rejected(name: str) -> None:
    for section in (
        {"flows": {name: FLOW}},
        {"tools": {name: FLOW}},
        {"mcp": {"servers": {name: {"url": "https://x.io"}}}},
        {"llm": {"profiles": {name: {"provider": "ollama", "model": "m"}}}},
    ):
        with pytest.raises(ValidationError):
            make(**section)


@pytest.mark.parametrize("target", ["no_colon", "a/b.py:f", "mod:", ":obj", "a.b:c.d", "a b:c"])
def test_bad_target_names_the_key(target: str) -> None:
    with pytest.raises(ValidationError, match=r"flows\.summarize\.target"):
        make(flows={"summarize": {"target": target}})


@pytest.mark.parametrize(
    "bad",
    [
        {"version": 0},
        {"durability": "never"},
        {"timeout_s": 0},
        {"max_concurrency": 0},
        {"unknown": 1},
    ],
)
def test_bad_flow_fields_rejected(bad: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        make(flows={"f": {**FLOW, **bad}})


def test_bad_tool_isolation_rejected() -> None:
    with pytest.raises(ValidationError, match=r"tools\.t\.isolation"):
        make(tools={"t": {**FLOW, "isolation": "fork"}})


@pytest.mark.parametrize("schedule", ["* * * *", "* * * * * *", "61 * * * *", "nope", "@daily"])
def test_bad_cron_rejected(schedule: str) -> None:
    with pytest.raises(ValidationError, match="cron"):
        make(flows={"f": {**FLOW, "crons": [{"schedule": schedule}]}})


def test_next_fire_is_utc_and_strictly_after() -> None:
    after = datetime(2026, 1, 1, 3, 0, tzinfo=UTC)
    assert next_fire("0 3 * * *", after) == datetime(2026, 1, 2, 3, 0, tzinfo=UTC)
    with pytest.raises(ValueError, match="timezone"):
        next_fire("0 3 * * *", datetime(2026, 1, 1))


@pytest.mark.parametrize(
    "profile",
    [
        {"provider": "openai", "model": "m"},  # keyed without api_key_env
        {"provider": "anthropic", "model": "m"},
        {"provider": "ollama", "model": "m", "base_url": "http://remote:11434"},
        {"provider": "ollama", "model": "m", "max_retries": 1},
        {"provider": "ollama", "model": "m", "api_key_env": "not a name"},
        {"provider": "other", "model": "m"},
        {"provider": "ollama", "model": ""},
    ],
)
def test_bad_profile_rejected(profile: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        make(llm={"profiles": {"p": profile}})


def test_profile_base_url_localhost_http_ok() -> None:
    s = make(
        llm={
            "profiles": {
                "p": {"provider": "ollama", "model": "m", "base_url": "http://localhost:11434"}
            }
        }
    )
    assert s.llm.profiles["p"].base_url == "http://localhost:11434"


def test_mcp_rules() -> None:
    with pytest.raises(ValidationError, match="https"):
        make(mcp={"servers": {"s": {"url": "http://remote.example/mcp"}}})
    with pytest.raises(ValidationError, match=r"headers_env"):
        make(mcp={"servers": {"s": {"url": "https://x.io", "headers_env": {"A": "not a name"}}}})


def test_unknown_provider_settings_key_rejected() -> None:
    with pytest.raises(ValidationError):
        make(llm={"providers": {"mistral": {}}})


def test_import_target_is_lazy_and_errors_name_module_and_attr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "t05_probe.py").write_text("IMPORTED = True\nobj = 42\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    make(flows={"f": {"target": "t05_probe:obj"}})
    assert "t05_probe" not in sys.modules  # validating models never imports
    assert import_target("t05_probe:obj") == 42
    with pytest.raises(TargetImportError, match=r"t05_probe.*missing"):
        import_target("t05_probe:missing")
    with pytest.raises(TargetImportError, match="no_such_mod_t05"):
        import_target("no_such_mod_t05:x")
