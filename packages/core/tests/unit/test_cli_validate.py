import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from graph_agent._internal.cli import main

type Manifest = Callable[[str], None]  # writes graph-agent.toml

MODULE_NAME = "t17_user_flows"
MODULE = """
from types import SimpleNamespace
from typing import TypedDict

from langgraph.graph import END, START, StateGraph


class State(TypedDict):
    x: int


def _builder():
    g = StateGraph(State)
    g.add_node("n", lambda s: {"x": 1})
    g.add_edge(START, "n")
    g.add_edge("n", END)
    return g


good = _builder()
compiled = _builder().compile()
not_a_graph = 42


def uses_unknown_profile(ctx):
    ctx.model("main")
    ctx.model("ghost")
    return _builder()


async def uses_unknown_refs(ctx):
    ctx.tools("nope")
    spec = SimpleNamespace(name="a", profile="main", tools=("web",), mcp_servers=("docs",))
    await ctx.agent(spec)
    return _builder()


def tool_fn(x: str) -> str:
    return x
"""

PROFILE = """
[llm.profiles.main]
provider = "ollama"
model = "llama3"
"""


@pytest.fixture
def manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Manifest:
    (tmp_path / f"{MODULE_NAME}.py").write_text(MODULE)
    monkeypatch.delitem(sys.modules, MODULE_NAME, raising=False)
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.chdir(tmp_path)  # no stray .env
    monkeypatch.setenv("GRAPH_AGENT_CONFIG", str(tmp_path / "graph-agent.toml"))

    def write(toml: str) -> None:
        (tmp_path / "graph-agent.toml").write_text(toml.replace("MOD", MODULE_NAME))

    return write


def test_good_manifest_prints_diagram(
    manifest: Manifest, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest(
        PROFILE
        + """
[flows.summarize]
target = "MOD:good"
[tools.echo]
target = "MOD:tool_fn"
"""
    )
    assert main(["validate", "--offline", "--diagram"]) == 0
    out = capsys.readouterr().out
    assert "%% flow: summarize" in out
    assert "n" in out


def test_collects_every_problem_and_names_keys(
    manifest: Manifest, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest(
        PROFILE
        + """
[flows.missing]
target = "MOD:nothing_here"
[flows.prebuilt]
target = "MOD:compiled"
[flows.scalar]
target = "MOD:not_a_graph"
[flows.profile]
target = "MOD:uses_unknown_profile"
[flows.refs]
target = "MOD:uses_unknown_refs"
[tools.bad]
target = "MOD:nothing_here"
"""
    )
    assert main(["validate", "--offline"]) == 1
    err = capsys.readouterr().err
    assert "flows.missing.target: " in err
    assert "flows.prebuilt.target: " in err
    assert "compiled graph" in err
    assert "flows.scalar.target: " in err
    assert "flows.profile.target: unknown LLM profile 'ghost'" in err
    assert "flows.refs.target: unknown tool 'nope'" in err
    assert "flows.refs.target: unknown tool 'web'" in err
    assert "flows.refs.target: unknown MCP server 'docs'" in err
    assert "tools.bad.target: " in err


def test_bad_cron_names_the_key(manifest: Manifest, capsys: pytest.CaptureFixture[str]) -> None:
    manifest(
        """
[flows.a]
target = "MOD:good"
[[flows.a.crons]]
schedule = "not a cron"
"""
    )
    assert main(["validate", "--offline"]) == 1
    assert "flows.a.crons.0.schedule: " in capsys.readouterr().err


def test_missing_header_env_is_named_by_variable_only(
    manifest: Manifest,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("T17_KEY", raising=False)
    manifest(
        PROFILE
        + """
[mcp.servers.docs]
url = "https://example.com/mcp"
headers_env = { Authorization = "T17_KEY" }
[flows.a]
target = "MOD:good"
"""
    )
    assert main(["validate"]) == 1
    assert "mcp.servers.docs.headers_env.Authorization: T17_KEY is not set" in (
        capsys.readouterr().err
    )
    # --offline skips the environment entirely
    assert main(["validate", "--offline"]) == 0
