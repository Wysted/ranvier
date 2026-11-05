import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from graph_agent._internal.cli import main

type Manifest = Callable[[str], None]  # writes graph-agent.toml

MODULE_NAME = "t17_user_flows"
MODULE = """
from typing import TypedDict

from langchain_core.tools import tool

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
    from graph_agent._internal.agents.spec import AgentSpec

    await ctx.agent(AgentSpec(name="a", profile="main", tools=("web",), mcp_servers=("docs",)))
    return _builder()


async def builds_agent(ctx):
    from graph_agent._internal.agents.spec import AgentSpec

    spec = AgentSpec(name="a", profile="main", tools=("echo",), mcp_servers=("docs",))
    agent = await ctx.agent(spec)
    assert hasattr(agent, "ainvoke")
    return _builder()


async def duplicate_tool(ctx):
    from graph_agent._internal.agents.spec import AgentSpec

    await ctx.agent(AgentSpec(name="a", profile="main", tools=("echo", echo)))
    return _builder()


async def duplicate_tool_ghost(ctx):
    from graph_agent._internal.agents.spec import AgentSpec

    await ctx.agent(AgentSpec(name="a", profile="ghost", tools=("echo", echo)))
    return _builder()


async def uses_tools(ctx):
    names = [t.name for t in ctx.tools("echo", "sync_only", "broken")]
    assert names == ["echo"], names
    return _builder()


async def uses_model(ctx):
    ctx.model("main")
    return _builder()


async def raises_with_secret(ctx):
    raise ValueError("token=hunter2")


@tool(description="Echo.")
async def echo(x: str) -> str:
    return x


@tool(description="Sync only.")
def sync_only(x: str) -> str:
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
target = "MOD:echo"
"""
    )
    assert main(["validate", "--offline", "--diagram"]) == 0
    out = capsys.readouterr().out
    assert "flow: summarize" in out
    assert "```mermaid" in out
    assert "__start__ --> n" in out


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
[flows.secret]
target = "MOD:raises_with_secret"
[tools.bad]
target = "MOD:nothing_here"
[tools.sync]
target = "MOD:sync_only"
[tools.scalar]
target = "MOD:not_a_graph"
"""
    )
    assert main(["validate", "--offline"]) == 1
    err = capsys.readouterr().err
    assert "flows.missing.target: " in err
    assert "flows.prebuilt: target is a compiled graph" in err
    assert "compiled graph" in err
    assert "flows.scalar: target must be a StateGraph" in err
    assert "flows.profile.target: unknown LLM profile 'ghost'" in err
    assert "flows.refs.target: unknown tool 'nope'" in err
    assert "flows.refs.target: unknown tool 'web'" in err
    assert "flows.refs.target: unknown MCP server 'docs'" in err
    assert "tools.bad: " in err
    assert "tools.sync" in err and "isolation = 'thread'" in err  # the registry's own rule
    assert "tools.scalar" in err and "not a langchain tool" in err
    assert "flows.secret.target: ValueError at " in err
    assert "hunter2" not in err


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
    assert "mcp.servers.docs.headers_env: MCP server 'docs': env var T17_KEY" in (
        capsys.readouterr().err
    )
    # --offline skips the environment entirely
    assert main(["validate", "--offline"]) == 0


def test_flow_sees_wrapped_registry_tools_and_unknown_names(
    manifest: Manifest, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest(
        """
[flows.t]
target = "MOD:uses_tools"
[tools.echo]
target = "MOD:echo"
[tools.sync_only]
target = "MOD:sync_only"
[tools.broken]
target = "MOD:nothing_here"
"""
    )
    assert main(["validate", "--offline"]) == 1
    err = capsys.readouterr().err
    assert "tools.sync_only" in err and "tools.broken" in err
    assert "flows.t" not in err  # the flow itself built fine: failing tools are omitted


def test_real_mode_builds_models_up_front(
    manifest: Manifest, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest(
        PROFILE
        + """
[llm.profiles.other]
provider = "ollama"
model = "x"
[flows.m]
target = "MOD:uses_model"
"""
    )
    assert main(["validate"]) == 0, capsys.readouterr().err


def test_missing_or_broken_manifest_is_one_line(
    manifest: Manifest, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest("this is = not [valid toml")
    assert main(["validate", "--offline"]) == 1
    err = capsys.readouterr().err.strip()
    assert len(err.splitlines()) == 1 and "Traceback" not in err


def test_missing_manifest_file_is_one_line(
    manifest: Manifest, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GRAPH_AGENT_CONFIG", "does-not-exist.toml")
    assert main(["validate", "--offline"]) == 1
    err = capsys.readouterr().err.strip()
    assert len(err.splitlines()) == 1 and "Traceback" not in err
    assert "does-not-exist.toml" in err


def test_flows_module_does_not_import_validation_deps() -> None:
    code = (
        "import sys, graph_agent._internal.runtime.flows;"
        "bad = [m for m in ('mcp', 'graph_agent._internal.runtime.offline',"
        " 'graph_agent._internal.llm', 'graph_agent._internal.config.settings')"
        " if m in sys.modules];"
        "print(','.join(bad))"
    )
    done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == ""


def test_agents_are_built_for_real_without_connecting_to_mcp(
    manifest: Manifest, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest(
        PROFILE
        + """
[mcp.servers.docs]
url = "https://example.invalid/mcp"
[tools.echo]
target = "MOD:echo"
[flows.ok]
target = "MOD:builds_agent"
[flows.dup]
target = "MOD:duplicate_tool"
"""
    )
    assert main(["validate", "--offline"]) == 1
    err = capsys.readouterr().err
    assert "flows.dup.target: agent 'a': duplicate tool name 'echo'" in err  # the real factory
    assert "flows.ok" not in err


def test_collision_does_not_hide_an_unknown_profile(
    manifest: Manifest, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest(
        """
[tools.echo]
target = "MOD:echo"
[flows.dup]
target = "MOD:duplicate_tool_ghost"
"""
    )
    assert main(["validate", "--offline"]) == 1
    err = capsys.readouterr().err
    assert "unknown LLM profile 'ghost'" in err and "duplicate tool name 'echo'" in err
