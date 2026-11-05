import json
import sys
from pathlib import Path

import psycopg
import pytest

from graph_agent._internal.cli import main

MODULE_NAME = "t18_db_flows"
MODULE = """
from typing import TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt


class State(TypedDict, total=False):
    answer: str


g = StateGraph(State)
g.add_node("n", lambda s: {"answer": interrupt("name?")})
g.add_edge(START, "n")
g.add_edge("n", END)
"""
FLOWS = """
[limits]
cpu_workers = 1
[flows.asks]
target = "MOD:g"
"""
THREAD = "8f5c3b52-5d3f-4c0e-9b53-0e0f3b1c2d4a"


@pytest.fixture
def cli_env(clean_database: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    (tmp_path / f"{MODULE_NAME}.py").write_text(MODULE)
    (tmp_path / "graph-agent.toml").write_text(FLOWS.replace("MOD", MODULE_NAME))
    monkeypatch.delitem(sys.modules, MODULE_NAME, raising=False)
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.chdir(tmp_path)  # no stray .env
    monkeypatch.setenv("GRAPH_AGENT_CONFIG", str(tmp_path / "graph-agent.toml"))
    monkeypatch.setenv("GRAPH_AGENT_DATABASE__URL", clean_database)
    return clean_database


def _tables(url: str) -> set[str]:
    with psycopg.connect(url) as conn:
        rows = conn.execute(
            "SELECT table_schema || '.' || table_name FROM information_schema.tables "
            "WHERE table_schema = 'graph_agent'"
        ).fetchall()
    return {row[0] for row in rows}


def test_migrate_applies_then_is_idempotent(
    cli_env: str, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["migrate"]) == 0
    first = capsys.readouterr().out
    assert "migrations: applied [1" in first and "checkpointer: ready" in first
    tables = _tables(cli_env)
    assert {"graph_agent.runs", "graph_agent.schema_version", "graph_agent.checkpoints"} <= tables

    assert main(["migrate"]) == 0
    assert "migrations: up to date" in capsys.readouterr().out
    assert _tables(cli_env) == tables


def test_run_persists_then_resumes_on_the_same_thread(
    cli_env: str, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["migrate"]) == 0
    capsys.readouterr()

    assert main(["run", "asks", "--thread", THREAD, "--input", "{}"]) == 0
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert events[-1]["data"]["status"] == "interrupted"
    interrupt_id = events[-2]["data"]["interrupts"][0]["id"]

    resume = json.dumps({interrupt_id: "ada"})
    assert main(["run", "asks", "--thread", THREAD, "--resume", resume]) == 0
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert events[-1]["data"]["status"] == "success"

    with psycopg.connect(cli_env) as conn:
        statuses = conn.execute(
            "SELECT status FROM graph_agent.runs WHERE thread_id = %s ORDER BY created_at",
            (THREAD,),
        ).fetchall()
    assert [s[0] for s in statuses] == ["interrupted", "success"]


def test_run_rejects_a_thread_of_another_flow(
    cli_env: str, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    assert main(["migrate"]) == 0
    assert main(["run", "asks", "--thread", THREAD, "--input", "{}"]) == 0
    capsys.readouterr()
    (tmp_path / "graph-agent.toml").write_text(
        FLOWS.replace("MOD", MODULE_NAME)
        + '[flows.other]\ntarget = "MOD:g"\n'.replace("MOD", MODULE_NAME)
    )
    assert main(["run", "other", "--thread", THREAD, "--input", "{}"]) == 1
    last = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert last["data"]["code"] == "invalid_request"
