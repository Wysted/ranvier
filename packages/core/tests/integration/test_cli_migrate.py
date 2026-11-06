import json
import sys
import threading
from pathlib import Path

import psycopg
import pytest

from ranvier._internal.cli import main

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
    (tmp_path / "ranvier.toml").write_text(FLOWS.replace("MOD", MODULE_NAME))
    monkeypatch.delitem(sys.modules, MODULE_NAME, raising=False)
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.chdir(tmp_path)  # no stray .env
    monkeypatch.setenv("RANVIER_CONFIG", str(tmp_path / "ranvier.toml"))
    monkeypatch.setenv("RANVIER_DATABASE__URL", clean_database)
    return clean_database


def _tables(url: str) -> set[str]:
    with psycopg.connect(url) as conn:
        rows = conn.execute(
            "SELECT table_schema || '.' || table_name FROM information_schema.tables "
            "WHERE table_schema = 'ranvier'"
        ).fetchall()
    return {row[0] for row in rows}


def test_migrate_applies_then_is_idempotent(
    cli_env: str, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["migrate"]) == 0
    first = capsys.readouterr().out
    assert "migrations: applied [1" in first and "checkpointer: ready" in first
    tables = _tables(cli_env)
    assert {"ranvier.runs", "ranvier.schema_version", "ranvier.checkpoints"} <= tables
    with psycopg.connect(cli_env) as conn:  # saver tables live in our schema, not the role's
        row = conn.execute("SELECT to_regclass('ranvier.checkpoints')").fetchone()
    assert row is not None and row[0] is not None

    assert main(["migrate"]) == 0
    assert "migrations: up to date" in capsys.readouterr().out
    assert _tables(cli_env) == tables


def test_migrate_pins_our_schema_whatever_the_url_search_path_says(
    cli_env: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The role's "$user" would hide a missing pin: a URL forcing `public` exposes it."""
    monkeypatch.setenv("RANVIER_DATABASE__URL", f"{cli_env}?options=-c%20search_path%3Dpublic")
    assert main(["migrate"]) == 0
    capsys.readouterr()
    with psycopg.connect(cli_env) as conn:
        rows = conn.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')"
        ).fetchall()
    by_schema = {schema: {name for s, name in rows if s == schema} for schema, _ in rows}
    assert set(by_schema) == {"ranvier"}, by_schema.keys()
    assert {"runs", "checkpoints", "procrastinate_jobs", "procrastinate_migrations"} <= by_schema[
        "ranvier"
    ]


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
            "SELECT status FROM ranvier.runs WHERE thread_id = %s ORDER BY created_at",
            (THREAD,),
        ).fetchall()
    assert [s[0] for s in statuses] == ["interrupted", "success"]


def test_run_rejects_a_thread_of_another_flow(
    cli_env: str, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    assert main(["migrate"]) == 0
    assert main(["run", "asks", "--thread", THREAD, "--input", "{}"]) == 0
    capsys.readouterr()
    (tmp_path / "ranvier.toml").write_text(
        FLOWS.replace("MOD", MODULE_NAME)
        + '[flows.other]\ntarget = "MOD:g"\n'.replace("MOD", MODULE_NAME)
    )
    assert main(["run", "other", "--thread", THREAD, "--input", "{}"]) == 1
    last = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert last["data"]["code"] == "invalid_request"


CONCURRENT_MIGRATE_TIMEOUT_S = 60


def test_concurrent_migrates_both_finish(cli_env: str) -> None:
    """A blocked `pg_advisory_lock` used to deadlock against `CREATE INDEX CONCURRENTLY`."""
    codes: list[int] = []
    threads = [
        threading.Thread(target=lambda: codes.append(main(["migrate"])), daemon=True)
        for _ in range(2)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(CONCURRENT_MIGRATE_TIMEOUT_S)
    assert not any(t.is_alive() for t in threads), "concurrent migrate hung"
    assert codes == [0, 0]


def test_run_on_a_busy_thread_cancels_its_row(
    cli_env: str, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["migrate"]) == 0
    with psycopg.connect(cli_env, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO ranvier.runs "
            "(run_id, thread_id, flow, flow_version, status, input, lease_owner, lease_expires_at) "
            "VALUES (gen_random_uuid(), %s, 'asks', 1, 'running', '{}', 'w', "
            "now() + interval '1 hour')",
            (THREAD,),
        )
    capsys.readouterr()
    assert main(["run", "asks", "--thread", THREAD, "--input", "{}"]) == 1
    last = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert last["data"]["code"] == "thread_busy"
    with psycopg.connect(cli_env) as conn:
        rows = conn.execute(
            "SELECT status FROM ranvier.runs WHERE thread_id = %s AND status <> 'running'",
            (THREAD,),
        ).fetchall()
    assert [r[0] for r in rows] == ["cancelled"]
