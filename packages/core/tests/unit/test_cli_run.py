import json
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from ranvier._internal.cli import EXIT_USAGE, main

MODULE_NAME = "t18_user_flows"
MODULE = """
import asyncio
from typing import TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt


class State(TypedDict, total=False):
    x: int
    answer: str


def _builder(node):
    g = StateGraph(State)
    g.add_node("n", node)
    g.add_edge(START, "n")
    g.add_edge("n", END)
    return g


good = _builder(lambda s: {"x": s["x"] + 1})
asks = _builder(lambda s: {"answer": interrupt("name?")})


def _boom(s):
    raise ValueError("token=hunter2")


boom = _builder(_boom)


async def _slow(s):
    await asyncio.sleep(30)


slow = _builder(_slow)
"""
FLOWS = """
[limits]
cpu_workers = 1
[flows.good]
target = "MOD:good"
[flows.asks]
target = "MOD:asks"
[flows.boom]
target = "MOD:boom"
[flows.slow]
target = "MOD:slow"
timeout_s = 0.2
"""

type Events = list[dict[str, object]]


@pytest.fixture(autouse=True)
def _manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / f"{MODULE_NAME}.py").write_text(MODULE)
    (tmp_path / "ranvier.toml").write_text(FLOWS.replace("MOD", MODULE_NAME))
    monkeypatch.delitem(sys.modules, MODULE_NAME, raising=False)
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.chdir(tmp_path)  # no stray .env
    monkeypatch.setenv("RANVIER_CONFIG", str(tmp_path / "ranvier.toml"))


@pytest.fixture
def run(capsys: pytest.CaptureFixture[str]) -> Callable[..., tuple[int, Events]]:
    def go(*argv: str) -> tuple[int, Events]:
        code = main(["run", *argv])
        return code, [json.loads(line) for line in capsys.readouterr().out.splitlines()]

    return go


def names(events: Events) -> list[object]:
    return [e["event"] for e in events]


def test_memory_success_prints_json_lines(run: Callable[..., tuple[int, Events]]) -> None:
    code, events = run("good", "--memory", "--input", '{"x": 1}')
    assert code == 0
    assert names(events) == ["metadata", "updates", "end"]
    assert events[1]["data"]["update"] == {"x": 2}  # type: ignore[index]
    assert events[-1]["data"]["status"] == "success"  # type: ignore[index]


def test_memory_interrupt_exits_zero(run: Callable[..., tuple[int, Events]]) -> None:
    code, events = run("asks", "--memory", "--input", "{}")
    assert code == 0
    assert names(events)[-2:] == ["interrupt", "end"]
    interrupts = events[-2]["data"]["interrupts"]  # type: ignore[index]
    assert interrupts[0]["value"] == "name?" and interrupts[0]["id"]
    assert events[-1]["data"]["status"] == "interrupted"  # type: ignore[index]


def test_memory_flow_error_exits_one_without_leaking(
    run: Callable[..., tuple[int, Events]],
) -> None:
    code, events = run("boom", "--memory", "--input", "{}")
    assert code == 1
    assert events[-1] == {
        "event": "error",
        "data": {"code": "internal_error", "message": "ValueError", "retryable": False},
    }


def test_memory_timeout_is_an_error_event(run: Callable[..., tuple[int, Events]]) -> None:
    code, events = run("slow", "--memory", "--input", "{}")
    assert code == 1
    assert events[-1]["data"]["code"] == "run_timeout"  # type: ignore[index]


def test_missing_api_key_env_is_a_one_line_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest = tmp_path / "ranvier.toml"
    manifest.write_text(
        manifest.read_text()
        + '[llm.profiles.p]\nprovider = "openai"\nmodel = "m"\napi_key_env = "T18_NO_SUCH_KEY"\n'
    )
    monkeypatch.delenv("T18_NO_SUCH_KEY", raising=False)
    assert main(["run", "good", "--memory", "--input", "{}"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "profile 'p': environment variable T18_NO_SUCH_KEY is not set\n"


def test_unknown_flow_exits_one(run: Callable[..., tuple[int, Events]]) -> None:
    code, events = run("nope", "--memory", "--input", "{}")
    assert code == 1
    assert events[-1]["data"]["code"] == "flow_not_found"  # type: ignore[index]


@pytest.mark.parametrize(
    "argv",
    [
        ["good", "--input", "{not json"],
        ["good", "--input", "null"],
        ["good", "--input", "{}", "--resume", "{}"],
        ["good"],
        ["good", "--input", "{}", "--thread", "not-a-uuid"],
    ],
)
def test_bad_arguments_exit_two(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["run", *argv])
    assert exc.value.code == EXIT_USAGE


@pytest.mark.parametrize(
    "argv",
    [
        ["good", "--resume", '{"a": 1}'],  # no --thread
        [
            "good",
            "--resume",
            '{"a": 1}',
            "--thread",
            "8f5c3b52-5d3f-4c0e-9b53-0e0f3b1c2d4a",
            "--memory",
        ],
    ],
)
def test_resume_needs_thread_and_database(
    argv: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["run", *argv]) == EXIT_USAGE
    assert "--resume needs" in capsys.readouterr().err


def test_unserializable_event_becomes_internal_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from ranvier._internal.cli import _emit
    from ranvier._internal.runtime.events import ValuesEvent

    assert _emit(ValuesEvent((), object())) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out)["data"]["code"] == "internal_error"
    assert "not JSON-serializable" in captured.err
