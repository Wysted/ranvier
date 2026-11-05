from pathlib import Path

import pytest

from graph_agent._internal.cli import main
from graph_agent._internal.config.manifest import CronSpec
from graph_agent._internal.worker import cron_periodic_id

MAX_PERIODIC_ID = 128  # Procrastinate's periodic_id column

TOML = """
[flows.alpha]
target = "pkg.mod:alpha"
[[flows.alpha.crons]]
schedule = "0 3 * * *"
input = { source = "daily" }
[[flows.alpha.crons]]
schedule = "*/15 * * * *"

[flows.beta]
target = "pkg.mod:beta"
"""


@pytest.fixture
def toml_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "graph-agent.toml"
    monkeypatch.chdir(tmp_path)  # no stray .env
    monkeypatch.setenv("GRAPH_AGENT_CONFIG", str(path))
    return path


def test_crons_lists_flow_schedule_and_next_fire_in_utc(
    toml_env: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    toml_env.write_text(TOML)
    assert main(["crons"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 2
    assert lines[0].startswith("alpha") and "0 3 * * *" in lines[0]
    assert lines[0].endswith(" 03:00:00 UTC")
    assert lines[1].startswith("alpha") and "*/15 * * * *" in lines[1]
    assert lines[1].endswith(" UTC")
    assert "beta" not in "".join(lines)


def test_crons_without_crons_says_so(toml_env: Path, capsys: pytest.CaptureFixture[str]) -> None:
    toml_env.write_text('[flows.beta]\ntarget = "pkg.mod:beta"\n')
    assert main(["crons"]) == 0
    assert capsys.readouterr().out.strip() == "no crons declared"


def test_periodic_id_follows_the_content_not_the_position() -> None:
    a = CronSpec(schedule="0 3 * * *", input={"x": 1, "y": 2})
    same = CronSpec(schedule="0 3 * * *", input={"y": 2, "x": 1})
    other_input = CronSpec(schedule="0 3 * * *", input={"x": 2})
    other_schedule = CronSpec(schedule="0 4 * * *", input={"x": 1, "y": 2})
    assert cron_periodic_id("f", a) == cron_periodic_id("f", same)
    assert len({cron_periodic_id("f", c) for c in (a, other_input, other_schedule)}) == 3
    assert cron_periodic_id("f", a) != cron_periodic_id("g", a)
    assert len(cron_periodic_id("f" * 63, a)) <= MAX_PERIODIC_ID
