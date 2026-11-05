from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from langchain.agents.middleware import ModelRequest, ModelResponse
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from graph_agent._internal.agents.skills import SkillError, SkillsMiddleware, load_skills

FIXTURES = Path(__file__).parent / "skills_fixtures"


def _write(root: Path, folder: str, content: str) -> None:
    (root / folder).mkdir(parents=True)
    (root / folder / "SKILL.md").write_text(content, encoding="utf-8")


def test_load_skills_parses_fixtures() -> None:
    skills = load_skills([FIXTURES])
    assert [s.name for s in skills] == ["code-review", "pdf-processing"]
    assert skills[1].description == "Extract PDF text. Use for PDFs."
    assert skills[1].body == "# PDF\n\nStep one."


@pytest.mark.parametrize(
    ("content", "fragment"),
    [
        ("no frontmatter", "missing YAML frontmatter"),
        ("---\nname: bad\n", "unterminated"),
        ("---\ndescription: d\n---\n", "'name' is required"),
        ("---\nname: bad\n---\n", "'description' is required"),
        ("---\nname: Bad\ndescription: d\n---\n", "invalid name"),
        ("---\nname: bad--x\ndescription: d\n---\n", "invalid name"),
        ("---\nname: other\ndescription: d\n---\n", "must match the directory"),
        (f"---\nname: bad\ndescription: {'x' * 1025}\n---\n", "exceeds"),
    ],
)
def test_invalid_skill_fails_with_path(tmp_path: Path, content: str, fragment: str) -> None:
    _write(tmp_path, "bad", content)
    with pytest.raises(SkillError, match=fragment) as exc:
        load_skills([tmp_path])
    assert str(tmp_path / "bad" / "SKILL.md") in str(exc.value)


def test_duplicate_names_fail(tmp_path: Path) -> None:
    _write(tmp_path, "pdf-processing", "---\nname: pdf-processing\ndescription: d\n---\n")
    with pytest.raises(SkillError, match="duplicate"):
        load_skills([FIXTURES, tmp_path])


def test_unreadable_file_fails(tmp_path: Path) -> None:
    (tmp_path / "bad").mkdir()
    (tmp_path / "bad" / "SKILL.md").write_bytes(b"\xff\xfe\x00")
    with pytest.raises(SkillError, match="cannot read"):
        load_skills([tmp_path])


def test_missing_dir_fails(tmp_path: Path) -> None:
    with pytest.raises(SkillError, match="does not exist"):
        load_skills([tmp_path / "nope"])


def _request(system: str | None) -> ModelRequest[Any]:
    return ModelRequest(
        model=GenericFakeChatModel(messages=iter([AIMessage(content="ok")])),
        messages=[HumanMessage(content="hi")],
        system_message=SystemMessage(content=system) if system else None,
        tools=[],
    )


def _capture(seen: list[ModelRequest[Any]]) -> Callable[[ModelRequest[Any]], ModelResponse[Any]]:
    def handler(req: ModelRequest[Any]) -> ModelResponse[Any]:
        seen.append(req)
        return ModelResponse(result=[AIMessage(content="ok")])

    return handler


@pytest.mark.parametrize("system", ["Be brief.", None])
def test_prompt_injection(system: str | None) -> None:
    seen: list[ModelRequest[Any]] = []
    SkillsMiddleware([FIXTURES]).wrap_model_call(_request(system), _capture(seen))
    prompt = seen[0].system_prompt or ""
    assert prompt.startswith("Be brief.\n\n## Skills") if system else prompt.startswith("## Skills")
    assert "- pdf-processing: Extract PDF text. Use for PDFs." in prompt
    assert "- code-review: Review code changes." in prompt
    assert "Step one." not in prompt  # bodies load on demand only


def test_load_skill_tool() -> None:
    (load_skill,) = SkillsMiddleware([FIXTURES]).tools
    assert load_skill.name == "load_skill"
    assert load_skill.invoke({"name": "code-review"}) == "Check tests."
    unknown = load_skill.invoke({"name": "nope"})
    assert "Unknown skill" in unknown
    assert "code-review" in unknown
    assert "pdf-processing" in unknown


def test_no_skills_is_a_noop(tmp_path: Path) -> None:
    mw = SkillsMiddleware([tmp_path])
    seen: list[ModelRequest[Any]] = []
    mw.wrap_model_call(_request("Be brief."), _capture(seen))
    assert seen[0].system_prompt == "Be brief."
    assert mw.tools == []
