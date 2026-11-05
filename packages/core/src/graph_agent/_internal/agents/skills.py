"""SKILL.md loader (agentskills.io) and the middleware exposing skills to an agent.

Only ``SKILL.md`` is ever read; bundled scripts are never executed.
"""

import re
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]  # shortcut: no types-PyYAML in dev deps, add it with the next pyproject edit
from langchain.agents.middleware import AgentMiddleware, ModelRequest, ModelResponse
from langchain_core.messages import AIMessage, SystemMessage
from langchain_core.tools import tool

SKILL_FILE = "SKILL.md"
MAX_NAME_LENGTH = 64
MAX_DESCRIPTION_LENGTH = 1024
_FRONTMATTER_DELIMITER = "---"
_NAME_PATTERN = re.compile(r"[a-z0-9]+(-[a-z0-9]+)*")


class SkillError(ValueError):
    """Invalid or unreadable skill; the message names the offending path."""


@dataclass(frozen=True, slots=True)
class Skill:
    name: str
    description: str
    body: str
    path: Path


def _parse_skill(path: Path) -> Skill:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise SkillError(f"{path}: cannot read skill file: {exc}") from exc

    lines = text.split("\n")
    if lines[0].strip() != _FRONTMATTER_DELIMITER:
        raise SkillError(f"{path}: missing YAML frontmatter")
    try:
        end = next(i for i, ln in enumerate(lines[1:], 1) if ln.strip() == _FRONTMATTER_DELIMITER)
    except StopIteration:
        raise SkillError(f"{path}: unterminated YAML frontmatter") from None
    try:
        meta = yaml.safe_load("\n".join(lines[1:end]))
    except yaml.YAMLError as exc:
        raise SkillError(f"{path}: invalid YAML frontmatter: {exc}") from exc
    if not isinstance(meta, dict):
        raise SkillError(f"{path}: frontmatter must be a mapping")

    name, description = meta.get("name"), meta.get("description")
    if not isinstance(name, str) or not name:
        raise SkillError(f"{path}: 'name' is required and must be a non-empty string")
    if len(name) > MAX_NAME_LENGTH or not _NAME_PATTERN.fullmatch(name):
        raise SkillError(
            f"{path}: invalid name {name!r} (lowercase letters, digits and single hyphens, "
            f"no leading/trailing hyphen, max {MAX_NAME_LENGTH} chars)"
        )
    if name != path.parent.name:
        raise SkillError(f"{path}: name {name!r} must match the directory name")
    if not isinstance(description, str) or not description.strip():
        raise SkillError(f"{path}: 'description' is required and must be a non-empty string")
    if len(description) > MAX_DESCRIPTION_LENGTH:
        raise SkillError(f"{path}: description exceeds {MAX_DESCRIPTION_LENGTH} chars")

    return Skill(name, description.strip(), "\n".join(lines[end + 1 :]).strip(), path)


def load_skills(dirs: Sequence[Path]) -> list[Skill]:
    """Load every ``<dir>/<skill>/SKILL.md``; subfolders without SKILL.md are ignored."""
    skills: dict[str, Skill] = {}
    for root in dirs:
        if not root.is_dir():
            raise SkillError(f"{root}: skills directory does not exist")
        for folder in sorted(p for p in root.iterdir() if p.is_dir()):
            file = folder / SKILL_FILE
            if not file.is_file():
                continue
            skill = _parse_skill(file)
            if skill.name in skills:
                raise SkillError(
                    f"{file}: duplicate skill name {skill.name!r} "
                    f"(also in {skills[skill.name].path})"
                )
            skills[skill.name] = skill
    return list(skills.values())


class SkillsMiddleware(AgentMiddleware):
    """Lists skills in the system prompt and exposes ``load_skill(name)``."""

    def __init__(self, dirs: Sequence[Path]) -> None:
        super().__init__()
        skills = load_skills(dirs)
        by_name = {s.name: s for s in skills}
        listing = "\n".join(f"- {s.name}: {s.description}" for s in skills)
        self._prompt = (
            "## Skills\n\nCall `load_skill(name)` to read a skill's full instructions "
            f"before using it. Available skills:\n{listing}"
            if skills
            else ""
        )

        @tool
        def load_skill(name: str) -> str:
            """Load the full instructions of a skill by name."""
            found = by_name.get(name)
            if found is None:
                return f"Unknown skill {name!r}. Valid skills: {', '.join(by_name) or 'none'}"
            return found.body

        self.tools = [load_skill] if skills else []

    def _with_skills(self, request: ModelRequest[Any]) -> ModelRequest[Any]:
        if not self._prompt:
            return request
        current = request.system_message.text if request.system_message else ""
        text = f"{current}\n\n{self._prompt}" if current else self._prompt
        return request.override(system_message=SystemMessage(content=text))

    def wrap_model_call(
        self,
        request: ModelRequest[Any],
        handler: Callable[[ModelRequest[Any]], ModelResponse[Any]],
    ) -> ModelResponse[Any] | AIMessage:
        return handler(self._with_skills(request))

    async def awrap_model_call(
        self,
        request: ModelRequest[Any],
        handler: Callable[[ModelRequest[Any]], Awaitable[ModelResponse[Any]]],
    ) -> ModelResponse[Any] | AIMessage:
        return await handler(self._with_skills(request))
