---
id: T28
title: SKILL.md loader and load_skill middleware
part: 9
wave: C
depends_on: [T02]
unblocks: [T30, T35]
owner: sa-dev-py
reviewer: sa-review-py
branch: task/T28-skills
status: done
---

## Goal
Read agentskills.io `SKILL.md` folders, inject name and description into the system prompt, and expose a `load_skill(name)` tool, as a LangChain `AgentMiddleware`.

## Design references
- design.md §7 table row "Skills"
- design.md §3 `AgentSpec.skills`
- design.md "Out of scope for v0.1" (skill script execution)

## Scope: files this task may create or edit
- packages/core/src/ranvier/_internal/agents/skills.py
- packages/core/tests/unit/agents/test_skills.py
- packages/core/tests/unit/agents/skills_fixtures/ (small sample skill folders)
Shared files: none.

## Out of scope
- Executing skill scripts (out of scope)
- Wiring into `build_agent` (T30)
- `AgentSpec` (T27)

## Acceptance criteria
- [ ] `load_skills(dirs) -> list[Skill]`: each subfolder with `SKILL.md`; frontmatter requires `name` and `description`; missing/invalid fields, duplicate names, unreadable files fail loudly naming the path
- [ ] `SkillsMiddleware(dirs)` is a `langchain.agents.middleware.AgentMiddleware` that appends a compact list (name + description) to the system prompt and contributes the tool `load_skill(name)` returning the file body; unknown name returns an error text listing valid names
- [ ] No file other than `SKILL.md` is read or executed
- [ ] Tests: parsing, error cases, prompt injection (use langchain-core's `GenericFakeChatModel`, the platform fake does not exist yet), `load_skill` output
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Check the exact name/description limits in the agentskills.io spec before coding validation.
- langchain is pinned to 1.4.x; verify the middleware hook names (`wrap_model_call` / request override) against the installed version, not memory.
- Frontmatter parsing uses PyYAML `safe_load` (already a transitive dependency; T01 declares it).
- Conventions: read `.claude/agents/_shared/python.md` and `_shared/decisions.md` first; never read `.env`; agintis is read-only reference.
