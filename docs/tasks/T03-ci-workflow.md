---
id: T03
title: CI: gate, clean installs, git-subdirectory install
part: 1
wave: C
depends_on: [T02]
unblocks: [T35]
owner: sa-build-py
reviewer: sa-review-py
branch: task/T03-ci-workflow
status: done
---

## Goal
A CI workflow that runs the gate on Python 3.13 and 3.14, installs every workspace member in a fresh venv, and proves the `git+...#subdirectory=packages/core` install works.

## Design references
- design.md §2 "Workspace" (paragraph on `v0.1.0` and the git-subdirectory dependency)
- design.md §11 "Platform test layers" row Packaging
- design.md §14 "Open checks" row `uv add "git+...@tag#subdirectory=packages/core"`
- design.md §13 row "Contracts and gate" (gate adds per-member clean install)

## Scope: files this task may create or edit
- .github/workflows/ci.yml
- scripts/ci-install-check.sh
Shared files: none.

## Out of scope
- Release/tag workflow
- Starter CI (T34)
- Changing `scripts/check.sh` (T02)

## Acceptance criteria
- [ ] Job `gate`: matrix 3.13 and 3.14, `uv sync`, `scripts/check.sh`; a Postgres service and a second step `pytest -m integration` (the tests arrive with T06+; the step must pass with zero collected tests via `--no-header` handling or `|| [ $? -eq 5 ]`, documented)
- [ ] Job `install`: for each uv workspace member (derive the list from the root `pyproject.toml`, do not hardcode) create a fresh venv, install only that member without dev group, import its top package
- [ ] `scripts/ci-install-check.sh` does the git-subdirectory install from the checked-out commit (`git+file://$PWD@$SHA#subdirectory=packages/core`) in a fresh venv and imports `ranvier`; the result of the open check is stated in the PR description
- [ ] Workflow lints clean (`actionlint` if available); uses `astral-sh/setup-uv`; caches uv
- [ ] `scripts/check.sh` green
- [ ] import-linter contracts hold
- [ ] no emojis, no AI attribution in code or commits; conventional commits

## Notes
- Port structure from `agintis/.github/workflows/ci.yml`; drop what ranvier does not have.
- The git host is GitHub (github.com/Wysted/ranvier); GitHub Actions is used. Keep host-specific lines isolated.
- Python 3.14 failing because langgraph is unclassified is information, not a reason to drop it: mark that matrix leg `continue-on-error` and say so (OD-7).
- Conventions: follow the project's Python conventions and the decision log; never read `.env`; agintis is read-only reference.
