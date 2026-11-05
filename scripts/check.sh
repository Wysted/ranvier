#!/usr/bin/env bash
# Local quality gate: lint, format, types, import contracts, unit tests.
# Integration tests need Postgres (docker compose up -d): uv run pytest -m integration
# Tool config lives in packages/core/pyproject.toml, so tools run from there (docs/ stays out of ruff).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../packages/core"

step() {
    printf '\n==> %s\n' "$1"
    shift
    "$@"
}

step "ruff check" uv run ruff check .
step "ruff format --check" uv run ruff format --check .
step "mypy" uv run mypy
step "import-linter" uv run lint-imports
run_pytest() {
    local rc=0
    uv run pytest -q || rc=$?
    # Exit 5 = "no tests collected".
    # shortcut: tolerates an empty suite; drop the exit-5 case once the first unit test lands.
    [ "$rc" -eq 0 ] || [ "$rc" -eq 5 ]
}

step "pytest (unit)" run_pytest

printf '\nAll checks passed.\n'
