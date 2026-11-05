#!/usr/bin/env bash
# Local quality gate (same shape as the platform's): lint, format, types, import contracts, tests.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

step() {
    printf '\n==> %s\n' "$1"
    shift
    "$@"
}

step "ruff check" uv run ruff check .
step "ruff format --check" uv run ruff format --check .
step "mypy" uv run mypy
step "import-linter" uv run lint-imports
step "pytest" uv run pytest -q

printf '\nAll checks passed.\n'
