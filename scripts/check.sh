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

step "ruff check" uv run ruff check . ../../examples
step "ruff format --check" uv run ruff format --check . ../../examples
step "mypy" uv run mypy
step "import-linter" uv run lint-imports
step "pytest (unit)" uv run pytest -q -c pyproject.toml tests ../../examples/tests

printf '\nAll checks passed.\n'
