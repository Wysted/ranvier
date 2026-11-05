#!/usr/bin/env bash
# Packaging checks (design.md section 11, row Packaging).
#   members  every uv workspace member installs alone (no dev group) in a fresh venv and imports
#   git      `graph-agent @ git+...#subdirectory=packages/core` installs from the current commit
#   all      both (default)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Fresh venv named $1; prints its python path.
fresh_venv() {
    uv venv --quiet "$WORK/$1" >&2
    printf '%s/bin/python' "$WORK/$1"
}

# Member directories from [tool.uv.workspace].members (globs expanded, one per line).
workspace_members() {
    # uv-managed Python: the system python3 may predate tomllib (3.11).
    uv run --no-project python -I - "$ROOT" <<'PY'
import sys
import tomllib
from pathlib import Path

root = Path(sys.argv[1])
members = tomllib.loads((root / "pyproject.toml").read_text())["tool"]["uv"]["workspace"]["members"]
for pattern in members:
    for path in sorted(root.glob(pattern)):
        if (path / "pyproject.toml").is_file():
            print(path.relative_to(root))
PY
}

# Top-level importable package of a member: the single src/<pkg>/__init__.py directory.
top_package() {
    local inits=("$1"/src/*/__init__.py)
    if [ "${#inits[@]}" -ne 1 ] || [ ! -f "${inits[0]}" ]; then
        echo "cannot determine a single top package for $1 (found: ${inits[*]})" >&2
        return 1
    fi
    basename "$(dirname "${inits[0]}")"
}

check_members() {
    local member members pkg py
    # Assigned first so a failing listing aborts (a process substitution would swallow it).
    members="$(workspace_members)"
    [ -n "$members" ] || { echo "no workspace members found" >&2; return 1; }
    while IFS= read -r member; do
        pkg="$(top_package "$member")"
        echo "==> $member: fresh venv, install without dev group, import $pkg"
        py="$(fresh_venv "member-${member//\//_}")"
        uv pip install --quiet --python "$py" "./$member"
        "$py" -I -c "import $pkg; print('imported', $pkg.__name__)"
    done <<<"$members"
}

check_git_subdirectory() {
    local sha py
    sha="$(git rev-parse HEAD)"
    echo "==> git+file install of packages/core at $sha"
    py="$(fresh_venv git-subdir)"
    uv pip install --quiet --python "$py" \
        "graph-agent @ git+file://$ROOT@$sha#subdirectory=packages/core"
    "$py" -I -c "import graph_agent; print('imported', graph_agent.__name__)"
}

case "${1:-all}" in
    members) check_members ;;
    git) check_git_subdirectory ;;
    all) check_members; check_git_subdirectory ;;
    *) echo "usage: $0 [members|git|all]" >&2; exit 2 ;;
esac
printf '\nInstall checks passed.\n'
