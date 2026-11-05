#!/usr/bin/env bash
# S5 spike: one command for every check. Run from anywhere: bench/spikes/S5/run.sh
set -euo pipefail
cd "$(dirname "$0")/../../.."
for py in 3.14 3.13; do
  echo "=== pool_check on $py ==="
  time uv run --no-project --python "$py" bench/spikes/S5/pool_check.py
  echo "=== recommended_check on $py ==="
  uv run --no-project --python "$py" bench/spikes/S5/recommended_check.py
done
echo "=== langgraph_check on the project venv ==="
uv run python bench/spikes/S5/langgraph_check.py
