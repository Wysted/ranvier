#!/usr/bin/env bash
# S4 spike: one command for every check. Needs compose Postgres (127.0.0.1:25432) and docker
# (pg_dump runs inside the compose container). Creates and drops s4_* databases only.
# Run from anywhere: bench/spikes/S4/run.sh
set -euo pipefail
cd "$(dirname "$0")/../../.."
S=bench/spikes/S4
echo "=== versions ==="
uv run python -c "import importlib.metadata as m, sys; print(sys.version.split()[0], {p: m.version(p) for p in ['procrastinate', 'croniter', 'langgraph', 'langgraph-checkpoint', 'langgraph-checkpoint-postgres', 'psycopg', 'psycopg-pool']})"
docker exec graph-agent-db-1 psql -U graph_agent -d graph_agent -tAc "show server_version"
echo "=== 1. periodic (TZ set to a non-UTC zone on purpose) ==="
TZ=America/Argentina/Buenos_Aires uv run python "$S/periodic_check.py"
echo "=== 2. stalled jobs ==="
uv run python "$S/stalled_check.py" 2>&1 | rg -v -e '^ ' -e '^Traceback' -e 'Error'
echo "=== 2b/1b. stalled sweep vs a re-deferred run; periodic dedup gap ==="
uv run python "$S/sweep_check.py"
echo "=== 3. schema install and upgrade 3.0.0 -> project version ==="
uv run --no-project --python 3.14 --with procrastinate==3.0.0 --with "psycopg[binary,pool]==3.3.8" \
  python "$S/schema_check.py" install s4_upgrade
uv run python "$S/schema_check.py" upgrade s4_upgrade
uv run python "$S/schema_check.py" upgrade s4_upgrade
uv run python "$S/schema_check.py" install s4_fresh
uv run python "$S/schema_check.py" reapply s4_fresh
uv run python "$S/schema_check.py" compare s4_fresh s4_upgrade
uv run python "$S/schema_check.py" drop s4_fresh s4_upgrade
echo "=== 4. lock= ==="
uv run python "$S/lock_check.py" 2>&1 | rg -v -e '^ ' -e '^Traceback' -e 'Error' -e 'boom'
echo "=== 5. AsyncPostgresSaver lock ==="
uv run python "$S/saver_check.py"
