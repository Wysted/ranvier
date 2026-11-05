"""Check 3: Procrastinate schema install and scripted upgrade path.

Procrastinate does not track applied migrations (it dropped its version table in 0.5). Scripted
path tested here: fresh install = `schema.sql` + record every shipped migration file name as
applied (baseline); upgrade = apply, in file-name order, each shipped file not yet recorded, one
transaction per file. Version-agnostic: only uses psycopg and `procrastinate.schema` statics.

  python schema_check.py install <db>   # with any procrastinate version
  python schema_check.py upgrade <db>   # apply pending migration files of the installed version
  python schema_check.py reapply <db>   # schema.sql over an installed schema must fail
  python schema_check.py compare <a> <b> # pg_dump --schema-only diff (via the compose container)
  python schema_check.py drop <db>...
"""

import difflib
import importlib.metadata
import pathlib
import re
import subprocess
import sys

import psycopg
from common import drop_db, dsn, recreate_db
from procrastinate.schema import SchemaManager

TRACK = "s4_procrastinate_migrations"
CONTAINER = "graph-agent-db-1"
JSON_OBJECT_RE = re.compile(r"json_object\('type': ('[a-z_]+'), 'job_id': NEW\.id\)")


def shipped() -> list[str]:
    return sorted(
        p.name for p in pathlib.Path(SchemaManager.get_migrations_path()).glob("*.sql")
    )


def install(db: str) -> None:
    recreate_db(db)
    with psycopg.connect(dsn(db), autocommit=True) as conn, conn.transaction():
        conn.execute(SchemaManager.get_schema())  # no params: '%' needs no escaping
        conn.execute(f"CREATE TABLE {TRACK} (name text PRIMARY KEY)")
        for name in shipped():
            conn.execute(f"INSERT INTO {TRACK} VALUES (%s)", (name,))
    print(
        f"install {db}: procrastinate {importlib.metadata.version('procrastinate')}, "
        f"baseline={len(shipped())} files, last={shipped()[-1]}"
    )


def upgrade(db: str) -> None:
    path = pathlib.Path(SchemaManager.get_migrations_path())
    with psycopg.connect(dsn(db), autocommit=True) as conn:
        done = {r[0] for r in conn.execute(f"SELECT name FROM {TRACK}").fetchall()}
        pending = [n for n in shipped() if n not in done]
        for name in pending:
            with conn.transaction():
                conn.execute(path.joinpath(name).read_text(encoding="utf-8"))
                conn.execute(f"INSERT INTO {TRACK} VALUES (%s)", (name,))
    print(
        f"upgrade {db}: procrastinate {importlib.metadata.version('procrastinate')}, "
        f"applied={pending}"
    )


def reapply(db: str) -> None:
    """schema.sql is not idempotent: applying it over an installed schema must fail."""
    with psycopg.connect(dsn(db), autocommit=True) as conn:
        try:
            with conn.transaction():
                conn.execute(SchemaManager.get_schema())
        except psycopg.errors.DuplicateObject as exc:
            print(
                f"reapply {db}: schema.sql over an installed schema -> {type(exc).__name__}: {exc}"
            )
            return
    raise AssertionError("schema.sql applied twice without error")


def dump(db: str) -> list[str]:
    out = subprocess.run(
        [
            "docker",
            "exec",
            CONTAINER,
            "pg_dump",
            "-U",
            "graph_agent",
            "-s",
            "--no-owner",
            "--exclude-table",
            TRACK,
            db,
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    lines = [
        ln
        for ln in out.splitlines()
        if ln and not ln.startswith(("--", "\\restrict", "\\unrestrict"))
    ]
    # Known, equivalent drift: 3.0.0's schema used json_object('k': v) (PG16+ syntax); upstream
    # later rewrote schema.sql and 03.00.00_01_pre in place to json_build_object, no new migration.
    return [
        JSON_OBJECT_RE.sub(r"json_build_object('type', \1, 'job_id', NEW.id)", ln)
        for ln in lines
    ]


def compare(a: str, b: str) -> None:
    diff = list(difflib.unified_diff(dump(a), dump(b), a, b, lineterm="", n=1))
    print(f"compare {a} vs {b}: {len(dump(a))} lines, diff lines={len(diff)}")
    for ln in diff[:80]:
        print(ln)
    if diff:
        sys.exit(1)


if __name__ == "__main__":
    modes = {
        "install": install,
        "upgrade": upgrade,
        "reapply": reapply,
        "compare": compare,
        "drop": lambda *dbs: [drop_db(db) for db in dbs],
    }
    modes[sys.argv[1]](*sys.argv[2:])
