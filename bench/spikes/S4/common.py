"""Shared helpers for the S4 spike: throwaway databases on compose Postgres (port 25432)."""

import os

import psycopg
from procrastinate import App, PsycopgConnector

HOST = os.environ.get("S4_PGHOST", "127.0.0.1")
PORT = os.environ.get("S4_PGPORT", "25432")
# Dev-only compose credentials (docker-compose.yml); never real secrets.
ADMIN_DSN = f"postgresql://graph_agent:graph_agent@{HOST}:{PORT}/graph_agent"


def dsn(db: str) -> str:
    return f"postgresql://graph_agent:graph_agent@{HOST}:{PORT}/{db}"


def recreate_db(db: str) -> str:
    """Drop and create a throwaway database (name must start with s4_)."""
    assert db.startswith("s4_"), db
    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
        conn.execute(f"CREATE DATABASE {db}")
    return dsn(db)


def drop_db(db: str) -> None:
    assert db.startswith("s4_"), db
    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")


def make_app(db: str, max_size: int = 5) -> App:
    return App(
        connector=PsycopgConnector(conninfo=dsn(db), min_size=1, max_size=max_size)
    )
