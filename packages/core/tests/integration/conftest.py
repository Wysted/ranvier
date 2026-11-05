"""Fixtures giving integration tests an isolated, disposable Postgres database.

The server is the dev compose service (`docker compose up -d`). One throwaway database is
created per session and dropped at the end; each test gets a freshly emptied database.
"""

import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import NoReturn
from urllib.parse import urlsplit

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

TEST_DATABASE_URL_ENV = "GRAPH_AGENT_TEST_DATABASE_URL"
REQUIRE_SERVICES_ENV = "GRAPH_AGENT_REQUIRE_SERVICES"
TRUTHY_VALUES = frozenset({"1", "true", "yes"})
# Matches the dev credentials and host port in docker-compose.yml.
DEFAULT_DATABASE_URL = (
    "postgresql://graph_agent:graph_agent@127.0.0.1:54329/graph_agent"
)
CONNECT_TIMEOUT_SECONDS = 3
INTEGRATION_DIR = Path(__file__).parent.resolve()


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark every test under tests/integration/ as `integration`."""
    for item in items:
        if item.path.is_relative_to(INTEGRATION_DIR):
            item.add_marker(pytest.mark.integration)


def _unavailable(message: str) -> NoReturn:
    """Fail when services are required (CI), else skip so a bare `pytest` stays usable."""
    if os.environ.get(REQUIRE_SERVICES_ENV, "").lower() in TRUTHY_VALUES:
        pytest.fail(message, pytrace=False)
    pytest.skip(message)


def _connect(url: str) -> psycopg.Connection[tuple[object, ...]]:
    return psycopg.connect(
        url, autocommit=True, connect_timeout=CONNECT_TIMEOUT_SECONDS
    )


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    """URL of a throwaway database, created for the session and dropped at the end.

    `GRAPH_AGENT_TEST_DATABASE_URL` (admin connection, may create databases) must be a
    `postgresql://` URL, not key/value conninfo: the result is that URL with its path
    replaced by the throwaway database name.
    """
    admin_url = os.environ.get(TEST_DATABASE_URL_ENV, DEFAULT_DATABASE_URL)
    name = f"graph_agent_test_{uuid.uuid4().hex[:12]}"
    try:
        admin = _connect(admin_url)
    except psycopg.OperationalError as exc:
        target = conninfo_to_dict(admin_url)
        _unavailable(
            f"Postgres is unreachable at {target.get('host')}:{target.get('port')} "
            f"(or rejected the credentials): run `docker compose up -d` "
            f"or set {TEST_DATABASE_URL_ENV}. Cause: {exc}"
        )
    with admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        yield urlsplit(admin_url)._replace(path=f"/{name}").geturl()
    finally:
        with _connect(admin_url) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(name)
                )
            )


@pytest.fixture
def clean_database(database_url: str) -> Iterator[str]:
    """`database_url` with every non-system schema dropped and an empty `public` recreated.

    Covers `public` and any schema the code creates (e.g. `graph_agent`, design section 10).
    """
    with _connect(database_url) as conn:
        schemas = conn.execute(
            "SELECT nspname FROM pg_namespace "
            "WHERE nspname NOT LIKE 'pg\\_%' AND nspname <> 'information_schema'"
        ).fetchall()
        for (schema,) in schemas:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(str(schema)))
            )
        conn.execute("CREATE SCHEMA public")
    yield database_url
