"""Fixtures giving integration tests an isolated, disposable Postgres database.

The server is the dev compose service (`docker compose up -d`). One throwaway database is
created per session and dropped at the end; each test gets a freshly emptied schema.
"""

import os
import re
import uuid
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

TEST_DATABASE_URL_ENV = "GRAPH_AGENT_TEST_DATABASE_URL"
REQUIRE_SERVICES_ENV = "GRAPH_AGENT_REQUIRE_SERVICES"
# Matches the dev credentials and host port in docker-compose.yml.
DEFAULT_DATABASE_URL = (
    "postgresql://graph_agent:graph_agent@127.0.0.1:54329/graph_agent"
)
CONNECT_TIMEOUT_SECONDS = 3
INTEGRATION_DIR = Path(__file__).parent.resolve()
_DISPOSABLE_NAME = re.compile(r"graph_agent_test_[0-9a-f]{12}")


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark every test under tests/integration/ as `integration`."""
    for item in items:
        if INTEGRATION_DIR in Path(str(item.fspath)).resolve().parents:
            item.add_marker(pytest.mark.integration)


def _unavailable(message: str) -> None:
    """Fail when services are required (CI), else skip so a bare `pytest` stays usable."""
    if os.environ.get(REQUIRE_SERVICES_ENV) == "1":
        pytest.fail(message, pytrace=False)
    pytest.skip(message)


def _connect(url: str) -> psycopg.Connection[tuple[object, ...]]:
    return psycopg.connect(
        url, autocommit=True, connect_timeout=CONNECT_TIMEOUT_SECONDS
    )


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    """URL of a throwaway database, created for the session and dropped at the end."""
    admin_url = os.environ.get(TEST_DATABASE_URL_ENV, DEFAULT_DATABASE_URL)
    name = f"graph_agent_test_{uuid.uuid4().hex[:12]}"
    # Only names generated above can be dropped, never the server's own databases.
    assert _DISPOSABLE_NAME.fullmatch(name)
    try:
        admin = _connect(admin_url)
    except psycopg.OperationalError as exc:
        _unavailable(
            f"Postgres is unreachable at {conninfo_to_dict(admin_url).get('host')}:"
            f"{conninfo_to_dict(admin_url).get('port')}: run `docker compose up -d` "
            f"(or set {TEST_DATABASE_URL_ENV}). Cause: {exc}"
        )
        raise  # unreachable: _unavailable always raises
    with admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            yield make_conninfo(admin_url, dbname=name)
        finally:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(name)
                )
            )


@pytest.fixture
def clean_database(database_url: str) -> Iterator[str]:
    """`database_url` with an empty `public` schema, reset before the test runs."""
    with _connect(database_url) as conn:
        conn.execute("DROP SCHEMA IF EXISTS public CASCADE")
        conn.execute("CREATE SCHEMA public")
    yield database_url
