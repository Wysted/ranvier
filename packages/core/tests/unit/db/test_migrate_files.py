from graph_agent._internal.db.migrate import discover_migrations


def test_packaged_migrations_are_ordered_and_contiguous() -> None:
    migrations = discover_migrations()
    assert [m.version for m in migrations] == list(range(1, len(migrations) + 1))
    assert migrations[0].name == "0001_init.sql"
    assert "graph_agent.runs" in migrations[0].sql
