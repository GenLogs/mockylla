import pytest
from cassandra import InvalidRequest
from cassandra.cluster import Cluster

from mockylla import get_keyspaces, mock_scylladb


@mock_scylladb
def test_drop_keyspace_removes_metadata():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.execute("CREATE TABLE ks.users (id int PRIMARY KEY, name text)")

    session.execute("DROP KEYSPACE ks")

    assert "ks" not in get_keyspaces()
    assert cluster.metadata.get_keyspace("ks") is None


@mock_scylladb
def test_drop_keyspace_if_not_exists_is_noop():
    cluster = Cluster()
    session = cluster.connect()

    session.execute("DROP KEYSPACE IF EXISTS missing")

    assert cluster.metadata.get_keyspace("missing") is None


@mock_scylladb
def test_drop_system_keyspace_raises():
    session = Cluster().connect()

    with pytest.raises(InvalidRequest):
        session.execute("DROP KEYSPACE system")


@mock_scylladb
def test_drop_index_updates_state_and_metadata():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.execute("USE ks")
    session.execute("CREATE TABLE users (id int PRIMARY KEY, email text)")
    session.execute("CREATE INDEX email_idx ON users (email)")

    indexes = session.execute(
        "SELECT index_name FROM system_schema.indexes WHERE keyspace_name = 'ks'"
    ).all()
    assert any(row.index_name.lower() == "email_idx" for row in indexes)

    session.execute("DROP INDEX email_idx")

    indexes_after = session.execute(
        "SELECT index_name FROM system_schema.indexes WHERE keyspace_name = 'ks'"
    ).all()
    assert all(row.index_name.lower() != "email_idx" for row in indexes_after)

    table_meta = cluster.metadata.get_keyspace("ks").tables["users"]
    assert not table_meta.indexes


@mock_scylladb
def test_drop_index_if_exists_suppresses_error():
    session = Cluster().connect()
    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.execute("USE ks")
    session.execute("CREATE TABLE users (id int PRIMARY KEY, email text)")

    session.execute("DROP INDEX IF EXISTS missing_idx")


@mock_scylladb
def test_alter_table_with_updates_options():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.execute("USE ks")
    session.execute(
        "CREATE TABLE users (id int PRIMARY KEY, name text) WITH comment = 'initial'"
    )

    session.execute(
        "ALTER TABLE users WITH default_time_to_live = 0 AND comment = 'updated'"
    )

    table_meta = cluster.metadata.get_keyspace("ks").tables["users"]
    assert table_meta.options["comment"] == "updated"
    assert table_meta.options.get("default_time_to_live") == "0"


@mock_scylladb
def test_materialized_view_create_and_drop():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.execute("USE ks")
    session.execute("CREATE TABLE users (id int PRIMARY KEY, name text)")

    session.execute(
        """
        CREATE MATERIALIZED VIEW users_by_name AS
            SELECT id, name FROM users
            WHERE name IS NOT NULL
            PRIMARY KEY (name, id)
        """
    )

    views_rows = session.execute(
        "SELECT view_name, base_table_name FROM system_schema.views WHERE keyspace_name = 'ks'"
    ).all()
    assert any(row.view_name == "users_by_name" for row in views_rows)

    view_meta = cluster.metadata.get_keyspace("ks").view("users_by_name")
    assert view_meta is not None
    assert view_meta.base_table == "users"

    session.execute("DROP MATERIALIZED VIEW users_by_name")

    views_rows_after = session.execute(
        "SELECT view_name FROM system_schema.views WHERE keyspace_name = 'ks'"
    ).all()
    assert all(row.view_name != "users_by_name" for row in views_rows_after)
    assert cluster.metadata.get_keyspace("ks").view("users_by_name") is None
