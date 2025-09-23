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
