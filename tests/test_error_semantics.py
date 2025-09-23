import pytest
from cassandra import InvalidRequest
from cassandra.cluster import Cluster

from mockylla import get_keyspaces, mock_scylladb


def test_get_keyspaces_without_context_raises_invalid_request():
    with pytest.raises(InvalidRequest):
        get_keyspaces()


@mock_scylladb
def test_set_keyspace_invalid_request():
    cluster = Cluster()
    session = cluster.connect()

    with pytest.raises(InvalidRequest, match="does not exist"):
        session.set_keyspace("missing")


@mock_scylladb
def test_insert_unknown_keyspace_raises_invalid_request():
    cluster = Cluster()
    session = cluster.connect()

    with pytest.raises(InvalidRequest, match="does not exist"):
        session.execute("INSERT INTO ks.users (id) VALUES (1)")


@mock_scylladb
def test_select_invalid_order_by_column():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute("CREATE TABLE users (id int PRIMARY KEY, name text)")
    session.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")

    with pytest.raises(InvalidRequest, match="ORDER BY"):
        session.execute("SELECT * FROM users ORDER BY unknown DESC")
