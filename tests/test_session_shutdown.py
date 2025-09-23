import pytest
from cassandra.cluster import Cluster

from mockylla import mock_scylladb


@mock_scylladb
def test_shutdown_prevents_further_queries():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    session.shutdown()
    session.shutdown()  # idempotent

    assert session.is_shutdown is True

    with pytest.raises(RuntimeError):
        session.execute("SELECT * FROM system.local")

    with pytest.raises(RuntimeError):
        session.set_keyspace("system")


@mock_scylladb
def test_close_aliases_shutdown():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    session.close()

    with pytest.raises(RuntimeError):
        session.execute("SELECT * FROM system.local")
