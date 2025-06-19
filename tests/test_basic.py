from pyscylladb_mock import mock_scylladb
from cassandra.cluster import Cluster


@mock_scylladb
def test_mock_connection():
    """
    Tests that the mock_scylladb decorator intercepts the connection.
    """
    print("Starting test_mock_connection...")

    # This will use our MockCluster because of the decorator
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    # This will call our MockSession's execute method
    rows = session.execute("SELECT * FROM system.local")

    assert len(rows) == 1
