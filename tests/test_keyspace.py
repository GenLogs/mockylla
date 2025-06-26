from mockylla import mock_scylladb, get_keyspaces
from cassandra.cluster import Cluster


@mock_scylladb
def test_create_keyspace():
    """
    Tests that a keyspace can be created and inspected.
    """
    # Arrange
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_test_keyspace"

    # Act
    session.execute(
        f"CREATE KEYSPACE {keyspace_name} "
        "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )

    # Assert
    created_keyspaces = get_keyspaces()
    assert keyspace_name in created_keyspaces
