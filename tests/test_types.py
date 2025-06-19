from cassandra.cluster import Cluster

from pyscylladb_mock import mock_scylladb


@mock_scylladb
def test_integer_type_casting():
    # Arrange
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    table_name = "my_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, value int)"
    )

    # Act
    session.execute(f"INSERT INTO {table_name} (id, value) VALUES (1, 100)")

    # Assert
    result = session.execute(f"SELECT * FROM {table_name} WHERE id = 1")
    rows = list(result)

    assert len(rows) == 1
    assert rows[0]["value"] == 100
    assert isinstance(rows[0]["value"], int)


@mock_scylladb
def test_numeric_comparison_in_where_clause():
    # Arrange
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    table_name = "my_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, value int)"
    )

    # Insert some data
    session.execute(f"INSERT INTO {table_name} (id, value) VALUES (1, 5)")
    session.execute(f"INSERT INTO {table_name} (id, value) VALUES (2, 100)")

    # Act
    # With string comparison, '5' > '10' so this would incorrectly return the row with id 1.
    result = session.execute(f"SELECT * FROM {table_name} WHERE value > 10")
    rows = list(result)

    # Assert
    assert len(rows) == 1
    assert rows[0]["id"] == 2
    assert rows[0]["value"] == 100
