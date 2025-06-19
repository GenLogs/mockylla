from cassandra.cluster import Cluster

from pyscylladb_mock import mock_scylladb, get_table_rows


@mock_scylladb
def test_delete_with_where_clause():
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
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, name text, city text)"
    )

    # Insert some data
    session.execute(
        f"INSERT INTO {table_name} (id, name, city) VALUES (1, 'Alice', 'New York')"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, city) VALUES (2, 'Bob', 'Los Angeles')"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, city) VALUES (3, 'Alice', 'Los Angeles')"
    )

    # Act
    session.execute(
        f"DELETE FROM {table_name} WHERE name = 'Alice' AND city = 'Los Angeles'"
    )

    # Assert
    remaining_rows = get_table_rows(keyspace_name, table_name)
    assert len(remaining_rows) == 2

    # Check that the correct rows remain
    remaining_ids = {row["id"] for row in remaining_rows}
    assert remaining_ids == {1, 2}
