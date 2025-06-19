from cassandra.cluster import Cluster

from pyscylladb_mock import mock_scylladb, get_table_rows


@mock_scylladb
def test_update_with_where_clause():
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
        f"UPDATE {table_name} SET city = 'San Francisco' WHERE name = 'Alice' AND city = 'Los Angeles'"
    )

    # Assert
    all_rows = get_table_rows(keyspace_name, table_name)
    assert len(all_rows) == 3

    # Find the updated row
    updated_row = None
    for row in all_rows:
        if row["id"] == 3:
            updated_row = row
            break

    assert updated_row is not None
    assert updated_row["city"] == "San Francisco"
