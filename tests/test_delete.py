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


@mock_scylladb
def test_delete_rows_with_multiple_conditions():
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
        f"CREATE TABLE {table_name} (id int, category int, value text, PRIMARY KEY (id, category))"
    )

    # Insert some data
    session.execute(
        f"INSERT INTO {table_name} (id, category, value) VALUES (1, 10, 'one')"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, category, value) VALUES (1, 20, 'two')"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, category, value) VALUES (2, 10, 'three')"
    )

    # Act
    # This will fail with the current simple replacement
    session.execute(
        f"DELETE FROM {table_name} WHERE id = %s AND category = %s", (1, 10)
    )

    # Assert
    rows = get_table_rows(keyspace_name, table_name)
    assert len(rows) == 2

    # Check that correct rows remain
    remaining_vals = {(r["id"], r["category"]) for r in rows}
    assert remaining_vals == {(1, 20), (2, 10)}


@mock_scylladb
def test_delete_if_exists():
    """
    Tests the IF EXISTS clause for DELETE statements.
    """
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
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, name text)"
    )
    session.execute(f"INSERT INTO {table_name} (id, name) VALUES (1, 'Alice')")
    assert len(get_table_rows(keyspace_name, table_name)) == 1

    # Act & Assert: Delete on non-existent row should fail
    delete_fail_query = f"DELETE FROM {table_name} WHERE id = 2 IF EXISTS"
    result_fail = session.execute(delete_fail_query)
    assert result_fail.one()["[applied]"] is False
    assert len(get_table_rows(keyspace_name, table_name)) == 1

    # Act & Assert: Delete on existing row should succeed
    delete_success_query = f"DELETE FROM {table_name} WHERE id = 1 IF EXISTS"
    result_success = session.execute(delete_success_query)
    assert result_success.one()["[applied]"] is True
    assert len(get_table_rows(keyspace_name, table_name)) == 0
