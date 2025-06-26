from cassandra.cluster import Cluster

from mockylla import mock_scylladb, get_table_rows


@mock_scylladb
def test_delete_with_where_clause():
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

    session.execute(
        f"INSERT INTO {table_name} (id, name, city) VALUES (1, 'Alice', 'New York')"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, city) VALUES (2, 'Bob', 'Los Angeles')"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, city) VALUES (3, 'Alice', 'Los Angeles')"
    )

    session.execute(
        f"DELETE FROM {table_name} WHERE name = 'Alice' AND city = 'Los Angeles'"
    )

    remaining_rows = get_table_rows(keyspace_name, table_name)
    assert len(remaining_rows) == 2

    remaining_ids = {row["id"] for row in remaining_rows}
    assert remaining_ids == {1, 2}


@mock_scylladb
def test_delete_rows_with_multiple_conditions():
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

    session.execute(
        f"INSERT INTO {table_name} (id, category, value) VALUES (1, 10, 'one')"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, category, value) VALUES (1, 20, 'two')"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, category, value) VALUES (2, 10, 'three')"
    )

    session.execute(
        f"DELETE FROM {table_name} WHERE id = %s AND category = %s", (1, 10)
    )

    rows = get_table_rows(keyspace_name, table_name)
    assert len(rows) == 2

    remaining_vals = {(r["id"], r["category"]) for r in rows}
    assert remaining_vals == {(1, 20), (2, 10)}


@mock_scylladb
def test_delete_if_exists():
    """
    Tests the IF EXISTS clause for DELETE statements.
    """

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

    delete_fail_query = f"DELETE FROM {table_name} WHERE id = 2 IF EXISTS"
    result_fail = session.execute(delete_fail_query)
    assert result_fail.one()["[applied]"] is False
    assert len(get_table_rows(keyspace_name, table_name)) == 1

    delete_success_query = f"DELETE FROM {table_name} WHERE id = 1 IF EXISTS"
    result_success = session.execute(delete_success_query)
    assert result_success.one()["[applied]"] is True
    assert len(get_table_rows(keyspace_name, table_name)) == 0
